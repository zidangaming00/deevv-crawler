import os
import sys
import time
import requests
from collections import defaultdict

# ============================================================
# DEEVV SEARCH - PAGERANK
# Pure Python - TANPA NumPy / SciPy
# ============================================================

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "").strip()
CF_D1_DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID", "").strip()
CF_API_TOKEN = os.environ.get("CF_API_TOKEN", "").strip()

# ============================================================
# CONFIG
# ============================================================

# Jumlah UPDATE documents dalam 1 D1 request
BATCH_SIZE = 50

# Jumlah graph edge yang diambil per request
# Jangan terlalu besar agar response D1 tidak terlalu berat.
GRAPH_PAGE_SIZE = 5000

ALPHA = 0.85
MAX_ITER = 100
TOLERANCE = 1.0e-6

# Cloudflare API/D1 request maksimal sekitar 30 detik.
D1_TIMEOUT = 30

# Retry lebih banyak untuk 503 / 429 / network error
D1_RETRIES = 5

# Jeda retry:
# 2s -> 5s -> 10s -> 20s -> 30s
RETRY_DELAYS = [2, 5, 10, 20, 30]


# ============================================================
# D1
# ============================================================

def d1_url():
    return (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"
    )


def check_config():
    missing = []

    if not CF_ACCOUNT_ID:
        missing.append("CF_ACCOUNT_ID")

    if not CF_D1_DATABASE_ID:
        missing.append("CF_D1_DATABASE_ID")

    if not CF_API_TOKEN:
        missing.append("CF_API_TOKEN")

    if missing:
        raise RuntimeError(
            "Secret GitHub Actions belum lengkap: "
            + ", ".join(missing)
        )


def d1_request(payload):
    """
    D1 REST API.

    Retry:
    - network error
    - HTTP 408
    - HTTP 429
    - HTTP 500-599

    Termasuk Cloudflare:
    503 / code 7010

    Error SQL/permanent seperti 400 tidak di-retry.
    """

    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }

    last_error = None

    for attempt in range(D1_RETRIES + 1):

        try:
            response = requests.post(
                d1_url(),
                headers=headers,
                json=payload,
                timeout=D1_TIMEOUT,
            )

            status = response.status_code

            try:
                data = response.json()
            except Exception:
                data = None

            # =================================================
            # SUCCESS
            # =================================================

            if 200 <= status < 300:

                if data and data.get("success") is True:
                    return data

                raise RuntimeError(
                    "D1 mengembalikan HTTP sukses tetapi "
                    f"success=false/tidak valid:\n"
                    f"{response.text[:4000]}"
                )

            # =================================================
            # TRANSIENT ERROR
            # =================================================

            if status in (408, 429) or 500 <= status <= 599:

                last_error = RuntimeError(
                    f"D1 HTTP {status}: "
                    f"{response.text[:4000]}"
                )

                if attempt < D1_RETRIES:

                    wait_seconds = RETRY_DELAYS[
                        min(attempt, len(RETRY_DELAYS) - 1)
                    ]

                    # Khusus 503 tampilkan informasi lebih jelas
                    if status == 503:

                        print(
                            f"[D1] Service unavailable "
                            f"(HTTP 503). "
                            f"Retry {attempt + 1}/"
                            f"{D1_RETRIES} "
                            f"dalam {wait_seconds}s..."
                        )

                    else:

                        print(
                            f"[D1] Error transient HTTP "
                            f"{status}. "
                            f"Retry {attempt + 1}/"
                            f"{D1_RETRIES} "
                            f"dalam {wait_seconds}s..."
                        )

                    time.sleep(wait_seconds)
                    continue

                raise last_error

            # =================================================
            # PERMANENT ERROR
            # =================================================

            raise RuntimeError(
                f"D1 HTTP {status}: "
                f"{response.text[:4000]}"
            )

        except requests.RequestException as exc:

            last_error = exc

            if attempt < D1_RETRIES:

                wait_seconds = RETRY_DELAYS[
                    min(attempt, len(RETRY_DELAYS) - 1)
                ]

                print(
                    f"[D1] Network error: {exc}"
                )

                print(
                    f"[D1] Retry {attempt + 1}/"
                    f"{D1_RETRIES} "
                    f"dalam {wait_seconds}s..."
                )

                time.sleep(wait_seconds)
                continue

            raise RuntimeError(
                f"D1 request gagal: {exc}"
            ) from exc

    raise RuntimeError(
        f"D1 request gagal: {last_error}"
    )


# ============================================================
# GET PAGE GRAPH
# PAGINATED / KEYSET PAGINATION
# ============================================================

def get_page_graph():
    """
    Mengambil page_graph secara bertahap.

    TIDAK lagi melakukan:

        SELECT seluruh page_graph

    sekaligus.

    Digunakan keyset pagination berdasarkan:

        source_url
        target_url

    sehingga database tidak perlu memakai OFFSET besar.
    """

    print("[D1] Mengambil page_graph secara bertahap...")
    print(
        f"[D1] Graph page size: "
        f"{GRAPH_PAGE_SIZE:,}"
    )

    edges = []

    last_source = ""
    last_target = ""

    page_number = 0

    while True:

        page_number += 1

        # ====================================================
        # QUERY PERTAMA
        # ====================================================

        if not last_source and not last_target:

            sql = f"""
                SELECT source_url, target_url
                FROM page_graph
                WHERE source_url IS NOT NULL
                  AND target_url IS NOT NULL
                ORDER BY source_url, target_url
                LIMIT {GRAPH_PAGE_SIZE}
            """

            payload = {
                "sql": sql
            }

        # ====================================================
        # QUERY LANJUTAN
        # ====================================================

        else:

            sql = f"""
                SELECT source_url, target_url
                FROM page_graph
                WHERE source_url IS NOT NULL
                  AND target_url IS NOT NULL
                  AND (
                      source_url > ?
                      OR (
                          source_url = ?
                          AND target_url > ?
                      )
                  )
                ORDER BY source_url, target_url
                LIMIT {GRAPH_PAGE_SIZE}
            """

            payload = {
                "sql": sql,
                "params": [
                    last_source,
                    last_source,
                    last_target,
                ],
            }

        # ====================================================
        # REQUEST
        # ====================================================

        data = d1_request(payload)

        results = data.get("result", [])

        if not results:
            break

        rows = results[0].get("results", [])

        if not rows:
            break

        page_edges = 0

        # ====================================================
        # PARSE
        # ====================================================

        for row in rows:

            source = row.get("source_url")
            target = row.get("target_url")

            if not source or not target:
                continue

            source = str(source).strip()
            target = str(target).strip()

            if not source or not target:
                continue

            # Buang self-link
            if source == target:
                continue

            edges.append(
                (
                    source,
                    target,
                )
            )

            page_edges += 1

        # ====================================================
        # CURSOR
        # ====================================================

        last_row = rows[-1]

        last_source = str(
            last_row.get("source_url") or ""
        ).strip()

        last_target = str(
            last_row.get("target_url") or ""
        ).strip()

        # ====================================================
        # PROGRESS
        # ====================================================

        print(
            f"[D1] Graph page {page_number:,} "
            f"| +{page_edges:,} edges "
            f"| total {len(edges):,}"
        )

        # ====================================================
        # END
        # ====================================================

        if len(rows) < GRAPH_PAGE_SIZE:
            break

    print(
        f"[D1] Selesai mengambil graph: "
        f"{len(edges):,} edges"
    )

    return edges


# ============================================================
# BUILD GRAPH
# ============================================================

def build_graph(edges):
    """
    Membuat graph directed sederhana.

    outgoing[source] = set(target)

    Tidak menggunakan:
    - NetworkX
    - NumPy
    - SciPy
    """

    outgoing = defaultdict(set)

    nodes = set()

    for source, target in edges:

        nodes.add(source)
        nodes.add(target)

        outgoing[source].add(target)

    # Pastikan semua node memiliki entry outgoing
    for node in nodes:
        outgoing[node]

    return nodes, outgoing


# ============================================================
# PURE PYTHON PAGERANK
# ============================================================

def calculate_pagerank(nodes, outgoing):
    """
    PageRank menggunakan power iteration pure Python.

    Rumus:

        PR(v) =
            (1-alpha)/N
            +
            alpha * incoming contribution
            +
            dangling contribution

    Dangling node:
    node tanpa outgoing link.

    Rank dangling node didistribusikan
    ke seluruh node.
    """

    print("[PAGERANK] Menghitung PageRank...")
    print(f"[PAGERANK] alpha = {ALPHA}")
    print(f"[PAGERANK] max_iter = {MAX_ITER}")
    print(f"[PAGERANK] tolerance = {TOLERANCE}")
    print("[PAGERANK] Mode = PURE PYTHON")
    print("[PAGERANK] NumPy = TIDAK DIPAKAI")
    print("[PAGERANK] SciPy = TIDAK DIPAKAI")

    node_list = list(nodes)

    n = len(node_list)

    if n == 0:
        return {}

    initial_rank = 1.0 / n

    ranks = {
        node: initial_rank
        for node in node_list
    }

    teleport = (
        (1.0 - ALPHA)
        / n
    )

    for iteration in range(
        1,
        MAX_ITER + 1
    ):

        # ====================================================
        # DANGLING RANK
        # ====================================================

        dangling_rank = 0.0

        for node in node_list:

            if not outgoing[node]:
                dangling_rank += ranks[node]

        dangling_share = (
            ALPHA
            * dangling_rank
            / n
        )

        # ====================================================
        # INITIAL NEW RANK
        # ====================================================

        new_ranks = {}

        base_rank = (
            teleport
            + dangling_share
        )

        for node in node_list:
            new_ranks[node] = base_rank

        # ====================================================
        # DISTRIBUTE OUTGOING RANK
        # ====================================================

        for source in node_list:

            targets = outgoing[source]

            if not targets:
                continue

            contribution = (
                ALPHA
                * ranks[source]
                / len(targets)
            )

            for target in targets:
                new_ranks[target] += contribution

        # ====================================================
        # CONVERGENCE
        # ====================================================

        error = 0.0

        for node in node_list:

            error += abs(
                new_ranks[node]
                - ranks[node]
            )

        ranks = new_ranks

        if (
            iteration == 1
            or iteration % 5 == 0
        ):

            print(
                f"[PAGERANK] Iterasi "
                f"{iteration}/{MAX_ITER} "
                f"| error = "
                f"{error:.10f}"
            )

        if error < TOLERANCE:

            print(
                f"[PAGERANK] Konvergen "
                f"pada iterasi "
                f"{iteration}"
            )

            print(
                f"[PAGERANK] Final error = "
                f"{error:.10f}"
            )

            break

    else:

        print(
            "[PAGERANK] Mencapai MAX_ITER "
            "tanpa konvergen penuh."
        )

        print(
            f"[PAGERANK] Final error = "
            f"{error:.10f}"
        )

    # ========================================================
    # NORMALISASI
    # ========================================================

    total = sum(
        ranks.values()
    )

    if total > 0:

        for node in ranks:
            ranks[node] /= total

    return ranks


# ============================================================
# UPDATE D1
# ============================================================

def update_pagerank(scores):
    """
    Update pagerank ke documents.

    50 UPDATE = 1 request D1.
    """

    if not scores:

        print(
            "[D1] Tidak ada score "
            "untuk di-update."
        )

        return

    items = list(
        scores.items()
    )

    print(
        f"[D1] Mengupdate "
        f"{len(items):,} PageRank..."
    )

    total_batches = (
        len(items)
        + BATCH_SIZE
        - 1
    ) // BATCH_SIZE

    completed = 0

    for batch_index in range(
        total_batches
    ):

        start = (
            batch_index
            * BATCH_SIZE
        )

        end = min(
            start + BATCH_SIZE,
            len(items)
        )

        chunk = items[
            start:end
        ]

        statements = []

        for url, score in chunk:

            statements.append({
                "sql": """
                    UPDATE documents
                    SET pagerank = ?
                    WHERE url = ?
                """,
                "params": [
                    float(score),
                    url,
                ],
            })

        d1_request({
            "batch": statements
        })

        completed += len(chunk)

        print(
            f"[D1] Update "
            f"{completed:,}/"
            f"{len(items):,}"
            f" ("
            f"{completed / len(items) * 100:.1f}"
            f"%)"
        )

    print(
        "[D1] Semua PageRank "
        "berhasil di-update."
    )


# ============================================================
# TOP RESULTS
# ============================================================

def print_top_scores(
    scores,
    limit=20
):

    if not scores:
        return

    print()
    print("=" * 60)
    print("TOP PAGERANK")
    print("=" * 60)

    top = sorted(
        scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:limit]

    for index, (
        url,
        score
    ) in enumerate(
        top,
        start=1
    ):

        print(
            f"{index:>2}. "
            f"{score:.10f} "
            f"{url}"
        )

    print("=" * 60)


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)
    print("DEEVV SEARCH - PAGERANK")
    print("=" * 60)
    print(
        f"Batch size      : "
        f"{BATCH_SIZE}"
    )
    print(
        f"Graph page size : "
        f"{GRAPH_PAGE_SIZE}"
    )
    print(
        f"Alpha           : "
        f"{ALPHA}"
    )
    print(
        f"Max iter        : "
        f"{MAX_ITER}"
    )
    print(
        f"Tolerance       : "
        f"{TOLERANCE}"
    )
    print(
        f"D1 timeout      : "
        f"{D1_TIMEOUT}s"
    )
    print(
        f"D1 retries      : "
        f"{D1_RETRIES}"
    )
    print(
        "Engine          : "
        "PURE PYTHON"
    )
    print(
        "NumPy           : NO"
    )
    print(
        "SciPy           : NO"
    )
    print("=" * 60)

    try:

        # ====================================================
        # 1. CONFIG
        # ====================================================

        check_config()

        # ====================================================
        # 2. GET GRAPH
        # ====================================================

        edges = get_page_graph()

        print(
            f"[D1] Graph edges: "
            f"{len(edges):,}"
        )

        if not edges:

            print(
                "[PAGERANK] page_graph kosong. "
                "Tidak ada yang dihitung."
            )

            return

        # ====================================================
        # 3. BUILD GRAPH
        # ====================================================

        print(
            "[PAGERANK] Membuat graph..."
        )

        nodes, outgoing = (
            build_graph(edges)
        )

        actual_edges = sum(
            len(targets)
            for targets in
            outgoing.values()
        )

        print(
            f"[PAGERANK] Nodes : "
            f"{len(nodes):,}"
        )

        print(
            f"[PAGERANK] Edges : "
            f"{actual_edges:,}"
        )

        # ====================================================
        # 4. CALCULATE
        # ====================================================

        scores = calculate_pagerank(
            nodes,
            outgoing,
        )

        # ====================================================
        # 5. SHOW TOP
        # ====================================================

        print_top_scores(
            scores,
            limit=20,
        )

        # ====================================================
        # 6. UPDATE D1
        # ====================================================

        print()
        print(
            "[D1] Menyimpan PageRank..."
        )

        update_pagerank(
            scores
        )

        # ====================================================
        # DONE
        # ====================================================

        elapsed = (
            time.time()
            - start_time
        )

        print()
        print("=" * 60)
        print("PAGERANK SELESAI")
        print("=" * 60)
        print(
            f"Nodes  : "
            f"{len(nodes):,}"
        )
        print(
            f"Edges  : "
            f"{actual_edges:,}"
        )
        print(
            f"Scores : "
            f"{len(scores):,}"
        )
        print(
            f"Waktu  : "
            f"{elapsed:.2f} detik"
        )
        print("=" * 60)

    except KeyboardInterrupt:

        print()
        print(
            "[ERROR] Proses dihentikan."
        )

        sys.exit(1)

    except Exception as exc:

        print()
        print("=" * 60)
        print("PAGERANK ERROR")
        print("=" * 60)
        print(str(exc))
        print("=" * 60)

        sys.exit(1)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
