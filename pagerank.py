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

BATCH_SIZE = 50

ALPHA = 0.85
MAX_ITER = 100
TOLERANCE = 1.0e-6

D1_TIMEOUT = 30
D1_RETRIES = 2

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

    Hanya retry error transient:
    - network error
    - 408
    - 429
    - 5xx

    Error 400/SQL/schema TIDAK di-retry
    supaya tidak membuang quota.
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

            # -----------------------------
            # SUCCESS
            # -----------------------------

            if 200 <= status < 300:
                if data and data.get("success") is True:
                    return data

                raise RuntimeError(
                    "D1 mengembalikan HTTP sukses tetapi "
                    f"success=false/tidak valid:\n{response.text[:2000]}"
                )

            # -----------------------------
            # TRANSIENT ERROR
            # -----------------------------

            if status in (408, 429) or 500 <= status <= 599:
                last_error = RuntimeError(
                    f"D1 HTTP {status}: {response.text[:2000]}"
                )

                if attempt < D1_RETRIES:
                    wait_seconds = 2 ** attempt

                    print(
                        f"[D1] Error transient HTTP {status}. "
                        f"Retry {attempt + 1}/{D1_RETRIES} "
                        f"dalam {wait_seconds}s..."
                    )

                    time.sleep(wait_seconds)
                    continue

                raise last_error

            # -----------------------------
            # PERMANENT ERROR
            # JANGAN RETRY
            # -----------------------------

            raise RuntimeError(
                f"D1 HTTP {status}: {response.text[:4000]}"
            )

        except requests.RequestException as exc:
            last_error = exc

            if attempt < D1_RETRIES:
                wait_seconds = 2 ** attempt

                print(
                    f"[D1] Network error. "
                    f"Retry {attempt + 1}/{D1_RETRIES} "
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
# ============================================================

def get_page_graph():
    print("[D1] Mengambil page_graph...")

    payload = {
        "sql": """
            SELECT source_url, target_url
            FROM page_graph
            WHERE source_url IS NOT NULL
              AND target_url IS NOT NULL
        """
    }

    data = d1_request(payload)

    results = data.get("result", [])

    if not results:
        return []

    rows = results[0].get("results", [])

    edges = []

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

        edges.append((source, target))

    return edges


# ============================================================
# BUILD GRAPH
# ============================================================

def build_graph(edges):
    """
    Membuat graph directed sederhana.

    outgoing[source] = set/list target
    incoming[target] = set/list source

    Tidak menggunakan NetworkX.
    Tidak menggunakan NumPy.
    Tidak menggunakan SciPy.
    """

    outgoing = defaultdict(set)
    incoming = defaultdict(set)

    nodes = set()

    for source, target in edges:
        nodes.add(source)
        nodes.add(target)

        outgoing[source].add(target)
        incoming[target].add(source)

    # Pastikan node tanpa outgoing tetap ada
    for node in nodes:
        outgoing[node]

    return nodes, outgoing, incoming


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
            alpha * jumlah(PR(u) / out_degree(u))

    Untuk dangling node (tidak punya outgoing link),
    rank-nya didistribusikan ke semua node.
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

    teleport = (1.0 - ALPHA) / n

    for iteration in range(1, MAX_ITER + 1):

        # ----------------------------------------------------
        # Total rank dari dangling nodes
        # ----------------------------------------------------

        dangling_rank = 0.0

        for node in node_list:
            if not outgoing[node]:
                dangling_rank += ranks[node]

        dangling_share = ALPHA * dangling_rank / n

        # ----------------------------------------------------
        # Rank baru
        # ----------------------------------------------------

        new_ranks = {}

        for node in node_list:
            new_rank = teleport + dangling_share

            # Semua incoming node
            # akan memberi kontribusi ke node ini.
            #
            # Kita tidak menyimpan incoming map di sini karena
            # graph kecil dan metode ini lebih sederhana.
            #
            # Namun untuk performa kita gunakan incoming map
            # dari global cache di bawah.
            new_ranks[node] = new_rank

        # ----------------------------------------------------
        # Distribusi incoming links
        # ----------------------------------------------------

        for source in node_list:

            targets = outgoing[source]

            if not targets:
                continue

            contribution = ALPHA * ranks[source] / len(targets)

            for target in targets:
                new_ranks[target] += contribution

        # ----------------------------------------------------
        # Cek konvergensi
        # ----------------------------------------------------

        error = 0.0

        for node in node_list:
            error += abs(
                new_ranks[node] - ranks[node]
            )

        ranks = new_ranks

        if iteration == 1 or iteration % 5 == 0:
            print(
                f"[PAGERANK] Iterasi {iteration}/{MAX_ITER} "
                f"| error = {error:.10f}"
            )

        if error < TOLERANCE:
            print(
                f"[PAGERANK] Konvergen pada iterasi "
                f"{iteration}"
            )
            print(
                f"[PAGERANK] Final error = "
                f"{error:.10f}"
            )
            break

    else:
        print(
            "[PAGERANK] Mencapai MAX_ITER tanpa "
            "konvergen penuh."
        )
        print(
            f"[PAGERANK] Final error = {error:.10f}"
        )

    # --------------------------------------------------------
    # Normalisasi akhir
    # --------------------------------------------------------

    total = sum(ranks.values())

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

    Menggunakan D1 batch.
    Tidak ada test query.

    50 UPDATE = 1 request D1.
    """

    if not scores:
        print("[D1] Tidak ada score untuk di-update.")
        return

    items = list(scores.items())

    print(
        f"[D1] Mengupdate {len(items):,} PageRank..."
    )

    total_batches = (
        len(items) + BATCH_SIZE - 1
    ) // BATCH_SIZE

    completed = 0

    for batch_index in range(total_batches):

        start = batch_index * BATCH_SIZE
        end = min(
            start + BATCH_SIZE,
            len(items)
        )

        chunk = items[start:end]

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
            f"{completed:,}/{len(items):,}"
            f" ({completed / len(items) * 100:.1f}%)"
        )

    print("[D1] Semua PageRank berhasil di-update.")


# ============================================================
# TOP RESULTS
# ============================================================

def print_top_scores(scores, limit=20):
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

    for index, (url, score) in enumerate(top, start=1):
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
    print(f"Batch size : {BATCH_SIZE}")
    print(f"Alpha      : {ALPHA}")
    print(f"Max iter   : {MAX_ITER}")
    print(f"Tolerance  : {TOLERANCE}")
    print("Engine     : PURE PYTHON")
    print("NumPy      : NO")
    print("SciPy      : NO")
    print("=" * 60)

    try:
        # ----------------------------------------------------
        # 1. CONFIG
        # ----------------------------------------------------

        check_config()

        # ----------------------------------------------------
        # 2. GET GRAPH
        # ----------------------------------------------------

        edges = get_page_graph()

        print(
            f"[D1] Graph edges: {len(edges):,}"
        )

        if not edges:
            print(
                "[PAGERANK] page_graph kosong. "
                "Tidak ada yang dihitung."
            )
            return

        # ----------------------------------------------------
        # 3. BUILD GRAPH
        # ----------------------------------------------------

        print("[PAGERANK] Membuat graph...")

        nodes, outgoing, incoming = build_graph(edges)

        print(
            f"[PAGERANK] Nodes : {len(nodes):,}"
        )

        print(
            f"[PAGERANK] Edges : "
            f"{sum(len(v) for v in outgoing.values()):,}"
        )

        # ----------------------------------------------------
        # 4. CALCULATE
        # ----------------------------------------------------

        print("[PAGERANK] Menghitung PageRank...")

        scores = calculate_pagerank(
            nodes,
            outgoing,
        )

        # ----------------------------------------------------
        # 5. SHOW TOP
        # ----------------------------------------------------

        print_top_scores(
            scores,
            limit=20,
        )

        # ----------------------------------------------------
        # 6. UPDATE D1
        # ----------------------------------------------------

        print()
        print("[D1] Menyimpan PageRank...")

        update_pagerank(scores)

        # ----------------------------------------------------
        # DONE
        # ----------------------------------------------------

        elapsed = time.time() - start_time

        print()
        print("=" * 60)
        print("PAGERANK SELESAI")
        print("=" * 60)
        print(
            f"Nodes  : {len(nodes):,}"
        )
        print(
            f"Edges  : "
            f"{sum(len(v) for v in outgoing.values()):,}"
        )
        print(
            f"Scores : {len(scores):,}"
        )
        print(
            f"Waktu  : {elapsed:.2f} detik"
        )
        print("=" * 60)

    except KeyboardInterrupt:
        print()
        print("[ERROR] Proses dihentikan.")
        sys.exit(1)

    except Exception as exc:
        print()
        print("=" * 60)
        print("PAGERANK ERROR")
        print("=" * 60)
        print(str(exc))
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
