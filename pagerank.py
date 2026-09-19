import os
import sys
import time
import requests
import networkx as nx


# ============================================================
# CONFIG
# ============================================================

D1_BATCH_SIZE = 50
D1_REQUEST_TIMEOUT = 60
D1_RETRY_COUNT = 3

# PageRank settings
PAGERANK_ALPHA = 0.85
PAGERANK_MAX_ITER = 100
PAGERANK_TOL = 1.0e-6

# Hanya update URL yang benar-benar memiliki PageRank.
# Tidak membuat / mengubah tabel apa pun.
UPDATE_SQL = """
UPDATE documents
SET pagerank = ?
WHERE url = ?
"""


# ============================================================
# CLOUDFLARE ENV
# ============================================================

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_D1_DATABASE_ID = os.getenv("CF_D1_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")


# ============================================================
# VALIDATE ENV
# ============================================================

def validate_environment():

    required = [
        "CF_ACCOUNT_ID",
        "CF_D1_DATABASE_ID",
        "CF_API_TOKEN",
    ]

    missing = [
        key
        for key in required
        if not os.getenv(key)
    ]

    if missing:

        print(
            "[FATAL] Environment variable "
            "belum lengkap:"
        )

        for key in missing:
            print(f" - {key}")

        sys.exit(1)


# ============================================================
# D1 API
# ============================================================

def get_d1_api_url():

    return (
        "https://api.cloudflare.com/client/v4/"
        f"accounts/{CF_ACCOUNT_ID}/"
        f"d1/database/{CF_D1_DATABASE_ID}/query"
    )


def d1_request(batch):

    url = get_d1_api_url()

    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "batch": batch
    }

    last_error = None

    for attempt in range(
        1,
        D1_RETRY_COUNT + 1,
    ):

        try:

            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=D1_REQUEST_TIMEOUT,
            )

            try:
                data = response.json()

            except Exception:

                data = {
                    "success": False,
                    "errors": [
                        {
                            "message": response.text[:1000]
                        }
                    ],
                }

            if response.status_code != 200:

                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{data}"
                )

                print(
                    f"[D1 ERROR] "
                    f"Attempt {attempt}/"
                    f"{D1_RETRY_COUNT}: "
                    f"{last_error}"
                )

                if attempt < D1_RETRY_COUNT:
                    time.sleep(
                        min(2 * attempt, 5)
                    )

                continue

            if not data.get(
                "success",
                False,
            ):

                last_error = str(
                    data.get("errors")
                )

                print(
                    f"[D1 ERROR] "
                    f"Attempt {attempt}/"
                    f"{D1_RETRY_COUNT}: "
                    f"{last_error}"
                )

                if attempt < D1_RETRY_COUNT:
                    time.sleep(
                        min(2 * attempt, 5)
                    )

                continue

            return data

        except requests.RequestException as exc:

            last_error = str(exc)

            print(
                f"[D1 NETWORK ERROR] "
                f"Attempt {attempt}/"
                f"{D1_RETRY_COUNT}: "
                f"{exc}"
            )

            if attempt < D1_RETRY_COUNT:
                time.sleep(
                    min(2 * attempt, 5)
                )

    raise RuntimeError(
        "D1 request gagal setelah "
        f"{D1_RETRY_COUNT} percobaan: "
        f"{last_error}"
    )


# ============================================================
# LOAD GRAPH FROM D1
# ============================================================

def load_page_graph():

    print(
        "[D1] Mengambil page_graph..."
    )

    batch = [
        {
            "sql": """
                SELECT source_url, target_url
                FROM page_graph
            """,
            "params": [],
        }
    ]

    data = d1_request(batch)

    results = data.get(
        "result",
        []
    )

    if not results:
        return []

    rows = results[0].get(
        "results",
        []
    )

    print(
        f"[D1] Graph edges: "
        f"{len(rows):,}"
    )

    return rows


# ============================================================
# BUILD GRAPH
# ============================================================

def build_graph(rows):

    print(
        "[PAGERANK] Membuat graph..."
    )

    graph = nx.DiGraph()

    for row in rows:

        source = row.get(
            "source_url"
        )

        target = row.get(
            "target_url"
        )

        if not source or not target:
            continue

        if source == target:
            continue

        graph.add_edge(
            source,
            target,
        )

    print(
        f"[PAGERANK] Nodes : "
        f"{graph.number_of_nodes():,}"
    )

    print(
        f"[PAGERANK] Edges : "
        f"{graph.number_of_edges():,}"
    )

    return graph


# ============================================================
# CALCULATE PAGERANK
# ============================================================

def calculate_pagerank(graph):

    if graph.number_of_nodes() == 0:

        print(
            "[PAGERANK] Graph kosong."
        )

        return {}

    print(
        "[PAGERANK] Menghitung PageRank..."
    )

    print(
        f"[PAGERANK] alpha = "
        f"{PAGERANK_ALPHA}"
    )

    print(
        f"[PAGERANK] max_iter = "
        f"{PAGERANK_MAX_ITER}"
    )

    try:

        scores = nx.pagerank(
            graph,
            alpha=PAGERANK_ALPHA,
            max_iter=PAGERANK_MAX_ITER,
            tol=PAGERANK_TOL,
        )

    except nx.PowerIterationFailedConvergence:

        print(
            "[PAGERANK] Konvergensi belum "
            "tercapai dengan parameter normal."
        )

        print(
            "[PAGERANK] Mencoba iterasi lebih tinggi..."
        )

        scores = nx.pagerank(
            graph,
            alpha=PAGERANK_ALPHA,
            max_iter=300,
            tol=PAGERANK_TOL,
        )

    print(
        f"[PAGERANK] "
        f"{len(scores):,} score berhasil dihitung."
    )

    return scores


# ============================================================
# UPDATE D1
# ============================================================

def update_pagerank_to_d1(scores):

    if not scores:

        print(
            "[D1] Tidak ada PageRank untuk di-update."
        )

        return

    items = list(
        scores.items()
    )

    total = len(items)

    print(
        f"[CLOUDFLARE PUSH] "
        f"Mengupdate {total:,} PageRank..."
    )

    success_count = 0

    total_batches = (
        total + D1_BATCH_SIZE - 1
    ) // D1_BATCH_SIZE

    for start in range(
        0,
        total,
        D1_BATCH_SIZE,
    ):

        chunk = items[
            start:start + D1_BATCH_SIZE
        ]

        batch = []

        for url, score in chunk:

            batch.append(
                {
                    "sql": UPDATE_SQL,
                    "params": [
                        float(score),
                        url,
                    ],
                }
            )

        try:

            d1_request(batch)

            success_count += len(chunk)

            current_batch = (
                start // D1_BATCH_SIZE
            ) + 1

            print(
                "[D1 PAGERANK] "
                f"Batch {current_batch}/"
                f"{total_batches} | "
                f"{success_count:,}/"
                f"{total:,}"
            )

        except Exception as exc:

            print(
                "[D1 PAGERANK ERROR]"
            )

            print(exc)

            print(
                "[D1] Proses update dihentikan "
                "agar tidak terus menghabiskan "
                "write quota."
            )

            return

    print(
        "[CLOUDFLARE PUSH] "
        f"PageRank berhasil diupdate: "
        f"{success_count:,}/{total:,}"
    )


# ============================================================
# SHOW TOP RESULTS
# ============================================================

def show_top_scores(scores):

    if not scores:
        return

    print()
    print(
        "=" * 60
    )

    print(
        "TOP PAGERANK"
    )

    print(
        "=" * 60
    )

    top = sorted(
        scores.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:20]

    for index, (url, score) in enumerate(
        top,
        start=1,
    ):

        print(
            f"{index:02d}. "
            f"{score:.10f} "
            f"{url}"
        )

    print(
        "=" * 60
    )


# ============================================================
# MAIN
# ============================================================

def main():

    started = time.monotonic()

    print(
        "=" * 60
    )

    print(
        "DEEVV SEARCH - PAGERANK"
    )

    print(
        "=" * 60
    )

    print(
        f"Batch size : {D1_BATCH_SIZE}"
    )

    print(
        f"Alpha      : {PAGERANK_ALPHA}"
    )

    print(
        f"Max iter   : {PAGERANK_MAX_ITER}"
    )

    print(
        "=" * 60
    )

    # --------------------------------------------------------
    # ENV ONLY
    # --------------------------------------------------------

    validate_environment()

    # --------------------------------------------------------
    # LOAD GRAPH
    # --------------------------------------------------------

    rows = load_page_graph()

    if not rows:

        print(
            "[PAGERANK] page_graph kosong."
        )

        print(
            "[PAGERANK] Tidak ada perubahan "
            "ke D1."
        )

        return

    # --------------------------------------------------------
    # BUILD GRAPH
    # --------------------------------------------------------

    graph = build_graph(rows)

    if graph.number_of_nodes() == 0:

        print(
            "[PAGERANK] Tidak ada node."
        )

        return

    # --------------------------------------------------------
    # CALCULATE
    # --------------------------------------------------------

    scores = calculate_pagerank(
        graph
    )

    # --------------------------------------------------------
    # SHOW TOP
    # --------------------------------------------------------

    show_top_scores(
        scores
    )

    # --------------------------------------------------------
    # WRITE BACK TO D1
    # --------------------------------------------------------

    update_pagerank_to_d1(
        scores
    )

    elapsed = (
        time.monotonic() - started
    )

    print()
    print(
        "=" * 60
    )

    print(
        "PAGERANK SELESAI"
    )

    print(
        f"Nodes       : "
        f"{graph.number_of_nodes():,}"
    )

    print(
        f"Edges       : "
        f"{graph.number_of_edges():,}"
    )

    print(
        f"Scores      : "
        f"{len(scores):,}"
    )

    print(
        f"Time        : "
        f"{elapsed:.2f}s"
    )

    print(
        "=" * 60
    )


if __name__ == "__main__":
    main()
