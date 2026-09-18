import asyncio
import json
import os
from collections import Counter

import aiohttp
import libsql_client
from bs4 import BeautifulSoup


TURSO_URL = os.environ.get("TURSO_DATABASE_URL")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN")

CONCURRENT_REQUESTS = 50
BATCH_SIZE = 100
TIMEOUT_SECONDS = 8


# ============================================================
# FETCH DAN DETEKSI LAST MODIFIED
# ============================================================

async def fetch_last_modified(session, url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DeevvBot/1.0)"
    }

    try:
        # ====================================================
        # 1. HTTP Last-Modified
        #
        # PENTING:
        # HANYA Last-Modified yang digunakan.
        #
        # HTTP Date SENGAJA TIDAK DIGUNAKAN karena itu adalah
        # waktu response server, bukan waktu halaman diperbarui.
        # ====================================================

        async with session.head(
            url,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
            headers=headers,
            allow_redirects=True
        ) as resp:

            last_modified = resp.headers.get("Last-Modified")

            if last_modified:
                return {
                    "url": url,
                    "value": last_modified.strip(),
                    "source": "http_last_modified"
                }

        # ====================================================
        # 2. GET halaman
        # ====================================================

        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
            headers=headers,
            allow_redirects=True
        ) as resp:

            if resp.status != 200:
                return {
                    "url": url,
                    "value": None,
                    "source": f"http_status_{resp.status}"
                }

            html = await resp.text(errors="ignore")

        soup = BeautifulSoup(html, "html.parser")

        # ====================================================
        # 3. JSON-LD dateModified
        # ====================================================

        jsonld_dates = []

        for script in soup.find_all(
            "script",
            attrs={"type": "application/ld+json"}
        ):
            raw = script.string or script.get_text(strip=True)

            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                continue

            def collect_date_modified(obj):
                if isinstance(obj, dict):

                    value = obj.get("dateModified")

                    if isinstance(value, str) and value.strip():
                        jsonld_dates.append(value.strip())

                    graph = obj.get("@graph")

                    if isinstance(graph, list):
                        for item in graph:
                            collect_date_modified(item)

                elif isinstance(obj, list):

                    for item in obj:
                        collect_date_modified(item)

            collect_date_modified(data)

        if jsonld_dates:
            return {
                "url": url,
                "value": jsonld_dates[0],
                "source": "jsonld_date_modified"
            }

        # ====================================================
        # 4. article:modified_time
        # ====================================================

        meta = soup.find(
            "meta",
            attrs={
                "property": "article:modified_time"
            }
        )

        if meta and meta.get("content"):

            return {
                "url": url,
                "value": meta["content"].strip(),
                "source": "meta_article_modified_time"
            }

        # ====================================================
        # 5. Meta name yang secara eksplisit menyatakan
        #    modified / last modified
        # ====================================================

        modified_meta_names = [
            "last-modified",
            "lastmodified",
            "last_modified",
            "modified",
            "modified-date",
            "modified_date",
            "date-modified",
            "date_modified"
        ]

        for name in modified_meta_names:

            meta = soup.find(
                "meta",
                attrs={
                    "name": name
                }
            )

            if meta and meta.get("content"):

                return {
                    "url": url,
                    "value": meta["content"].strip(),
                    "source": f"meta_name_{name}"
                }

        # ====================================================
        # 6. Meta property yang secara eksplisit menyatakan
        #    modified
        # ====================================================

        modified_meta_properties = [
            "last-modified",
            "lastmodified",
            "dateModified",
            "dateModifiedTime"
        ]

        for prop in modified_meta_properties:

            meta = soup.find(
                "meta",
                attrs={
                    "property": prop
                }
            )

            if meta and meta.get("content"):

                return {
                    "url": url,
                    "value": meta["content"].strip(),
                    "source": f"meta_property_{prop}"
                }

        # ====================================================
        # 7. <time datetime="">
        #
        # Hanya digunakan jika konteksnya jelas menunjukkan
        # updated / modified.
        # ====================================================

        modified_keywords = [
            "updated",
            "update",
            "modified",
            "modification",
            "last modified",
            "last updated",
            "diperbarui",
            "diperbaharui",
            "diubah",
            "terakhir diperbarui"
        ]

        for time_tag in soup.find_all("time"):

            datetime_value = time_tag.get("datetime")

            if not datetime_value:
                continue

            text = time_tag.get_text(
                " ",
                strip=True
            ).lower()

            classes = " ".join(
                time_tag.get("class", [])
            ).lower()

            parent_text = ""

            if time_tag.parent:

                parent_text = time_tag.parent.get_text(
                    " ",
                    strip=True
                ).lower()[:300]

            context = (
                f"{text} "
                f"{classes} "
                f"{parent_text}"
            )

            if any(
                keyword in context
                for keyword in modified_keywords
            ):

                return {
                    "url": url,
                    "value": datetime_value.strip(),
                    "source": "time_modified"
                }

        # ====================================================
        # Tidak ditemukan last modified
        #
        # JANGAN menggunakan:
        # - HTTP Date
        # - datePublished
        # - article:published_time
        # ====================================================

        return {
            "url": url,
            "value": None,
            "source": "not_found"
        }

    except asyncio.TimeoutError:

        return {
            "url": url,
            "value": None,
            "source": "timeout"
        }

    except aiohttp.ClientError:

        return {
            "url": url,
            "value": None,
            "source": "connection_error"
        }

    except Exception:

        return {
            "url": url,
            "value": None,
            "source": "unknown_error"
        }


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TURSO_URL or not TURSO_TOKEN:

        print("Error: Kredensial Turso tidak ditemukan!")
        return

    client = libsql_client.create_client_sync(
        url=TURSO_URL,
        auth_token=TURSO_TOKEN
    )

    print("Mencari URL yang belum memiliki last_modified...")

    result = client.execute("""
        SELECT DISTINCT url
        FROM documents
        WHERE last_modified IS NULL
        LIMIT 30440
    """)

    urls = [
        row[0]
        for row in result.rows
        if row[0]
    ]

    print(
        f"Ditemukan {len(urls)} URL unik."
    )

    print(
        f"Memulai sinkronisasi dengan "
        f"{CONCURRENT_REQUESTS} koneksi..."
    )

    connector = aiohttp.TCPConnector(
        limit=CONCURRENT_REQUESTS,
        limit_per_host=5,
        ttl_dns_cache=300
    )

    timeout = aiohttp.ClientTimeout(
        total=TIMEOUT_SECONDS
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        semaphore = asyncio.Semaphore(
            CONCURRENT_REQUESTS
        )

        async def worker(url):

            async with semaphore:

                return await fetch_last_modified(
                    session,
                    url
                )

        tasks = [
            worker(url)
            for url in urls
        ]

        results = await asyncio.gather(
            *tasks
        )

    # ========================================================
    # STATISTIK
    # ========================================================

    stats = Counter(
        item["source"]
        for item in results
    )

    # Gabungkan semua sumber META
    meta_count = 0

    for source, count in stats.items():

        if (
            source.startswith("meta_name_")
            or
            source.startswith("meta_property_")
        ):
            meta_count += count

    http_error_count = 0

    for source, count in stats.items():

        if source.startswith("http_status_"):
            http_error_count += count

    print()
    print("========================================")
    print("HASIL PEMERIKSAAN")
    print("========================================")

    print(
        f"Total URL diperiksa : {len(urls)}"
    )

    print(
        f"HTTP Last-Modified  : "
        f"{stats['http_last_modified']}"
    )

    print(
        f"JSON-LD dateModified: "
        f"{stats['jsonld_date_modified']}"
    )

    print(
        f"Meta modified       : "
        f"{stats['meta_article_modified_time'] + meta_count}"
    )

    print(
        f"<time> modified     : "
        f"{stats['time_modified']}"
    )

    print(
        f"Tidak ditemukan     : "
        f"{stats['not_found']}"
    )

    print(
        f"Timeout             : "
        f"{stats['timeout']}"
    )

    print(
        f"Connection error    : "
        f"{stats['connection_error']}"
    )

    print(
        f"HTTP error          : "
        f"{http_error_count}"
    )

    print(
        f"Error lainnya       : "
        f"{stats['unknown_error']}"
    )

    # ========================================================
    # BUAT UPDATE
    # ========================================================

    updates = []

    for item in results:

        if item["value"]:

            updates.append(
                libsql_client.Statement(
                    """
                    UPDATE documents
                    SET last_modified = ?
                    WHERE url = ?
                    AND last_modified IS NULL
                    """,
                    [
                        item["value"],
                        item["url"]
                    ]
                )
            )

    print()
    print("========================================")
    print("PENYIMPANAN")
    print("========================================")

    print(
        f"Last modified valid ditemukan : "
        f"{len(updates)}"
    )

    if not updates:

        print(
            "Tidak ada last_modified baru "
            "yang perlu disimpan."
        )

        client.close()
        return

    success_batches = 0
    failed_batches = 0

    for i in range(
        0,
        len(updates),
        BATCH_SIZE
    ):

        batch = updates[
            i:i + BATCH_SIZE
        ]

        batch_number = (
            i // BATCH_SIZE
        ) + 1

        try:

            client.batch(batch)

            success_batches += 1

            print(
                f"Batch {batch_number} berhasil "
                f"({len(batch)} update)"
            )

        except Exception as e:

            failed_batches += 1

            print(
                f"Batch {batch_number} GAGAL: {e}"
            )

    print()
    print("========================================")
    print("SELESAI")
    print("========================================")

    print(
        f"URL diperiksa       : {len(urls)}"
    )

    print(
        f"Last modified valid : {len(updates)}"
    )

    print(
        f"Batch berhasil      : {success_batches}"
    )

    print(
        f"Batch gagal         : {failed_batches}"
    )

    if failed_batches == 0:

        print(
            "Semua update berhasil disimpan ke Turso."
        )

    else:

        print(
            "Ada batch yang gagal disimpan."
        )

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
