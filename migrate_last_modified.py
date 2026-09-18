import asyncio
import os
import aiohttp
import libsql_client
from bs4 import BeautifulSoup

TURSO_URL = os.environ.get('TURSO_DATABASE_URL')
TURSO_TOKEN = os.environ.get('TURSO_AUTH_TOKEN')


async def fetch_last_modified(session, url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; DeevvBot/1.0)'
    }

    try:
        # Metode 1: Cek Header HTTP
        async with session.head(
            url,
            timeout=5,
            headers=headers,
            allow_redirects=True
        ) as resp:

            lm = resp.headers.get('Last-Modified')

            if lm:
                return url, lm

        # Metode 2: Cek Meta Tags
        async with session.get(
            url,
            timeout=5,
            headers=headers
        ) as resp:

            if resp.status == 200:
                html = await resp.text(errors='ignore')

                soup = BeautifulSoup(html, 'html.parser')

                meta_mod = (
                    soup.find(
                        'meta',
                        attrs={'property': 'article:modified_time'}
                    )
                    or
                    soup.find(
                        'meta',
                        attrs={'property': 'article:published_time'}
                    )
                )

                if meta_mod and meta_mod.get('content'):
                    return url, meta_mod['content']

    except Exception:
        pass

    return url, None


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

    urls = [row[0] for row in result.rows if row[0]]

    print(f"Ditemukan {len(urls)} URL unik. Memulai sinkronisasi cepat...")

    updates = []

    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = [
            fetch_last_modified(session, url)
            for url in urls
        ]

        results = await asyncio.gather(*tasks)

        for url, lm in results:

            if lm:
                updates.append(
                    libsql_client.Statement(
                        """
                        UPDATE documents
                        SET last_modified = ?
                        WHERE url = ?
                        AND last_modified IS NULL
                        """,
                        [lm, url]
                    )
                )

    print(
        f"Berhasil mendapatkan {len(updates)} tanggal "
        f"dari {len(urls)} URL unik."
    )

    if updates:

        print("Menyimpan ke Turso...")

        success_batches = 0
        failed_batches = 0

        for i in range(0, len(updates), 100):

            batch = updates[i:i + 100]

            try:
                client.batch(batch)
                success_batches += 1

            except Exception as e:
                failed_batches += 1
                print(
                    f"Error simpan batch "
                    f"{i // 100 + 1}: {e}"
                )

        print()
        print("=== HASIL ===")
        print(f"URL target       : {len(urls)}")
        print(f"Dapat tanggal    : {len(updates)}")
        print(f"Batch berhasil   : {success_batches}")
        print(f"Batch gagal      : {failed_batches}")

        if failed_batches == 0:
            print("Semua batch berhasil dikirim ke Turso.")
        else:
            print(
                "Ada batch yang gagal. "
                "Jangan tampilkan 'Sukses' sebagai hasil akhir."
            )

    else:
        print("Tidak ada data baru yang didapat.")

    client.close()


if __name__ == "__main__":
    asyncio.run(main())
