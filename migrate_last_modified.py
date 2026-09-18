import asyncio
import os
import aiohttp
import libsql_client
from bs4 import BeautifulSoup

TURSO_URL = os.environ.get('TURSO_DATABASE_URL')
TURSO_TOKEN = os.environ.get('TURSO_AUTH_TOKEN')

async def fetch_last_modified(session, url):
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; DeevvBot/1.0)'}
    try:
        # Metode 1: Cek Header HTTP (Sangat Ringan & Cepat)
        async with session.head(url, timeout=5, headers=headers, allow_redirects=True) as resp:
            lm = resp.headers.get('Last-Modified') or resp.headers.get('Date')
            if lm: return url, lm
            
        # Metode 2: Cek Meta Tags Artikel jika Header kosong
        async with session.get(url, timeout=5, headers=headers) as resp:
            if resp.status == 200:
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')
                meta_mod = soup.find('meta', attrs={'property': 'article:modified_time'}) or soup.find('meta', attrs={'property': 'article:published_time'})
                if meta_mod and meta_mod.get('content'):
                    return url, meta_mod['content']
    except Exception:
        pass
    
    return url, None

async def main():
    if not TURSO_URL or not TURSO_TOKEN:
        print("Error: Kredensial Turso tidak ditemukan!")
        return

    client = libsql_client.create_client_sync(url=TURSO_URL, auth_token=TURSO_TOKEN)
    
    print("Mencari URL yang belum memiliki last_modified...")
    result = client.execute("SELECT url FROM documents WHERE last_modified IS NULL LIMIT 30440")
    urls = [row[0] for row in result.rows]
    print(f"Ditemukan {len(urls)} target. Memulai sinkronisasi cepat...")

    updates = []
    connector = aiohttp.TCPConnector(limit=50) # 50 koneksi sekaligus
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_last_modified(session, u) for u in urls]
        # Proses pararel
        results = await asyncio.gather(*tasks)

        for url, lm in results:
            if lm:
                updates.append(libsql_client.Statement(
                    "UPDATE documents SET last_modified = ? WHERE url = ?", [lm, url]
                ))
    
    if updates:
        print(f"Menyimpan {len(updates)} tanggal baru ke Turso...")
        for i in range(0, len(updates), 100):
            try:
                client.batch(updates[i:i+100])
            except Exception as e:
                print(f"Error simpan batch: {e}")
        print("Sukses!")
    else:
        print("Tidak ada data baru yang didapat.")

    client.close()

if __name__ == "__main__":
    asyncio.run(main())
