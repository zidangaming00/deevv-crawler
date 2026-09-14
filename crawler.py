import os
import re
import requests

# Mengambil variabel sensitif dari GitHub Secrets
CF_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID')
CF_DATABASE_ID = os.getenv('CF_D1_DATABASE_ID')
CF_API_TOKEN = os.getenv('CF_API_TOKEN')


def push_to_cloudflare_d1(crawled_data):
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[ERROR] Secret Cloudflare belum terkonfigurasi di GitHub!')
    return

  url = f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/raw'

  headers = {
      'Authorization': f'Bearer {CF_API_TOKEN}',
      'Content-Type': 'application/json',
  }

  statements = []
  for page in crawled_data:
    # Escape single quote (') agar SQL Query tidak error saat baca teks
    title = page.get('title', '').replace("'", "''")
    snippet = page.get('snippet', '').replace("'", "''")
    page_url = page.get('url', '').replace("'", "''")
    domain = page.get('domain', '').replace("'", "''")
    favicon = page.get('favicon', '').replace("'", "''")
    thumbnail = page.get('thumbnail', '').replace("'", "''")
    pagerank = page.get('pagerank', 0.0)

    # Clean text untuk virtual table FTS5
    clean_title = re.sub(r'[^\w\s]', '', title)
    clean_snippet = re.sub(r'[^\w\s]', '', snippet)

    sql = f"""
        INSERT INTO documents (url, domain, title, snippet, favicon, thumbnail, pagerank)
        VALUES ('{page_url}', '{domain}', '{title}', '{snippet}', '{favicon}', '{thumbnail}', {pagerank})
        ON CONFLICT(url) DO UPDATE SET 
            title=excluded.title, 
            snippet=excluded.snippet,
            favicon=excluded.favicon,
            thumbnail=excluded.thumbnail,
            pagerank=excluded.pagerank;
        
        INSERT INTO documents_fts(rowid, title, snippet)
        SELECT id, '{clean_title}', '{clean_snippet}' FROM documents WHERE url = '{page_url}';
        """
    statements.append(sql)

  if not statements:
    print('[INFO] Tidak ada data baru untuk di-push.')
    return

  # Kirim gabungan query ke D1
  full_sql = '\n'.join(statements)
  payload = {'sql': full_sql}

  response = requests.post(url, headers=headers, json=payload)

  if response.status_code == 200 and response.json().get('success'):
    print(
        f'[SUCCESS] Berhasil menyetor {len(crawled_data)} dokumen ke Cloudflare'
        ' D1!'
    )
  else:
    print(f'[ERROR] Gagal menyetor ke D1: {response.text}')


# Panggil fungsi ini di akhir eksekusi crawler kamu
# push_to_cloudflare_d1(hasil_crawl_list)
