import os
import re
import requests

# Secrets dari GitHub
CF_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID')
CF_DATABASE_ID = os.getenv('CF_D1_DATABASE_ID')
CF_API_TOKEN = os.getenv('CF_API_TOKEN')


def push_to_cloudflare_d1(crawled_data):
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[ERROR] Secret Cloudflare belum lengkap!')
    return

  url = f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/raw'
  headers = {
      'Authorization': f'Bearer {CF_API_TOKEN}',
      'Content-Type': 'application/json',
  }

  print(f'[INFO] Memulai penyetoran {len(crawled_data)} data ke D1...')

  for page in crawled_data:
    title = page.get('title', '').replace("'", "''")
    snippet = page.get('snippet', '').replace("'", "''")
    page_url = page.get('url', '').replace("'", "''")
    domain = page.get('domain', '').replace("'", "''")
    favicon = page.get('favicon', '').replace("'", "''")
    thumbnail = page.get('thumbnail', '').replace("'", "''")
    pagerank = page.get('pagerank', 0.0)

    clean_title = re.sub(r'[^\w\s]', '', title)
    clean_snippet = re.sub(r'[^\w\s]', '', snippet)

    sql_doc = f"""
        INSERT INTO documents (url, domain, title, snippet, favicon, thumbnail, pagerank)
        VALUES ('{page_url}', '{domain}', '{title}', '{snippet}', '{favicon}', '{thumbnail}', {pagerank})
        ON CONFLICT(url) DO UPDATE SET 
            title=excluded.title, 
            snippet=excluded.snippet,
            favicon=excluded.favicon,
            thumbnail=excluded.thumbnail,
            pagerank=excluded.pagerank;
        """

    res_doc = requests.post(url, headers=headers, json={'sql': sql_doc})

    sql_fts = f"""
        DELETE FROM documents_fts WHERE rowid = (SELECT id FROM documents WHERE url = '{page_url}');
        INSERT INTO documents_fts(rowid, title, snippet)
        SELECT id, '{clean_title}', '{clean_snippet}' FROM documents WHERE url = '{page_url}';
        """

    res_fts = requests.post(url, headers=headers, json={'sql': sql_fts})

    if res_doc.status_code == 200 and res_fts.status_code == 200:
      print(f'[SUCCESS] Ter-upload ke D1: {page_url}')
    else:
      print(f'[ERROR] Gagal upload {page_url}: {res_doc.text}')


# --- WAJIB DIPANGGIL DI BAGIAN PALING BAWAH ---
if __name__ == '__main__':
  # Dummy data untuk pengujian pertama kali (opsional untuk tes)
  data_tes = [{
      'url': 'https://minecraft.net',
      'domain': 'minecraft.net',
      'title': 'Minecraft Official Site',
      'snippet': 'Explore new gaming adventures in Minecraft',
      'favicon': 'https://minecraft.net/favicon.ico',
      'thumbnail': '',
      'pagerank': 0.08,
  }]

  # PANGGIL FUNGSI PUSH DISINI
  push_to_cloudflare_d1(data_tes)
