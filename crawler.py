import asyncio
import json
import os
import re
import sys
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
from bs4 import BeautifulSoup
import requests

# --- CONFIGURATION ---
MAX_RUN_SECONDS = 3600  # Maksimal 1 Jam per run
CONCURRENCY = 15  # 15 Pekerja simultan saat merayap

# Secrets dari GitHub Actions
CF_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID')
CF_DATABASE_ID = os.getenv('CF_D1_DATABASE_ID')
CF_API_TOKEN = os.getenv('CF_API_TOKEN')


class ProductionD1Crawler:

  def __init__(
      self,
      seed_urls,
      max_run_seconds=MAX_RUN_SECONDS,
      concurrency=CONCURRENCY,
  ):
    self.seed_urls = seed_urls
    self.max_run_seconds = max_run_seconds
    self.concurrency = concurrency

    self.queue = asyncio.PriorityQueue()
    self.visited_urls = set()
    self.visited_domains = set()
    self.domain_robots = {}
    self.pages_data = {}
    self.graph = {}
    self.inbound = {}
    
    self.domain_counts = {}
    self.MAX_PAGES_PER_DOMAIN = 40  # Cegah dominasi 1 situs

    self.start_time = time.time()

    self.headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
            ' DeevvBot/1.0'
        ),
        'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
    }

  # --- ANTI SPAM & URL SANITIZER ---
  def is_spam_domain(self, domain):
    # Memblokir TLD murah/gratis yang sering jadi Spider Trap
    spam_tlds = ('.cn', '.xyz', '.top', '.pw', '.tk', '.ml', '.ga', '.cf', '.gq', '.wang', '.icu')
    if any(domain.endswith(tld) for tld in spam_tlds):
      return True
    return False

  def clean_url_string(self, url):
    """
    Membuang query parameters (?q=...) dan fragment (#) dari URL
    """
    parsed = urlparse(url)
    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    # Hapus trailing slash jika bukan root domain (misal: /about/ -> /about)
    if len(clean_url) > len(f"{parsed.scheme}://{parsed.netloc}/") and clean_url.endswith('/'):
        clean_url = clean_url[:-1]
    return clean_url

  async def is_allowed_by_robots(self, session, url):
    parsed = urlparse(url)
    domain_base = f'{parsed.scheme}://{parsed.netloc}'

    if domain_base not in self.domain_robots:
      robots_url = f'{domain_base}/robots.txt'
      rfp = RobotFileParser()
      rfp.set_url(robots_url)

      try:
        # Timeout agresif untuk robots.txt
        async with session.get(robots_url, timeout=3, headers=self.headers) as resp:
          if resp.status == 200:
            content = await resp.text()
            rfp.parse(content.splitlines())
          else:
            rfp.allow_all = True
      except Exception:
        rfp.allow_all = True

      self.domain_robots[domain_base] = rfp

    return self.domain_robots[domain_base].can_fetch(self.headers['User-Agent'], url)

  def is_valid_url(self, url):
    parsed = urlparse(url)
    invalid_exts = (
        '.png', '.jpg', '.jpeg', '.gif', '.pdf', '.zip', 
        '.css', '.js', '.svg', '.mp4', '.mp3', '.webp', '.xml', '.json'
    )
    if any(parsed.path.lower().endswith(ext) for ext in invalid_exts):
      return False
    return bool(parsed.netloc) and parsed.scheme in ['http', 'https']

  def clean_text(self, text):
    return re.sub(r'\s+', ' ', text).strip()

  async def fetch(self, session, url):
    try:
      # Timeout dipersingkat menjadi 5 detik agar tidak hang di server lambat
      async with session.get(url, timeout=5, headers=self.headers, allow_redirects=True) as response:
        content_type = response.headers.get('Content-Type', '').lower()
        if response.status == 200 and 'text/html' in content_type:
          return await response.text()
        return None
    except Exception:
      return None

  async def process_page(self, url, html):
    soup = BeautifulSoup(html, 'html.parser')
    parsed_url = urlparse(url)
    domain_name = parsed_url.netloc

    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else domain_name

    meta_desc = (
        soup.find('meta', attrs={'name': 'description'})
        or soup.find('meta', attrs={'property': 'og:description'})
        or soup.find('meta', attrs={'name': 'twitter:description'})
    )
    description = (
        self.clean_text(meta_desc['content'])
        if meta_desc and meta_desc.get('content')
        else ''
    )

    icon_tag = soup.find('link', rel=lambda r: r and ('icon' in r.lower() or 'shortcut icon' in r.lower()))
    if icon_tag and icon_tag.get('href'):
      favicon = urljoin(url, icon_tag['href'])
    else:
      favicon = f'https://www.google.com/s2/favicons?domain={domain_name}&sz=64'

    og_image = soup.find('meta', attrs={'property': 'og:image'}) or soup.find('meta', attrs={'name': 'twitter:image'})
    thumbnail = urljoin(url, og_image['content']) if og_image and og_image.get('content') else ''

    for element in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'aside']):
      element.extract()

    main_content = soup.find('main') or soup.find('article') or soup.body
    body_text = ""
    if main_content:
      body_text = self.clean_text(main_content.get_text(separator=' '))
      
    # Logika Snippet: Prioritaskan Meta Description!
    if description and len(description) > 20:
      snippet = description
    elif body_text:
      snippet = body_text[:250] + '...' if len(body_text) > 250 else body_text
    else:
      snippet = title

    outgoing_links = set()
    for link in soup.find_all('a', href=True):
      raw_url = urljoin(url, link['href'])
      clean_url = self.clean_url_string(raw_url)

      if self.is_valid_url(clean_url):
        parsed_abs = urlparse(clean_url)
        target_domain = parsed_abs.netloc
        
        # Tolak domain spam
        if self.is_spam_domain(target_domain):
            continue

        outgoing_links.add(clean_url)
        root_domain_url = f'{parsed_abs.scheme}://{target_domain}/'

        # Eksekusi Root Domain terlebih dahulu
        if target_domain not in self.visited_domains and root_domain_url not in self.visited_urls:
          self.visited_domains.add(target_domain)
          await self.queue.put((0, root_domain_url))

        if target_domain not in self.domain_counts:
          self.domain_counts[target_domain] = 0

        # Cek batas maksimal halaman per domain
        if self.domain_counts[target_domain] < self.MAX_PAGES_PER_DOMAIN:
          if clean_url not in self.visited_urls:
            self.visited_urls.add(clean_url)
            self.domain_counts[target_domain] += 1
            
            # Sistem Prioritas: URL tanpa direktori/path panjang diutamakan
            path_segments = [p for p in parsed_abs.path.split('/') if p]
            priority_score = len(path_segments) * 10
            
            if target_domain.count('.') > 1 and "www" not in target_domain:
                priority_score -= 5
                
            await self.queue.put((priority_score, clean_url))

    self.pages_data[url] = {
        'url': url,
        'domain': domain_name,
        'title': title,
        'snippet': snippet,
        'favicon': favicon,
        'thumbnail': thumbnail,
        'pagerank': 0.0,
    }
    self.graph[url] = list(outgoing_links)

  async def worker(self, session):
    while True:
      if time.time() - self.start_time > self.max_run_seconds:
        break

      try:
        priority, url = await asyncio.wait_for(self.queue.get(), timeout=3.0)
      except asyncio.TimeoutError:
        if time.time() - self.start_time > self.max_run_seconds or self.queue.empty():
          break
        continue

      allowed = await self.is_allowed_by_robots(session, url)
      if not allowed:
        self.queue.task_done()
        continue

      elapsed = int(time.time() - self.start_time)
      if len(self.pages_data) % 50 == 0:
        print(f'[{elapsed}s/{self.max_run_seconds}s] [{len(self.pages_data)} indexed] Merayapi: {url}')

      html = await self.fetch(session, url)
      if html:
        await self.process_page(url, html)

      self.queue.task_done()

  async def run(self):
    for url in self.seed_urls:
      parsed = urlparse(url)
      self.visited_domains.add(parsed.netloc)
      # Seed selalu dapat antrean VVIP (0)
      await self.queue.put((0, self.clean_url_string(url)))

    # Pakai connector limit untuk mencegah Error: Too many open files
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
      tasks = [asyncio.create_task(self.worker(session)) for _ in range(self.concurrency)]
      await asyncio.gather(*tasks, return_exceptions=True)

  def calculate_pagerank(self, damping_factor=0.85, iterations=15):
    print(f'\n[PAGERANK] Menghitung PageRank untuk {len(self.pages_data)} halaman...')
    for node in self.pages_data.keys():
      self.inbound[node] = []

    for source, targets in self.graph.items():
      for target in targets:
        if target in self.inbound:
          self.inbound[target].append(source)

    N = len(self.pages_data)
    if N == 0: return

    initial_pr = 1.0 / N
    for url in self.pages_data:
      self.pages_data[url]['pagerank'] = initial_pr

    for _ in range(iterations):
      new_pr = {}
      for url in self.pages_data:
        rank_sum = sum(
            (self.pages_data[in_node]['pagerank'] / max(len(self.graph.get(in_node, [])), 1))
            for in_node in self.inbound.get(url, []) if in_node in self.pages_data
        )
        new_pr[url] = ((1 - damping_factor) / N) + (damping_factor * rank_sum)
      for url in new_pr:
        self.pages_data[url]['pagerank'] = new_pr[url]

    # --- DOMAIN AUTHORITY BOOST ---
    # Mendongkrak nilai PR untuk domain besar dan seed urls agar selalu tampil duluan di pencarian
    print("[PAGERANK] Menyuntikkan bobot Domain Authority...")
    seed_domains = {urlparse(seed).netloc for seed in self.seed_urls}
    for url in self.pages_data:
        domain = self.pages_data[url]['domain']
        if domain in seed_domains or "google" in domain or "wikipedia" in domain:
             self.pages_data[url]['pagerank'] *= 15.0  # Bobot x15 untuk website utama


def get_already_visited_urls():
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[CRITICAL ERROR] Secrets Cloudflare D1 belum terpasang!')
    sys.exit(1)

  url_d1 = f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query'
  headers = {'Authorization': f'Bearer {CF_API_TOKEN}', 'Content-Type': 'application/json'}

  visited = set()
  limit = 2500
  offset = 0
  max_retries = 3

  print('\n[D1 SYNC] Mengambil daftar URL lama dari Cloudflare D1...')

  while True:
    success = False
    for attempt in range(max_retries):
      try:
        sql_query = f'SELECT url FROM documents LIMIT {limit} OFFSET {offset}'
        res = requests.post(url_d1, headers=headers, json={'sql': sql_query}, timeout=30)
        
        if res.status_code == 200:
          data = res.json()
          if data.get('success'):
            results_data = data.get('result', [{}])[0].get('results', [])
            rows = results_data['rows'] if isinstance(results_data, dict) and 'rows' in results_data else results_data
            
            for row in rows:
              if isinstance(row, dict) and 'url' in row:
                visited.add(row['url'])
              elif isinstance(row, (list, tuple)) and len(row) > 0:
                visited.add(row[0])

            print(f'[D1 SYNC] Terbaca: {len(visited)} URL')
            success = True
            
            if len(rows) < limit:
              print(f'[D1 SYNC DONE] Total {len(visited)} URL lama masuk memory!')
              return visited

            offset += limit
            break
      except Exception as e:
        print(f'[D1 SYNC WARNING] Error jaringan: {e}')

      time.sleep(3)

    if not success:
      print(f'\n[CRITICAL ERROR] Gagal sync D1 di offset {offset}! Mencegah duplikasi...')
      sys.exit(1)

  return visited


async def push_single_page_async(session, url_d1_query, headers, page):
  title = page.get('title', '').replace("'", "''").replace("\n", " ")
  snippet = page.get('snippet', '').replace("'", "''").replace("\n", " ")
  page_url = page.get('url', '').replace("'", "''")
  domain = page.get('domain', '').replace("'", "''")
  favicon = page.get('favicon', '').replace("'", "''")
  thumbnail = page.get('thumbnail', '').replace("'", "''")
  pagerank = page.get('pagerank', 0.0)

  clean_title = re.sub(r'[^\w\s]', ' ', title)
  clean_snippet = re.sub(r'[^\w\s]', ' ', snippet)

  payload = [
      {
          'sql': f"""
            INSERT INTO documents (url, domain, title, snippet, favicon, thumbnail, pagerank)
            VALUES ('{page_url}', '{domain}', '{title}', '{snippet}', '{favicon}', '{thumbnail}', {pagerank})
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                snippet=excluded.snippet,
                favicon=excluded.favicon,
                thumbnail=excluded.thumbnail,
                pagerank=excluded.pagerank;
        """
      },
      {
          'sql': f"DELETE FROM documents_fts WHERE rowid = (SELECT id FROM documents WHERE url = '{page_url}');"
      },
      {
          'sql': f"""
            INSERT INTO documents_fts(rowid, title, snippet)
            SELECT id, '{clean_title}', '{clean_snippet}' FROM documents WHERE url = '{page_url}';
        """
      }
  ]

  body = {"batch": payload}

  try:
    async with session.post(url_d1_query, headers=headers, json=body) as response:
      if response.status == 200:
          return True
      
      text = await response.text()
      # Jika limit harian D1 habis, lempar error spesifik agar tidak membuang kuota Action
      if "exceeded D1's free tier" in text:
          raise Exception("LIMIT_REACHED")
          
      print(f'[D1 PUSH ERROR] Gagal push {page_url} -> {text[:200]}')
      return False
  except Exception as e:
      if str(e) == "LIMIT_REACHED":
          raise e # Lemparkan ke atas
      return False


async def push_to_cloudflare_d1_async(crawled_data):
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[ERROR] Secrets Cloudflare D1 belum terpasang!')
    return

  url_d1_query = f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query'
  headers = {'Authorization': f'Bearer {CF_API_TOKEN}', 'Content-Type': 'application/json'}

  print(f'\n[D1 PUSH] Memulai upload {len(crawled_data)} dokumen ke D1...')
  
  # Turunkan load koneksi ke API D1 biar tidak di-rate-limit
  semaphore = asyncio.Semaphore(15)
  success_count = 0

  async def sem_push(session, page):
    nonlocal success_count
    async with semaphore:
      try:
          res = await push_single_page_async(session, url_d1_query, headers, page)
          if res: success_count += 1
      except Exception as e:
          if str(e) == "LIMIT_REACHED":
              print("\n[ALERT] Kuota tulis harian Cloudflare D1 habis (100.000 rows/day). Sisa batch dibatalkan.")
              # Batalkan paksa sisa tugas
              for t in asyncio.all_tasks():
                  t.cancel()

  connector = aiohttp.TCPConnector(limit=30)
  async with aiohttp.ClientSession(connector=connector) as session:
    tasks = [sem_push(session, page) for page in crawled_data]
    await asyncio.gather(*tasks, return_exceptions=True)

  print(f'[D1 FINISH] Selesai! {success_count} dokumen baru berhasil disimpan.')


if __name__ == '__main__':
  # Seed URLs Termasuk Google
  initial_seeds = [
      'https://www.google.com',
      'https://www.google.co.id',
      'https://duckduckgo.com',
      'https://www.bing.com',
      'https://www.yahoo.com',
      'https://www.ecosia.org',
      'https://id.wikipedia.org',
      'https://www.kompas.com',
      'https://www.detik.com',
      'https://www.liputan6.com',
      'https://www.tribunnews.com',
      'https://www.cnnindonesia.com',
      'https://www.theverge.com',
      'https://techcrunch.com',
      'https://github.com',
      'https://stackoverflow.com',
      'https://www.bola.net',
      'https://www.bolasport.com',
      'https://www.goal.com/id',
      'https://oneesports.gg/id',
      'https://www.hltv.org',
      'https://liquipedia.net',
      'https://www.ign.com',
      'https://www.gamespot.com',
      'https://www.minecraft.net',
      'https://store.steampowered.com',
      'https://www.epicgames.com',
      'https://www.roblox.com',
      'https://m.mobilelegends.com',
      'https://ff.garena.com',
      'https://www.codashop.com/id-id',
      'https://www.unipin.com',
      'https://www.itemku.com',
      'https://kiosgamer.co.id',
      'https://www.kaskus.co.id',
      'https://id.quora.com',
      'https://brainly.co.id',
      'https://www.reddit.com',
      'https://stackexchange.com',
      'https://www.minecraftforum.net',
      'https://indonesia.go.id',
      'https://www.kemdikbud.go.id',
      'https://www.kominfo.go.id',
      'https://www.setneg.go.id',
      'https://www.bps.go.id',
      'https://www.pajak.go.id',
      'https://kampusmerdeka.kemdikbud.go.id',
      'https://www.ui.ac.id',
      'https://www.itb.ac.id',
      'https://www.ugm.ac.id',
      'https://www.ut.ac.id',
      'https://developer.mozilla.org',
      'https://www.w3schools.com',
      'https://dev.to',
      'https://medium.com',
      'https://www.behance.net',
      'https://dribbble.com',
      'https://id.pinterest.com',
  ]

  existing_urls = get_already_visited_urls()

  crawler = ProductionD1Crawler(
      seed_urls=initial_seeds,
      max_run_seconds=3600,
      concurrency=15,
  )

  crawler.visited_urls.update(existing_urls)
  asyncio.run(crawler.run())
  crawler.calculate_pagerank()

  crawled_results = list(crawler.pages_data.values())
  if crawled_results:
      asyncio.run(push_to_cloudflare_d1_async(crawled_results))
  else:
      print("\n[INFO] Tidak ada halaman baru yang dicrawl. Skip upload.")
