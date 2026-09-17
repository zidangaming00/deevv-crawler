import asyncio
import json
import math
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
MAX_RUN_SECONDS = 3600  # Maksimal 1 Jam Execution Time
CONCURRENCY = 15        # 15 Pekerja simultan
MAX_URL_LENGTH = 200    # Batas panjang URL untuk cegah spider trap
MAX_PATH_DEPTH = 6      # Maksimal kedalaman direktori (/a/b/c/d/e/f)
MAX_PAGES_PER_DOMAIN = 40  # Cegah 1 situs mendominasi indeks

# Domain yang boleh dapat boost "authority" - exact match (bukan substring!)
# supaya "mygoogleaccount.tk" dkk nggak ikut ke-boost.
AUTHORITY_DOMAINS_SUFFIX = ('google.com', 'wikipedia.org')

# Secrets dari Cloudflare D1 via Environment Variables
CF_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID')
CF_DATABASE_ID = os.getenv('CF_D1_DATABASE_ID')
CF_API_TOKEN = os.getenv('CF_API_TOKEN')


class ProfessionalSearchCrawler:

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
    # --- [FIX BUG FATAL] ---
    # Sebelumnya baris ini nggak ada, padahal process_page() manggil
    # self.MAX_PAGES_PER_DOMAIN. Tanpa ini -> AttributeError di HAMPIR
    # SETIAP halaman (karena hampir semua halaman punya link keluar),
    # bikin tiap worker mati diam-diam (ketelan asyncio.gather return_exceptions=True)
    # begitu dapat halaman pertama yang punya link normal.
    self.MAX_PAGES_PER_DOMAIN = MAX_PAGES_PER_DOMAIN

    self.active_workers = 0
    self.worker_lock = asyncio.Lock()

    self.start_time = time.time()

    self.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }

  def is_spam_domain(self, domain):
    spam_tlds = (
        '.cn', '.xyz', '.top', '.pw', '.tk', '.ml', '.ga', '.cf', '.gq',
        '.wang', '.icu', '.best', '.monster', '.work', '.click', '.loan'
    )
    return any(domain.endswith(tld) for tld in spam_tlds)

  def is_authority_domain(self, domain):
    # Exact suffix match (dengan titik di depan) - bukan substring "in" biasa,
    # supaya "mygoogleaccount.tk" atau "wikipedia-fake.xyz" TIDAK ikut ke-boost.
    return any(
        domain == suf or domain.endswith('.' + suf)
        for suf in AUTHORITY_DOMAINS_SUFFIX
    )

  def is_spider_trap(self, url):
    parsed = urlparse(url)
    path = parsed.path.lower()

    if len(url) > MAX_URL_LENGTH:
      return True

    path_segments = [p for p in path.split('/') if p]
    if len(path_segments) > MAX_PATH_DEPTH:
      return True

    if re.search(r'/(.+?)/\1/', path):
      return True

    trap_keywords = (
        'login', 'register', 'signup', 'signin', 'logout', 'cart', 'checkout',
        'add-to-cart', 'replytocom', 'wp-json', 'xmlrpc.php', 'calendar',
        'event', 'archive', 'share.php', 'print', 'action=', 'do=', 'redirect=',
        'goto=', 'feed/', 'rss/', 'trackback/'
    )
    if any(keyword in url.lower() for keyword in trap_keywords):
      return True

    return False

  def clean_url_string(self, url):
    parsed = urlparse(url)
    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if len(clean_url) > len(f"{parsed.scheme}://{parsed.netloc}/") and clean_url.endswith('/'):
      clean_url = clean_url[:-1]
    return clean_url

  async def get_robots_rules(self, session, url):
    parsed = urlparse(url)
    domain_base = f'{parsed.scheme}://{parsed.netloc}'

    if domain_base not in self.domain_robots:
      robots_url = f'{domain_base}/robots.txt'
      rfp = RobotFileParser()
      rfp.set_url(robots_url)

      try:
        async with session.get(robots_url, timeout=4, headers=self.headers) as resp:
          if resp.status == 200:
            content = await resp.text()
            rfp.parse(content.splitlines())
          else:
            rfp.allow_all = True
      except Exception:
        rfp.allow_all = True

      self.domain_robots[domain_base] = rfp

    return self.domain_robots[domain_base]

  def is_valid_url(self, url):
    parsed = urlparse(url)
    invalid_exts = (
        '.png', '.jpg', '.jpeg', '.gif', '.pdf', '.zip', '.rar', '.7z',
        '.css', '.js', '.svg', '.mp4', '.mp3', '.webp', '.xml', '.json',
        '.ico', '.exe', '.dmg', '.iso', '.csv', '.xlsx', '.doc', '.docx'
    )
    if any(parsed.path.lower().endswith(ext) for ext in invalid_exts):
      return False
    return bool(parsed.netloc) and parsed.scheme in ['http', 'https']

  def clean_text(self, text):
    return re.sub(r'\s+', ' ', text).strip()

  async def fetch(self, session, url):
    """Mengembalikan (final_url, html) - final_url = URL setelah redirect,
    biar halaman yang di-redirect (http->https, non-www->www, dst) disimpan
    dengan alamat yang benar, bukan alamat lama sebelum redirect."""
    try:
      async with session.get(url, timeout=8, headers=self.headers, allow_redirects=True) as response:
        content_type = response.headers.get('Content-Type', '').lower()
        if response.status == 200 and 'text/html' in content_type:
          html = await response.text()
          final_url = self.clean_url_string(str(response.url))
          return final_url, html
        else:
          print(f"[FETCH ERROR] Status {response.status} -> {url}")
          return None, None
    except Exception:
      return None, None

  async def process_page(self, url, html):
    soup = BeautifulSoup(html, 'html.parser')
    domain_name = urlparse(url).netloc

    # --- [BARU] Hormati <meta name="robots" content="noindex"> ---
    robots_meta = soup.find('meta', attrs={'name': re.compile(r'^robots$', re.I)})
    if robots_meta and robots_meta.get('content') and 'noindex' in robots_meta['content'].lower():
      return  # situs eksplisit minta jangan diindex - jangan disimpan

    title_tag = soup.find('title')
    title = self.clean_text(title_tag.get_text()) if title_tag else domain_name

    snippet = ""
    meta_desc = (
        soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)}) or
        soup.find('meta', attrs={'property': re.compile(r'^og:description$', re.I)}) or
        soup.find('meta', attrs={'name': re.compile(r'^twitter:description$', re.I)})
    )

    if meta_desc and meta_desc.get('content'):
      cand = self.clean_text(meta_desc['content'])
      if len(cand) > 30:
        snippet = cand

    if not snippet:
      for element in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'aside', 'form', 'button', 'svg']):
        element.extract()

      main_content = soup.find('main') or soup.find('article') or soup.find(id=re.compile(r'content|main', re.I)) or soup.body
      if main_content:
        paragraphs = main_content.find_all('p')
        valid_paragraphs = []
        for p in paragraphs:
          txt = self.clean_text(p.get_text())
          if len(txt) > 35:
            valid_paragraphs.append(txt)

        if valid_paragraphs:
          combined_text = " ... ".join(valid_paragraphs)
          snippet = combined_text[:160] + '...' if len(combined_text) > 160 else combined_text
        else:
          raw_text = self.clean_text(main_content.get_text(separator=' '))
          snippet = raw_text[:160] + '...' if len(raw_text) > 160 else raw_text

    if not snippet:
      snippet = title

    icon_tag = soup.find('link', rel=lambda r: r and ('icon' in r.lower()))
    favicon = urljoin(url, icon_tag['href']) if (icon_tag and icon_tag.get('href')) else f'https://www.google.com/s2/favicons?domain={domain_name}&sz=64'

    og_image = soup.find('meta', attrs={'property': lambda x: x and x.lower() == 'og:image'})
    thumbnail = urljoin(url, og_image['content']) if (og_image and og_image.get('content')) else ''

    outgoing_links = set()
    for link in soup.find_all('a', href=True):
      # --- [BARU] Jangan ikuti link rel="nofollow" (konvensi standar Google/Bing) ---
      rel_attr = link.get('rel')
      if rel_attr and 'nofollow' in [r.lower() for r in rel_attr]:
        continue

      raw_url = urljoin(url, link['href'])
      parsed_raw = urlparse(raw_url)

      if parsed_raw.query:
        continue

      clean_url = self.clean_url_string(raw_url)

      if self.is_valid_url(clean_url):
        target_domain = urlparse(clean_url).netloc

        if self.is_spam_domain(target_domain) or self.is_spider_trap(clean_url):
          continue

        outgoing_links.add(clean_url)
        root_domain_url = f'{urlparse(clean_url).scheme}://{target_domain}/'

        if target_domain not in self.visited_domains and root_domain_url not in self.visited_urls:
          self.visited_domains.add(target_domain)
          await self.queue.put((0, root_domain_url))

        if target_domain not in self.domain_counts:
          self.domain_counts[target_domain] = 0

        if self.domain_counts[target_domain] < self.MAX_PAGES_PER_DOMAIN:
          if clean_url not in self.visited_urls:
            self.visited_urls.add(clean_url)
            self.domain_counts[target_domain] += 1

            path_segments = [p for p in urlparse(clean_url).path.split('/') if p]
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
        priority, url = await asyncio.wait_for(self.queue.get(), timeout=2.0)
      except asyncio.TimeoutError:
        if time.time() - self.start_time > self.max_run_seconds:
          break
        async with self.worker_lock:
          if self.queue.empty() and self.active_workers == 0:
            break
        continue

      async with self.worker_lock:
        self.active_workers += 1

      try:
        robots_rules = await self.get_robots_rules(session, url)
        if robots_rules.can_fetch(self.headers['User-Agent'], url):
          crawl_delay = robots_rules.crawl_delay(self.headers['User-Agent'])
          if crawl_delay:
            await asyncio.sleep(crawl_delay)

          elapsed = int(time.time() - self.start_time)
          if len(self.pages_data) % 10 == 0 and len(self.pages_data) > 0:
            print(f'[{elapsed}s/{self.max_run_seconds}s] [{len(self.pages_data)} terindeks] Merayapi: {url}')

          final_url, html = await self.fetch(session, url)
          if html:
            try:
              # --- [BARU] Isolasi error per-halaman ---
              # Kalau ada bug/kasus aneh di SATU halaman (HTML rusak, dst),
              # itu nggak lagi bisa membunuh seluruh worker kayak kejadian
              # kemarin - cuma halaman itu yang di-skip, worker tetap hidup.
              await self.process_page(final_url or url, html)
            except Exception as e:
              print(f'[PROCESS ERROR] Gagal proses {url}: {e}')
      finally:
        async with self.worker_lock:
          self.active_workers -= 1
        self.queue.task_done()

  async def run(self):
    for url in self.seed_urls:
      parsed = urlparse(url)
      self.visited_domains.add(parsed.netloc)
      await self.queue.put((0, self.clean_url_string(url)))

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
      tasks = [asyncio.create_task(self.worker(session)) for _ in range(self.concurrency)]
      await asyncio.gather(*tasks, return_exceptions=True)

  def calculate_pagerank(self):
    """Skor otoritas ABSOLUT (bukan PageRank iteratif ternormalisasi).

    PageRank klasik yang ternormalisasi (sum semua skor = 1) cuma valid kalau
    dihitung sekali atas SATU graph utuh. Crawler ini jalan per-run dan cuma
    melihat subgraph halaman BARU di run itu doang - kalau tetap dinormalisasi
    per-run, skor dari run yang berbeda jadi nggak bisa dibandingkan langsung
    (basis normalisasi N-nya beda tiap run), padahal semuanya numpuk di kolom
    yang sama di D1 dan di-ORDER BY bareng.

    Gantinya: skor absolut & stabil = tier_dasar(domain) + log(1 + inbound
    link yang KETAHUAN di run ini). log1p dipakai supaya 1 halaman dengan
    1000 inbound link nggak otomatis ngalahin yang lain 1000x lipat.
    """
    print(f'\n[PAGERANK] Menghitung skor otoritas untuk {len(self.pages_data)} halaman...')
    for node in self.pages_data.keys():
      self.inbound[node] = []

    for source, targets in self.graph.items():
      for target in targets:
        if target in self.inbound:
          self.inbound[target].append(source)

    seed_domains = {urlparse(seed).netloc for seed in self.seed_urls}

    for url, page in self.pages_data.items():
      domain = page['domain']
      in_degree = len(self.inbound.get(url, []))

      if self.is_authority_domain(domain):
        tier_base = 3.0
      elif domain in seed_domains:
        tier_base = 1.5
      else:
        tier_base = 0.1

      page['pagerank'] = tier_base + math.log1p(in_degree)


def get_already_visited_urls():
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[CRITICAL ERROR] Secrets Cloudflare D1 belum terpasang!')
    sys.exit(1)

  def clean_url_helper(url):
    parsed = urlparse(url)
    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    if len(clean) > len(f"{parsed.scheme}://{parsed.netloc}/") and clean.endswith('/'):
      clean = clean[:-1]
    return clean

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
              raw_url = row['url'] if isinstance(row, dict) and 'url' in row else row[0]
              visited.add(clean_url_helper(raw_url))

            print(f'[D1 SYNC] Terbaca: {len(visited)} URL')
            success = True

            if len(rows) < limit:
              print(f'[D1 SYNC DONE] Total {len(visited)} URL lama berhasil disinkronisasi!')
              return visited

            offset += limit
            break
      except Exception as e:
        print(f'[D1 SYNC WARNING] Error jaringan: {e}')

      time.sleep(3)

    if not success:
      print(f'\n[CRITICAL ERROR] Gagal sync D1 di offset {offset}!')
      sys.exit(1)

  return visited


async def push_to_cloudflare_d1_async(crawled_data, batch_size=50):
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[ERROR] Secrets Cloudflare D1 belum terpasang!')
    return

  url_d1_query = f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/query'
  headers = {'Authorization': f'Bearer {CF_API_TOKEN}', 'Content-Type': 'application/json'}

  print(f'\n[D1 PUSH] Memulai upload {len(crawled_data)} dokumen ke D1 (Group Batching)...')

  chunks = [crawled_data[i:i + batch_size] for i in range(0, len(crawled_data), batch_size)]
  success_count = 0

  connector = aiohttp.TCPConnector(limit=10)
  async with aiohttp.ClientSession(connector=connector) as session:
    for chunk_idx, chunk in enumerate(chunks):
      batch_payload = []
      for page in chunk:
        # --- [FIX BUG] ---
        # Versi sebelumnya CUMA insert ke `documents`, tabel `documents_fts`
        # (yang dipakai buat MATCH/pencarian) nggak pernah di-update lagi.
        # Efeknya: data masuk ke DB, tapi nggak akan pernah ketemu di hasil
        # pencarian. Sekarang 3 statement per halaman, sama kayak sebelumnya,
        # cuma pakai parameterized query (lebih aman dari SQL injection/typo
        # escaping dibanding string-interpolation manual).
        batch_payload.append({
            "sql": """
                INSERT INTO documents (url, domain, title, snippet, favicon, thumbnail, pagerank, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(url) DO UPDATE SET
                    title=excluded.title,
                    snippet=excluded.snippet,
                    favicon=excluded.favicon,
                    thumbnail=excluded.thumbnail,
                    pagerank=excluded.pagerank;
            """,
            "params": [
                page['url'], page['domain'], page['title'], page['snippet'],
                page['favicon'], page['thumbnail'], page['pagerank'],
            ],
        })
        batch_payload.append({
            "sql": "DELETE FROM documents_fts WHERE rowid = (SELECT id FROM documents WHERE url = ?);",
            "params": [page['url']],
        })
        batch_payload.append({
            "sql": "INSERT INTO documents_fts(rowid, title, snippet) SELECT id, ?, ? FROM documents WHERE url = ?;",
            "params": [page['title'], page['snippet'], page['url']],
        })

      try:
        async with session.post(url_d1_query, headers=headers, json={"batch": batch_payload}, timeout=30) as resp:
          if resp.status == 200:
            res_data = await resp.json()
            if res_data.get('success'):
              success_count += len(chunk)
              print(f'[D1 PUSH] Batch {chunk_idx + 1}/{len(chunks)} OK ({len(chunk)} item)')
            else:
              print(f'[D1 PUSH ERROR] Batch {chunk_idx + 1} gagal: {res_data}')
          else:
            text = await resp.text()
            if "exceeded D1's free tier" in text:
              print("\n[ALERT] Kuota write harian Cloudflare D1 habis! Upload dihentikan.")
              break
            print(f'[D1 PUSH ERROR] Status {resp.status}: {text[:150]}')
      except Exception as e:
        print(f'[D1 PUSH EXCEPTION] Error pada batch {chunk_idx + 1}: {e}')

  print(f'[D1 FINISH] Selesai! {success_count}/{len(crawled_data)} dokumen berhasil disimpan di D1.')


if __name__ == '__main__':
  initial_seeds = [
      'https://www.google.com',
      'https://www.google.co.id',
      'https://duckduckgo.com',
      'https://www.bing.com',
      'https://www.yahoo.com',
      'https://www.ecosia.org',
      'https://id.wikipedia.org',
      'https://en.wikipedia.org',
      'https://id.wikihow.com',
      'https://brainly.co.id',
      'https://www.kompas.com',
      'https://www.detik.com',
      'https://www.liputan6.com',
      'https://www.tribunnews.com',
      'https://www.cnnindonesia.com',
      'https://www.antaraNews.com',
      'https://www.tempo.co',
      'https://www.cnbcindonesia.com',
      'https://www.bbc.com',
      'https://www.theverge.com',
      'https://techcrunch.com',
      'https://www.wired.com',
      'https://github.com',
      'https://stackoverflow.com',
      'https://developer.mozilla.org',
      'https://www.w3schools.com',
      'https://dev.to',
      'https://medium.com',
      'https://news.ycombinator.com',
      'https://www.minecraft.net',
      'https://store.steampowered.com',
      'https://www.epicgames.com',
      'https://www.roblox.com',
      'https://m.mobilelegends.com',
      'https://ff.garena.com',
      'https://www.ign.com',
      'https://www.gamespot.com',
      'https://oneesports.gg/id',
      'https://www.hltv.org',
      'https://liquipedia.net',
      'https://www.codashop.com/id-id',
      'https://www.unipin.com',
      'https://www.itemku.com',
      'https://kiosgamer.co.id',
      'https://www.kaskus.co.id',
      'https://id.quora.com',
      'https://www.reddit.com',
      'https://stackexchange.com',
      'https://indonesia.go.id',
      'https://www.kemdikbud.go.id',
      'https://www.kominfo.go.id',
      'https://www.bps.go.id',
      'https://www.pajak.go.id',
      'https://www.ui.ac.id',
      'https://www.itb.ac.id',
      'https://www.ugm.ac.id',
      'https://www.ut.ac.id',
      'https://www.behance.net',
      'https://dribbble.com',
      'https://id.pinterest.com',
  ]

  existing_urls = get_already_visited_urls()

  crawler = ProfessionalSearchCrawler(
      seed_urls=initial_seeds,
      max_run_seconds=3600,
      concurrency=15,
  )

  crawler.visited_urls.update(existing_urls)

  print("\n[START CRAWLER] Memulai perayapan web (Maksimal 1 Jam)...")
  asyncio.run(crawler.run())

  crawler.calculate_pagerank()

  crawled_results = list(crawler.pages_data.values())
  if crawled_results:
    asyncio.run(push_to_cloudflare_d1_async(crawled_results))
  else:
    print("\n[INFO] Tidak ada halaman baru yang dicrawl. Skip upload.")
