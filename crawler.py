import asyncio
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import aiohttp
from bs4 import BeautifulSoup

# --- PENGATURAN CRAWLER ---
INDEX_FILE = 'data/index/search_index.json'
MAX_RUN_SECONDS = 3600  # Bot berjalan maksimal 1 jam (3600 detik)
CONCURRENCY = 15  # Jumlah koneksi simultan (worker)
REINDEX_AFTER_DAYS = 7  # Re-index halaman jika data lebih lama dari 7 hari


class ProductionCrawler:

  def __init__(
      self,
      seed_urls,
      index_file=INDEX_FILE,
      max_run_seconds=MAX_RUN_SECONDS,
      concurrency=CONCURRENCY,
  ):
    self.seed_urls = seed_urls
    self.index_file = index_file
    self.max_run_seconds = max_run_seconds
    self.concurrency = concurrency

    self.queue = asyncio.Queue()
    self.visited_urls = set()
    self.domain_robots = {}  # Cache aturan robots.txt per domain
    self.pages_data = {}  # URL -> Dict Data Index
    self.graph = {}
    self.inbound = {}

    self.start_time = time.time()

    self.headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
    }

  # --- LOGIKA INKREMENTAL (LOAD DATA LAMA) ---
  def load_existing_index(self):
    if os.path.exists(self.index_file):
      try:
        with open(self.index_file, 'r', encoding='utf-8') as f:
          data = json.load(f)
          now = time.time()
          reindex_threshold = REINDEX_AFTER_DAYS * 86400

          for item in data:
            url = item.get('url')
            last_updated = item.get('last_updated', 0)

            # Jika data masih segar (< REINDEX_AFTER_DAYS), tandai sudah dikunjungi agar tidak di-crawl ulang
            if now - last_updated < reindex_threshold:
              self.visited_urls.add(url)

            # Tetap simpan ke memori agar index lama tidak hilang
            self.pages_data[url] = item
        print(
            f'[LOAD] Berhasil memuat {len(self.pages_data)} data indeks lama.'
        )
      except Exception as e:
        print(f'[WARN] Gagal memuat indeks lama ({e}), membuat indeks baru.')

  # --- LOGIKA ROBOTS.TXT ---
  async def is_allowed_by_robots(self, session, url):
    parsed = urlparse(url)
    domain_base = f'{parsed.scheme}://{parsed.netloc}'

    if domain_base not in self.domain_robots:
      robots_url = f'{domain_base}/robots.txt'
      rfp = RobotFileParser()
      rfp.set_url(robots_url)

      try:
        async with session.get(
            robots_url, timeout=5, headers=self.headers
        ) as resp:
          if resp.status == 200:
            content = await resp.text()
            rfp.parse(content.splitlines())
          else:
            rfp.allow_all = True
      except Exception:
        rfp.allow_all = True

      self.domain_robots[domain_base] = rfp

    rfp = self.domain_robots[domain_base]
    return rfp.can_fetch(self.headers['User-Agent'], url)

  def is_valid_url(self, url):
    parsed = urlparse(url)
    invalid_exts = (
        '.png',
        '.jpg',
        '.jpeg',
        '.gif',
        '.pdf',
        '.zip',
        '.css',
        '.js',
        '.svg',
        '.mp4',
        '.mp3',
    )
    if any(parsed.path.lower().endswith(ext) for ext in invalid_exts):
      return False
    return bool(parsed.netloc) and parsed.scheme in ['http', 'https']

  def clean_text(self, text):
    return re.sub(r'\s+', ' ', text).strip()

  async def fetch(self, session, url):
    try:
      async with session.get(
          url, timeout=12, headers=self.headers, allow_redirects=True
      ) as response:
        content_type = response.headers.get('Content-Type', '').lower()
        if response.status == 200 and 'text/html' in content_type:
          return await response.text()
        return None
    except Exception:
      return None

  # --- PARSING KONTEN, FAVICON, & THUMBNAIL ---
  async def process_page(self, url, html):
    soup = BeautifulSoup(html, 'html.parser')
    parsed_url = urlparse(url)
    domain_base = f'{parsed_url.scheme}://{parsed_url.netloc}'

    # 1. Title
    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else parsed_url.netloc

    # 2. Description
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

    # 3. Favicon Discovery
    favicon = ''
    icon_tag = soup.find(
        'link',
        rel=lambda r: r
        and ('icon' in r.lower() or 'shortcut icon' in r.lower()),
    )
    if icon_tag and icon_tag.get('href'):
      favicon = urljoin(url, icon_tag['href'])
    else:
      # Fallback ke Google Favicon Service jika tidak terdeteksi
      favicon = (
          f'https://www.google.com/s2/favicons?domain={parsed_url.netloc}&sz=64'
      )

    # 4. Thumbnail (OG Image / Twitter Image)
    thumbnail = ''
    og_image = soup.find('meta', attrs={'property': 'og:image'}) or soup.find(
        'meta', attrs={'name': 'twitter:image'}
    )
    if og_image and og_image.get('content'):
      thumbnail = urljoin(url, og_image['content'])

    # 5. Snippet (Ekstraksi Konten Utama)
    for element in soup([
        'script',
        'style',
        'nav',
        'header',
        'footer',
        'noscript',
        'aside',
    ]):
      element.extract()

    main_content = soup.find('main') or soup.find('article') or soup.body
    if main_content:
      raw_text = main_content.get_text(separator=' ')
      cleaned = self.clean_text(raw_text)
      snippet = cleaned[:250] + '...' if len(cleaned) > 250 else cleaned
    else:
      snippet = description

    # 6. Ekstraksi Tautan Baru & Prioritas Domain Utama
    outgoing_links = set()
    for link in soup.find_all('a', href=True):
      abs_url = urljoin(url, link['href']).split('#')[0]

      if self.is_valid_url(abs_url):
        outgoing_links.add(abs_url)

        # PRINSIP PRIORITAS: Jika menemukan domain baru, dahulukan root domain-nya
        parsed_abs = urlparse(abs_url)
        root_domain_url = f'{parsed_abs.scheme}://{parsed_abs.netloc}/'

        # Antrekan Root Domain Terlebih Dahulu jika belum dikunjungi
        if root_domain_url not in self.visited_urls:
          await self.queue.put(root_domain_url)

        # Antrekan URL spesifiknya jika belum
        if abs_url not in self.visited_urls:
          await self.queue.put(abs_url)

    # Simpan/Update Hasil Indeks
    self.pages_data[url] = {
        'url': url,
        'domain': parsed_url.netloc,
        'title': title,
        'description': description,
        'snippet': snippet,
        'favicon': favicon,
        'thumbnail': thumbnail,
        'pagerank': 0.0,
        'last_updated': int(time.time()),
    }
    self.graph[url] = list(outgoing_links)

  # --- WORKER CRAWLER ---
  async def worker(self, session):
    while True:
      # Hentikan crawling jika waktu habis (misal > 1 jam)
      if time.time() - self.start_time > self.max_run_seconds:
        print('[TIME LIMIT] Batas waktu 1 jam tercapai, menghentikan worker...')
        break

      try:
        url = await asyncio.wait_for(self.queue.get(), timeout=3.0)
      except asyncio.TimeoutError:
        if time.time() - self.start_time > self.max_run_seconds or self.queue.empty():
          break
        continue

      if url in self.visited_urls:
        self.queue.task_done()
        continue

      self.visited_urls.add(url)

      # Cek robots.txt
      allowed = await self.is_allowed_by_robots(session, url)
      if not allowed:
        print(f'[ROBOTS BLOCKED] Disallow: {url}')
        self.queue.task_done()
        continue

      elapsed = int(time.time() - self.start_time)
      print(
          f'[{elapsed}s / {self.max_run_seconds}s] [{len(self.pages_data)}'
          f' indexed] Merayapi: {url}'
      )

      html = await self.fetch(session, url)
      if html:
        await self.process_page(url, html)

      self.queue.task_done()

  async def run(self):
    self.load_existing_index()

    # Masukkan Seed Awal
    for url in self.seed_urls:
      await self.queue.put(url)

    async with aiohttp.ClientSession() as session:
      tasks = [
          asyncio.create_task(self.worker(session))
          for _ in range(self.concurrency)
      ]
      await asyncio.gather(*tasks, return_exceptions=True)

  # --- PAGERANK ALGORITHM ---
  def calculate_pagerank(self, damping_factor=0.85, iterations=20):
    print('\nMenghitung ulang PageRank seluruh indeks...')
    for node in self.pages_data.keys():
      self.inbound[node] = []

    for source, targets in self.graph.items():
      for target in targets:
        if target in self.inbound:
          self.inbound[target].append(source)

    N = len(self.pages_data)
    if N == 0:
      return

    initial_pr = 1.0 / N
    for url in self.pages_data:
      self.pages_data[url]['pagerank'] = initial_pr

    for _ in range(iterations):
      new_pr = {}
      for url in self.pages_data:
        rank_sum = sum(
            (
                self.pages_data[in_node]['pagerank']
                / max(len(self.graph.get(in_node, [])), 1)
            )
            for in_node in self.inbound.get(url, [])
            if in_node in self.pages_data
        )
        new_pr[url] = ((1 - damping_factor) / N) + (damping_factor * rank_sum)

      for url in new_pr:
        self.pages_data[url]['pagerank'] = new_pr[url]

  def save_results(self):
    os.makedirs(os.path.dirname(self.index_file), exist_ok=True)
    sorted_results = sorted(
        self.pages_data.values(), key=lambda x: x['pagerank'], reverse=True
    )
    with open(self.index_file, 'w', encoding='utf-8') as f:
      json.dump(sorted_results, f, indent=4, ensure_ascii=False)
    print(
        f'\n[SELESAI] Indeks diperbarui! Total {len(sorted_results)} halaman'
        f' tersimpan di: {self.index_file}'
    )


if __name__ == '__main__':
  # Seed Awal Lintas Sektor (Tech, Search, Edu, Govt, Games) untuk Discovery Cepat
  initial_seeds = [
      'https://www.google.com',
      'https://www.bing.com',
      'https://duckduckgo.com',
      'https://search.brave.com',
      'https://www.microsoft.com',
      'https://www.kominfo.go.id',
      'https://id.wikipedia.org',
      'https://www.minecraft.net',
      'https://github.com',
      'https://stackoverflow.com',
  ]

  # Dijalankan dengan durasi maksimal 3600 detik (1 Jam)
  crawler = ProductionCrawler(
      seed_urls=initial_seeds,
      index_file='data/index/search_index.json',
      max_run_seconds=3600,
      concurrency=15,
  )

  asyncio.run(crawler.run())
  crawler.calculate_pagerank()
  crawler.save_results()
