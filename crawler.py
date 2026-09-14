import asyncio
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
from bs4 import BeautifulSoup
import requests

# --- CONFIGURATION ---
MAX_RUN_SECONDS = 3600  # Maksimal 1 Jam per run di GitHub Actions
CONCURRENCY = 15  # 15 Pekerja simultan

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

    self.queue = asyncio.Queue()
    self.visited_urls = set()
    self.visited_domains = set()  # Track domain untuk melebarkan pencarian
    self.domain_robots = {}
    self.pages_data = {}  # URL -> Dict Data
    self.graph = {}
    self.inbound = {}

    self.start_time = time.time()

    self.headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
            ' DeevvBot/1.0'
        ),
        'Accept-Language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
    }

  async def is_allowed_by_robots(self, session, url):
    parsed = urlparse(url)
    domain_base = f'{parsed.scheme}://{parsed.netloc}'

    if domain_base not in self.domain_robots:
      robots_url = f'{domain_base}/robots.txt'
      rfp = RobotFileParser()
      rfp.set_url(robots_url)

      try:
        async with session.get(
            robots_url, timeout=4, headers=self.headers
        ) as resp:
          if resp.status == 200:
            content = await resp.text()
            rfp.parse(content.splitlines())
          else:
            rfp.allow_all = True
      except Exception:
        rfp.allow_all = True

      self.domain_robots[domain_base] = rfp

    return self.domain_robots[domain_base].can_fetch(
        self.headers['User-Agent'], url
    )

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
        '.webp',
    )
    if any(parsed.path.lower().endswith(ext) for ext in invalid_exts):
      return False
    return bool(parsed.netloc) and parsed.scheme in ['http', 'https']

  def clean_text(self, text):
    return re.sub(r'\s+', ' ', text).strip()

  async def fetch(self, session, url):
    try:
      async with session.get(
          url, timeout=10, headers=self.headers, allow_redirects=True
      ) as response:
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

    # Meta Extract
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

    # Favicon
    icon_tag = soup.find(
        'link',
        rel=lambda r: r
        and ('icon' in r.lower() or 'shortcut icon' in r.lower()),
    )
    if icon_tag and icon_tag.get('href'):
      favicon = urljoin(url, icon_tag['href'])
    else:
      favicon = f'https://www.google.com/s2/favicons?domain={domain_name}&sz=64'

    # Thumbnail
    og_image = soup.find('meta', attrs={'property': 'og:image'}) or soup.find(
        'meta', attrs={'name': 'twitter:image'}
    )
    thumbnail = (
        urljoin(url, og_image['content'])
        if og_image and og_image.get('content')
        else ''
    )

    # Snippet Cleaning
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
      cleaned = self.clean_text(main_content.get_text(separator=' '))
      snippet = cleaned[:250] + '...' if len(cleaned) > 250 else cleaned
    else:
      snippet = description

    # Discovery URL & Spread to Broad Domains
    outgoing_links = set()
    for link in soup.find_all('a', href=True):
      abs_url = urljoin(url, link['href']).split('#')[0]

      if self.is_valid_url(abs_url):
        outgoing_links.add(abs_url)
        parsed_abs = urlparse(abs_url)
        target_domain = parsed_abs.netloc
        root_domain_url = f'{parsed_abs.scheme}://{target_domain}/'

        # STRATEGI MENYEBAR KELUAR: Utamakan root domain baru yang belum dikunjungi
        if (
            target_domain not in self.visited_domains
            and root_domain_url not in self.visited_urls
        ):
          self.visited_domains.add(target_domain)
          await self.queue.put(root_domain_url)

        if abs_url not in self.visited_urls:
          await self.queue.put(abs_url)

    # Temporary Store
    self.pages_data[url] = {
        'url': url,
        'domain': domain_name,
        'title': title,
        'snippet': snippet if snippet else title,
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
        url = await asyncio.wait_for(self.queue.get(), timeout=3.0)
      except asyncio.TimeoutError:
        if (
            time.time() - self.start_time > self.max_run_seconds
            or self.queue.empty()
        ):
          break
        continue

      if url in self.visited_urls:
        self.queue.task_done()
        continue

      self.visited_urls.add(url)

      allowed = await self.is_allowed_by_robots(session, url)
      if not allowed:
        self.queue.task_done()
        continue

      elapsed = int(time.time() - self.start_time)
      print(
          f'[{elapsed}s/{self.max_run_seconds}s] [{len(self.pages_data)} indexed]'
          f' Merayapi: {url}'
      )

      html = await self.fetch(session, url)
      if html:
        await self.process_page(url, html)

      self.queue.task_done()

  async def run(self):
    for url in self.seed_urls:
      parsed = urlparse(url)
      self.visited_domains.add(parsed.netloc)
      await self.queue.put(url)

    async with aiohttp.ClientSession() as session:
      tasks = [
          asyncio.create_task(self.worker(session))
          for _ in range(self.concurrency)
      ]
      await asyncio.gather(*tasks, return_exceptions=True)

  def calculate_pagerank(self, damping_factor=0.85, iterations=15):
    print(
        f'\n[PAGERANK] Menghitung PageRank untuk {len(self.pages_data)} halaman...'
    )
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


# --- CLOUDFLARE D1 INTEGRATION ---
def push_to_cloudflare_d1(crawled_data):
  if not all([CF_ACCOUNT_ID, CF_DATABASE_ID, CF_API_TOKEN]):
    print('[ERROR] Secrets Cloudflare D1 belum terpasang di GitHub!')
    return

  url = f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DATABASE_ID}/raw'
  headers = {
      'Authorization': f'Bearer {CF_API_TOKEN}',
      'Content-Type': 'application/json',
  }

  print(
      f'\n[D1 PUSH] Menyetor {len(crawled_data)} dokumen ke Cloudflare D1...'
  )

  success_count = 0
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

    if (
        res_doc.status_code == 200
        and res_doc.json().get('success')
        and res_fts.status_code == 200
        and res_fts.json().get('success')
    ):
      success_count += 1
    else:
      print(f'[ERROR] Gagal upload {page_url}')

  print(
      f'[D1 FINISH] Selesai! {success_count}/{len(crawled_data)} dokumen'
      ' berhasil tersimpan di Cloudflare D1.'
  )


if __name__ == '__main__':
  # Seed Awal Lintas Sektor Global & Indonesia (Gaming, News, Wiki, Tech, Edu, Govt)
  initial_seeds = [
      'https://id.wikipedia.org',
      'https://www.kompas.com',
      'https://www.detik.com',
      'https://www.ign.com',
      'https://www.minecraft.net',
      'https://github.com',
      'https://stackoverflow.com',
      'https://www.theverge.com',
      'https://www.kemdikbud.go.id',
      'https://store.steampowered.com',
      'https://www.reddit.com',
  ]

  crawler = ProductionD1Crawler(
      seed_urls=initial_seeds,
      max_run_seconds=3600,  # Berjalan 1 jam per jadwal run
      concurrency=15,
  )

  # Run Crawler
  asyncio.run(crawler.run())

  # Calculate PageRank
  crawler.calculate_pagerank()

  # Push to Cloudflare D1
  crawled_results = list(crawler.pages_data.values())
  push_to_cloudflare_d1(crawled_results)
