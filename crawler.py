import asyncio
import json
import os
import re
from urllib.parse import urljoin, urlparse
import aiohttp
from bs4 import BeautifulSoup


class MassiveWebCrawler:

  def __init__(self, start_urls, max_pages=100, concurrency=10):
    self.start_urls = start_urls
    self.max_pages = max_pages
    self.concurrency = concurrency

    self.visited = set()
    self.queue = asyncio.Queue()

    self.pages_data = {}
    self.graph = {}
    self.inbound = {}

    # Header User-Agent agar tidak gampang diblokir oleh situs besar seperti Google/Microsoft
    self.headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            ' (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'en-US,en;q=0.9,id;q=0.8',
    }

  def is_valid_url(self, url):
    parsed = urlparse(url)
    # Filter file biner/media agar crawler fokus ke halaman HTML
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
        # Pastikan hanya mengambil dokumen HTML
        content_type = response.headers.get('Content-Type', '').lower()
        if response.status == 200 and 'text/html' in content_type:
          return await response.text()
        return None
    except Exception:
      return None

  async def process_page(self, url, html):
    soup = BeautifulSoup(html, 'html.parser')

    # 1. Ekstraksi Title
    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else 'Tanpa Judul'

    # 2. Ekstraksi Description Cerdas (Fallback: Standard -> Open Graph -> Twitter)
    description = ''
    meta_desc = (
        soup.find('meta', attrs={'name': 'description'})
        or soup.find('meta', attrs={'property': 'og:description'})
        or soup.find('meta', attrs={'name': 'twitter:description'})
    )
    if meta_desc and meta_desc.get('content'):
      description = self.clean_text(meta_desc['content'])

    # 3. Ekstraksi Snippet Cerdas (Buang tag pengganggu dulu)
    for element in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript']):
      element.extract()

    # Cari teks dari elemen utama atau body
    main_content = soup.find('main') or soup.find('article') or soup.body
    if main_content:
      raw_text = main_content.get_text(separator=' ')
      cleaned_text = self.clean_text(raw_text)
      snippet = (
          cleaned_text[:250] + '...'
          if len(cleaned_text) > 250
          else cleaned_text
      )
    else:
      snippet = description

    # 4. Ekstraksi Link (Internal & External)
    outgoing_links = set()
    for link in soup.find_all('a', href=True):
      absolute_url = urljoin(url, link['href']).split('#')[0]
      if self.is_valid_url(absolute_url):
        outgoing_links.add(absolute_url)
        if (
            absolute_url not in self.visited
            and absolute_url not in [item[0] for item in self.queue._queue]
        ):
          await self.queue.put(absolute_url)

    # Simpan Data
    self.pages_data[url] = {
        'url': url,
        'title': title,
        'description': description,
        'snippet': snippet,
        'pagerank': 0.0,
    }
    self.graph[url] = list(outgoing_links)

  async def worker(self, session):
    while len(self.visited) < self.max_pages and not self.queue.empty():
      try:
        url = await asyncio.wait_for(self.queue.get(), timeout=2.0)
      except asyncio.TimeoutError:
        break

      if url in self.visited:
        self.queue.task_done()
        continue

      self.visited.add(url)
      print(f'[{len(self.visited)}/{self.max_pages}] Merayapi: {url}')

      html = await self.fetch(session, url)
      if html:
        await self.process_page(url, html)

      self.queue.task_done()

  async def run_crawler(self):
    for url in self.start_urls:
      await self.queue.put(url)

    async with aiohttp.ClientSession() as session:
      tasks = [
          asyncio.create_task(self.worker(session))
          for _ in range(self.concurrency)
      ]
      await asyncio.gather(*tasks, return_exceptions=True)

  def calculate_pagerank(self, damping_factor=0.85, iterations=20):
    print('\nMenghitung PageRank...')
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
                / len(self.graph.get(in_node, []))
            )
            for in_node in self.inbound[url]
            if len(self.graph.get(in_node, [])) > 0
        )
        new_pr[url] = ((1 - damping_factor) / N) + (damping_factor * rank_sum)

      for url in new_pr:
        self.pages_data[url]['pagerank'] = new_pr[url]

  def save_results(self, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    sorted_results = sorted(
        self.pages_data.values(), key=lambda x: x['pagerank'], reverse=True
    )
    with open(filepath, 'w', encoding='utf-8') as f:
      json.dump(sorted_results, f, indent=4, ensure_ascii=False)
    print(f'\nData berhasil disimpan di: {filepath}')


if __name__ == '__main__':
  # Seed URLs yang mencakup situs-situs besar dan beragam
  seed_urls = [
      'https://www.google.com',
      'https://www.microsoft.com',
      'https://www.minecraft.net',
      'https://id.wikipedia.org',
      'https://github.com',
      'https://stackoverflow.com',
      'https://www.python.org',
      'https://quotes.toscrape.com/',
  ]

  # Kamu bisa naikkan max_pages misalnya ke 100 atau 150 kalau mau data indeks lebih banyak
  crawler = MassiveWebCrawler(seed_urls, max_pages=100, concurrency=10)
  asyncio.run(crawler.run_crawler())
  crawler.calculate_pagerank()
  crawler.save_results('data/index/search_index.json')
