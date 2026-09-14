import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import re
import os

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

    def is_valid_url(self, url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and parsed.scheme in ['http', 'https']

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text).strip()

    async def fetch(self, session, url):
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                return None
        except Exception:
            return None

    async def process_page(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else "Tanpa Judul"
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        description = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
        
        paragraphs = soup.find_all('p')
        text_content = " ".join([p.get_text() for p in paragraphs])
        snippet = self.clean_text(text_content)[:200] + "..." if text_content else ""

        outgoing_links = set()
        for link in soup.find_all('a', href=True):
            absolute_url = urljoin(url, link['href']).split('#')[0]
            if self.is_valid_url(absolute_url):
                outgoing_links.add(absolute_url)
                if absolute_url not in self.visited and absolute_url not in [item[0] for item in self.queue._queue]:
                    await self.queue.put(absolute_url)

        self.pages_data[url] = {
            "url": url,
            "title": title,
            "description": description,
            "snippet": snippet,
            "pagerank": 0.0
        }
        self.graph[url] = list(outgoing_links)

    async def worker(self, session):
        while len(self.visited) < self.max_pages and not self.queue.empty():
            url = await self.queue.get()
            if url in self.visited:
                self.queue.task_done()
                continue
                
            self.visited.add(url)
            print(f"[{len(self.visited)}/{self.max_pages}] Merayapi: {url}")
            
            html = await self.fetch(session, url)
            if html:
                await self.process_page(url, html)
                
            self.queue.task_done()

    async def run_crawler(self):
        for url in self.start_urls:
            await self.queue.put(url)
            
        async with aiohttp.ClientSession(headers={'User-Agent': 'MassiveSpiderBot/1.0'}) as session:
            tasks = [asyncio.create_task(self.worker(session)) for _ in range(self.concurrency)]
            await self.queue.join()
            for task in tasks:
                task.cancel()

    def calculate_pagerank(self, damping_factor=0.85, iterations=20):
        print("\nMenghitung PageRank...")
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
            self.pages_data[url]["pagerank"] = initial_pr

        for _ in range(iterations):
            new_pr = {}
            for url in self.pages_data:
                rank_sum = sum((self.pages_data[in_node]["pagerank"] / len(self.graph.get(in_node, []))) 
                               for in_node in self.inbound[url] if len(self.graph.get(in_node, [])) > 0)
                new_pr[url] = ((1 - damping_factor) / N) + (damping_factor * rank_sum)
                
            for url in new_pr:
                self.pages_data[url]["pagerank"] = new_pr[url]

    def save_results(self, filepath):
        # Membuat folder target jika belum ada
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        sorted_results = sorted(self.pages_data.values(), key=lambda x: x['pagerank'], reverse=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(sorted_results, f, indent=4, ensure_ascii=False)
        print(f"\nData disimpan di: {filepath}")

if __name__ == "__main__":
    seed_urls = [
        "https://quotes.toscrape.com/",
        # Masukkan URL targetmu di sini
    ]
    
    crawler = MassiveWebCrawler(seed_urls, max_pages=50, concurrency=10)
    asyncio.run(crawler.run_crawler())
    crawler.calculate_pagerank()
    
    # Path disesuaikan dengan permintaanmu
    crawler.save_results("data/index/search_index.json")
