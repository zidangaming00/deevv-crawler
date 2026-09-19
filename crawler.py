import asyncio
import os
import re
import sys
import time
import hashlib
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import aiohttp
import requests
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
MAX_RUN_SECONDS = 1200  
CONCURRENCY = 15        
MAX_URL_LENGTH = 200    
MAX_PATH_DEPTH = 6      
MAX_PAGES_PER_DOMAIN = 40  

# Secrets dari Cloudflare via Environment Variables GitHub
CF_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID')
CF_D1_DATABASE_ID = os.getenv('CF_D1_DATABASE_ID')
CF_API_TOKEN = os.getenv('CF_API_TOKEN')

def execute_d1_queries(queries):
    """Fungsi helper untuk mengeksekusi query ke Cloudflare D1 via REST API"""
    if not CF_ACCOUNT_ID or not CF_D1_DATABASE_ID or not CF_API_TOKEN:
        print('[CRITICAL ERROR] Kredensial Cloudflare D1 tidak lengkap di GitHub Secrets!')
        sys.exit(1)

    url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json=queries)
    if response.status_code != 200:
        print(f"[ERROR D1 API] {response.text}")
    return response.json()

class ProfessionalSearchCrawler:
    def __init__(self, seed_urls, max_run_seconds=MAX_RUN_SECONDS, concurrency=CONCURRENCY):
        self.seed_urls = seed_urls
        self.max_run_seconds = max_run_seconds
        self.concurrency = concurrency

        self.queue = asyncio.PriorityQueue()
        self.visited_urls = set()
        self.visited_domains = set()
        self.domain_robots = {}
        self.pages_data = {}
        self.graph = {}
        self.domain_counts = {}
        
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
        spam_tlds = ('.cn', '.xyz', '.top', '.pw', '.tk', '.ml', '.ga', '.cf', '.gq', '.wang', '.icu', '.best', '.monster', '.work', '.click', '.loan')
        return any(domain.endswith(tld) for tld in spam_tlds)

    def is_spider_trap(self, url):
        parsed = urlparse(url)
        path = parsed.path.lower()
        if len(url) > MAX_URL_LENGTH: return True
        path_segments = [p for p in path.split('/') if p]
        if len(path_segments) > MAX_PATH_DEPTH: return True
        if re.search(r'/(.+?)/\1/', path): return True

        trap_keywords = ('login', 'register', 'signup', 'signin', 'logout', 'cart', 'checkout', 'add-to-cart', 'replytocom', 'wp-json', 'xmlrpc.php', 'calendar', 'event', 'archive', 'share.php', 'print', 'action=', 'do=', 'redirect=', 'goto=', 'feed/', 'rss/', 'trackback/')
        return any(keyword in url.lower() for keyword in trap_keywords)

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
        invalid_exts = ('.png', '.jpg', '.jpeg', '.gif', '.pdf', '.zip', '.rar', '.7z', '.css', '.js', '.svg', '.mp4', '.mp3', '.webp', '.xml', '.json', '.ico', '.exe', '.dmg', '.iso', '.csv', '.xlsx', '.doc', '.docx')
        if any(parsed.path.lower().endswith(ext) for ext in invalid_exts): return False
        return bool(parsed.netloc) and parsed.scheme in ['http', 'https']

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text).strip()

    async def fetch(self, session, url):
        try:
            async with session.get(url, timeout=8, headers=self.headers, allow_redirects=True) as response:
                content_type = response.headers.get('Content-Type', '').lower()
                if response.status == 200 and 'text/html' in content_type:
                    html = await response.text()
                    final_url = self.clean_url_string(str(response.url))
                    last_mod = response.headers.get('Last-Modified') or response.headers.get('Date')
                    return final_url, html, last_mod
                else:
                    return None, None, None
        except Exception:
            return None, None, None

    async def process_page(self, url, html, last_mod_header):
        soup = BeautifulSoup(html, 'html.parser')
        domain_name = urlparse(url).netloc

        robots_meta = soup.find('meta', attrs={'name': re.compile(r'^robots$', re.I)})
        if robots_meta and robots_meta.get('content') and 'noindex' in robots_meta['content'].lower():
            return

        # --- Deteksi Bahasa ---
        html_tag = soup.find('html')
        language = html_tag.get('lang') if html_tag and html_tag.get('lang') else 'id'
        language = language.split('-')[0].lower()[:5]

        title_tag = soup.find('title')
        title = self.clean_text(title_tag.get_text()) if title_tag else domain_name

        snippet = ""
        meta_desc = soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)}) or soup.find('meta', attrs={'property': re.compile(r'^og:description$', re.I)})
        if meta_desc and meta_desc.get('content'):
            cand = self.clean_text(meta_desc['content'])
            if len(cand) > 30: snippet = cand

        if not snippet:
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'aside', 'form', 'button', 'svg']):
                element.extract()
            main_content = soup.find('main') or soup.find('article') or soup.find(id=re.compile(r'content|main', re.I)) or soup.body
            if main_content:
                paragraphs = main_content.find_all('p')
                valid_paragraphs = [self.clean_text(p.get_text()) for p in paragraphs if len(self.clean_text(p.get_text())) > 35]
                if valid_paragraphs:
                    combined_text = " ... ".join(valid_paragraphs)
                    snippet = combined_text[:160] + '...' if len(combined_text) > 160 else combined_text
                else:
                    raw_text = self.clean_text(main_content.get_text(separator=' '))
                    snippet = raw_text[:160] + '...' if len(raw_text) > 160 else raw_text

        if not snippet: snippet = title

        # --- Generate MD5 Content Hash ---
        content_hash = hashlib.md5((title + snippet).encode('utf-8')).hexdigest()

        icon_tag = soup.find('link', rel=lambda r: r and ('icon' in r.lower()))
        favicon = urljoin(url, icon_tag['href']) if (icon_tag and icon_tag.get('href')) else f'https://www.google.com/s2/favicons?domain={domain_name}&sz=64'

        og_image = soup.find('meta', attrs={'property': lambda x: x and x.lower() == 'og:image'})
        thumbnail = urljoin(url, og_image['content']) if (og_image and og_image.get('content')) else ''

        last_modified = last_mod_header
        if not last_modified:
            meta_mod = soup.find('meta', attrs={'property': lambda x: x and 'modified_time' in x.lower()}) or soup.find('meta', attrs={'property': lambda x: x and 'published_time' in x.lower()})
            if meta_mod and meta_mod.get('content'):
                last_modified = meta_mod['content']

        outgoing_links = set()
        for link in soup.find_all('a', href=True):
            rel_attr = link.get('rel')
            if rel_attr and 'nofollow' in [r.lower() for r in rel_attr]: continue

            raw_url = urljoin(url, link['href'])
            parsed_raw = urlparse(raw_url)
            if parsed_raw.query: continue
            clean_url = self.clean_url_string(raw_url)

            if self.is_valid_url(clean_url):
                target_domain = urlparse(clean_url).netloc
                if self.is_spam_domain(target_domain) or self.is_spider_trap(clean_url): continue

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
                        if target_domain.count('.') > 1 and "www" not in target_domain: priority_score -= 5
                        await self.queue.put((priority_score, clean_url))

        self.pages_data[url] = {
            'url': url,
            'domain': domain_name,
            'title': title,
            'snippet': snippet,
            'favicon': favicon,
            'thumbnail': thumbnail,
            'last_modified': last_modified,
            'content_hash': content_hash,
            'language': language
        }
        self.graph[url] = list(outgoing_links)

    async def worker(self, session):
        while True:
            if time.time() - self.start_time > self.max_run_seconds: break
            try:
                priority, url = await asyncio.wait_for(self.queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                if time.time() - self.start_time > self.max_run_seconds: break
                async with self.worker_lock:
                    if self.queue.empty() and self.active_workers == 0: break
                continue

            async with self.worker_lock:
                self.active_workers += 1

            try:
                robots_rules = await self.get_robots_rules(session, url)
                if robots_rules.can_fetch(self.headers['User-Agent'], url):
                    crawl_delay = robots_rules.crawl_delay(self.headers['User-Agent'])
                    if crawl_delay: await asyncio.sleep(crawl_delay)

                    elapsed = int(time.time() - self.start_time)
                    if len(self.pages_data) % 10 == 0 and len(self.pages_data) > 0:
                        print(f'[{elapsed}s/{self.max_run_seconds}s] [{len(self.pages_data)} terindeks] Merayapi: {url}')

                    final_url, html, last_mod = await self.fetch(session, url)
                    if html:
                        try:
                            await self.process_page(final_url or url, html, last_mod)
                        except Exception:
                            pass
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

def get_already_visited_urls_d1():
    """Mengambil daftar URL yang sudah pernah dicrawl dari D1 agar tidak merayapi dari nol."""
    visited = set()
    print('\n[CLOUDFLARE SYNC] Mengambil daftar URL lama dari D1...')
    
    try:
        queries = [{"sql": "SELECT url FROM documents"}]
        data = execute_d1_queries(queries)
        
        if data.get('success'):
            results = data['result'][0]['results']
            for row in results:
                url_val = row['url']
                if url_val:
                    parsed = urlparse(url_val)
                    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    if len(clean) > len(f"{parsed.scheme}://{parsed.netloc}/") and clean.endswith('/'):
                        clean = clean[:-1]
                    visited.add(clean)
            print(f'[CLOUDFLARE SYNC DONE] Terbaca: {len(visited)} URL lama berhasil disinkronisasi!')
        else:
            print(f"[CLOUDFLARE SYNC ERROR] Gagal sync URL: {data}")
    except Exception as e:
        print(f'[CLOUDFLARE SYNC WARNING] Exception: {e}')
        
    return visited

def push_to_d1(crawled_data, graph_data, batch_size=50):
    print(f'\n[CLOUDFLARE PUSH] Memulai upload {len(crawled_data)} dokumen ke D1...')

    chunks = [crawled_data[i:i + batch_size] for i in range(0, len(crawled_data), batch_size)]
    success_docs = 0
    
    for chunk_idx, chunk in enumerate(chunks):
        queries = []
        for page in chunk:
            queries.append({
                "sql": """
                    INSERT INTO documents (url, domain, title, snippet, favicon, thumbnail, created_at, last_modified, content_hash, language)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title,
                        snippet=excluded.snippet,
                        favicon=excluded.favicon,
                        thumbnail=excluded.thumbnail,
                        language=excluded.language,
                        last_modified=CASE 
                            WHEN excluded.content_hash != documents.content_hash THEN CURRENT_TIMESTAMP
                            ELSE COALESCE(excluded.last_modified, documents.last_modified)
                        END,
                        content_hash=excluded.content_hash;
                """,
                "params": [page['url'], page['domain'], page['title'], page['snippet'], page['favicon'], 
                           page['thumbnail'], page.get('last_modified'), page['content_hash'], page['language']]
            })
        
        try:
            res = execute_d1_queries(queries)
            if res.get('success'):
                success_docs += len(chunk)
                print(f'[CLOUDFLARE PUSH] Batch Dokumen {chunk_idx + 1}/{len(chunks)} OK')
            else:
                print(f'[CLOUDFLARE PUSH ERROR] Batch {chunk_idx + 1} Error API: {res}')
        except Exception as e:
            print(f'[CLOUDFLARE PUSH ERROR] Batch Dokumen {chunk_idx + 1} exception: {e}')

    print(f'\n[CLOUDFLARE PUSH] Menyimpan struktur graph link...')
    graph_queries = []
    for source, targets in graph_data.items():
        for target in targets:
            if source != target:
                graph_queries.append({
                    "sql": """
                        INSERT INTO page_graph (source_url, target_url)
                        VALUES (?, ?)
                        ON CONFLICT(source_url, target_url) DO NOTHING;
                    """,
                    "params": [source, target]
                })
                
    graph_chunks = [graph_queries[i:i + 100] for i in range(0, len(graph_queries), 100)]
    for i, g_chunk in enumerate(graph_chunks):
        try:
            execute_d1_queries(g_chunk)
        except Exception as e:
            print(f'[CLOUDFLARE PUSH ERROR] Batch Graph {i + 1} gagal: {e}')

    print(f'[CLOUDFLARE FINISH] Selesai! {success_docs} dokumen dan relasinya berhasil disimpan di D1.')

if __name__ == '__main__':
    # Full Seed URLs dikembalikan
    initial_seeds = [
        'https://www.google.com', 'https://www.google.co.id', 'https://duckduckgo.com', 'https://www.bing.com', 
        'https://id.wikipedia.org', 'https://en.wikipedia.org', 'https://www.kompas.com', 'https://www.detik.com',
        'https://www.liputan6.com', 'https://www.tribunnews.com', 'https://www.cnnindonesia.com', 'https://www.tempo.co',
        'https://www.cnbcindonesia.com', 'https://www.bbc.com', 'https://www.theverge.com', 'https://techcrunch.com',
        'https://github.com', 'https://stackoverflow.com', 'https://developer.mozilla.org', 'https://dev.to',
        'https://news.ycombinator.com', 'https://www.minecraft.net', 'https://store.steampowered.com',
        'https://m.mobilelegends.com', 'https://ff.garena.com', 'https://www.hltv.org', 'https://liquipedia.net',
        'https://id.quora.com', 'https://www.reddit.com', 'https://indonesia.go.id', 'https://www.kemdikbud.go.id',
        'https://www.kominfo.go.id', 'https://www.bps.go.id', 'https://www.ui.ac.id', 'https://www.itb.ac.id',
        'https://www.behance.net', 'https://dribbble.com', 'https://id.pinterest.com',
    ]

    existing_urls = get_already_visited_urls_d1()

    crawler = ProfessionalSearchCrawler(
        seed_urls=initial_seeds,
        max_run_seconds=3600,
        concurrency=15,
    )

    crawler.visited_urls.update(existing_urls)

    print("\n[START CRAWLER] Memulai perayapan web D1 (Maksimal 1 Jam)...")
    asyncio.run(crawler.run())

    crawled_results = list(crawler.pages_data.values())
    graph_results = crawler.graph

    if crawled_results:
        push_to_d1(crawled_results, graph_results)
    else:
        print("\n[INFO] Tidak ada halaman baru yang dicrawl. Skip upload.")
