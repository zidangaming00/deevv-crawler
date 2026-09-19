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


# ============================================================
# CONFIGURATION
# ============================================================

MAX_RUN_SECONDS = 3600

CONCURRENCY = 15

MAX_URL_LENGTH = 200
MAX_PATH_DEPTH = 6
MAX_PAGES_PER_DOMAIN = 40

# Jumlah SQL statement dalam SATU request D1.
# Ini bukan batas jumlah data yang bisa disimpan.
D1_BATCH_SIZE = 50

D1_REQUEST_TIMEOUT = 30
D1_RETRY_COUNT = 3

HTTP_TIMEOUT = 8
ROBOTS_TIMEOUT = 4

USER_AGENT = (
    "Mozilla/5.0 "
    "(compatible; DeevvBot/1.0; "
    "+https://deevv.pages.dev)"
)


# ============================================================
# CLOUDFLARE ENV
# ============================================================

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_D1_DATABASE_ID = os.getenv("CF_D1_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")


# ============================================================
# GLOBAL STATE
# ============================================================

START_TIME = time.monotonic()

visited_urls = set()
queued_urls = set()

domain_page_count = {}

documents = []
graph_edges = []

robots_cache = {}

stats = {
    "crawled": 0,
    "saved": 0,
    "skipped": 0,
    "errors": 0,
    "graph": 0,
}


# ============================================================
# TIME CONTROL
# ============================================================

def time_exceeded():
    return (
        time.monotonic() - START_TIME
    ) >= MAX_RUN_SECONDS


def elapsed_seconds():
    return int(time.monotonic() - START_TIME)


# ============================================================
# D1 API
# ============================================================

def get_d1_api_url():
    return (
        "https://api.cloudflare.com/client/v4/"
        f"accounts/{CF_ACCOUNT_ID}/"
        f"d1/database/{CF_D1_DATABASE_ID}/query"
    )


def d1_request(batch):
    """
    Kirim batch SQL ke Cloudflare D1.

    Format API yang benar:

    {
        "batch": [
            {
                "sql": "...",
                "params": [...]
            }
        ]
    }

    Tidak ada request test tambahan.
    """

    if not CF_ACCOUNT_ID:
        raise RuntimeError("CF_ACCOUNT_ID tidak tersedia")

    if not CF_D1_DATABASE_ID:
        raise RuntimeError("CF_D1_DATABASE_ID tidak tersedia")

    if not CF_API_TOKEN:
        raise RuntimeError("CF_API_TOKEN tidak tersedia")

    url = get_d1_api_url()

    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "batch": batch
    }

    last_error = None

    for attempt in range(1, D1_RETRY_COUNT + 1):

        try:

            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=D1_REQUEST_TIMEOUT,
            )

            try:
                data = response.json()
            except Exception:
                data = {
                    "success": False,
                    "errors": [
                        {
                            "message": response.text[:1000]
                        }
                    ],
                }

            if response.status_code != 200:

                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{data}"
                )

                print(
                    f"[D1 ERROR] Attempt "
                    f"{attempt}/{D1_RETRY_COUNT}: "
                    f"{last_error}"
                )

                if attempt < D1_RETRY_COUNT:
                    time.sleep(min(2 * attempt, 5))

                continue

            if not data.get("success", False):

                last_error = str(data.get("errors"))

                print(
                    f"[D1 ERROR] Attempt "
                    f"{attempt}/{D1_RETRY_COUNT}: "
                    f"{last_error}"
                )

                # Error SQL/schema tidak perlu diulang berkali-kali.
                if attempt >= D1_RETRY_COUNT:
                    break

                time.sleep(min(2 * attempt, 5))
                continue

            return data

        except requests.RequestException as exc:

            last_error = str(exc)

            print(
                f"[D1 NETWORK ERROR] Attempt "
                f"{attempt}/{D1_RETRY_COUNT}: "
                f"{exc}"
            )

            if attempt < D1_RETRY_COUNT:
                time.sleep(min(2 * attempt, 5))

    raise RuntimeError(
        f"D1 request gagal setelah "
        f"{D1_RETRY_COUNT} percobaan: "
        f"{last_error}"
    )


# ============================================================
# GET EXISTING URLS
# ============================================================

def get_already_visited_urls_d1():
    """
    Hanya SATU query read untuk mengambil URL yang sudah ada.

    Ini sengaja dilakukan sekali di awal supaya crawler tidak
    melakukan SELECT ke D1 untuk setiap URL.
    """

    print("[D1] Mengambil daftar URL yang sudah tersimpan...")

    batch = [
        {
            "sql": """
                SELECT url
                FROM documents
            """,
            "params": [],
        }
    ]

    try:

        data = d1_request(batch)

        urls = set()

        results = data.get("result", [])

        if not results:
            return urls

        for result in results:

            rows = result.get("results", [])

            for row in rows:

                url = row.get("url")

                if url:
                    urls.add(url)

        print(
            f"[D1] {len(urls):,} URL sudah ada."
        )

        return urls

    except Exception as exc:

        print(
            f"[D1 READ ERROR] {exc}"
        )

        print(
            "[D1] Crawler dihentikan agar tidak "
            "mengulang crawl dan berpotensi menulis data duplikat."
        )

        raise


# ============================================================
# URL HELPERS
# ============================================================

def normalize_url(url):
    try:

        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            return None

        if not parsed.netloc:
            return None

        hostname = parsed.hostname

        if not hostname:
            return None

        hostname = hostname.lower()

        # Buang www hanya untuk normalisasi hostname.
        # URL tetap menggunakan hostname hasil normalisasi.
        if hostname.startswith("www."):
            hostname = hostname[4:]

        path = parsed.path or "/"

        # Hilangkan // berlebihan
        path = re.sub(r"/+", "/", path)

        # Jangan simpan fragment/query agar URL canonical lebih stabil.
        normalized = (
            f"{parsed.scheme.lower()}://"
            f"{hostname}"
            f"{path}"
        )

        if len(normalized) > MAX_URL_LENGTH:
            return None

        return normalized.rstrip("/") if path != "/" else normalized

    except Exception:
        return None


def get_domain(url):
    try:
        hostname = urlparse(url).hostname

        if not hostname:
            return ""

        hostname = hostname.lower()

        if hostname.startswith("www."):
            hostname = hostname[4:]

        return hostname

    except Exception:
        return ""


def get_path_depth(url):
    try:

        path = urlparse(url).path

        parts = [
            x for x in path.split("/")
            if x
        ]

        return len(parts)

    except Exception:
        return 999


def is_valid_url(url):
    if not url:
        return False

    if len(url) > MAX_URL_LENGTH:
        return False

    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        return False

    if not parsed.hostname:
        return False

    # Hindari URL dengan username/password.
    if parsed.username or parsed.password:
        return False

    if get_path_depth(url) > MAX_PATH_DEPTH:
        return False

    return True


# ============================================================
# SPAM / TRAP FILTER
# ============================================================

SPAM_TLDS = {
    ".zip",
    ".mov",
    ".click",
    ".xyz",
    ".top",
    ".gq",
    ".tk",
    ".ml",
    ".cf",
    ".ga",
}


TRAP_PATTERNS = [
    r"/calendar/",
    r"/tag/",
    r"/tags/",
    r"/page/\d+",
    r"/search",
    r"/login",
    r"/signin",
    r"/signup",
    r"/register",
    r"/cart",
    r"/checkout",
    r"/wp-admin",
    r"/feed",
    r"/comments/feed",
]


def is_spam_domain(url):

    hostname = get_domain(url)

    for tld in SPAM_TLDS:

        if hostname.endswith(tld):
            return True

    return False


def is_spider_trap(url):

    lowered = url.lower()

    for pattern in TRAP_PATTERNS:

        if re.search(pattern, lowered):
            return True

    return False


# ============================================================
# ROBOTS
# ============================================================

def can_fetch_robots(url):

    domain = get_domain(url)

    if not domain:
        return False

    if domain in robots_cache:
        return robots_cache[domain]

    parsed = urlparse(url)

    robots_url = (
        f"{parsed.scheme}://"
        f"{parsed.netloc}/robots.txt"
    )

    parser = RobotFileParser()

    parser.set_url(robots_url)

    try:

        parser.read()

        allowed = parser.can_fetch(
            USER_AGENT,
            url,
        )

    except Exception:

        # Jika robots tidak dapat dibaca,
        # jangan langsung memblokir seluruh domain.
        allowed = True

    robots_cache[domain] = allowed

    return allowed


# ============================================================
# HTML EXTRACTION
# ============================================================

def clean_text(text):

    if not text:
        return ""

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


def extract_title(soup):

    if soup.title:

        title = clean_text(
            soup.title.get_text(" ", strip=True)
        )

        if title:
            return title[:500]

    og_title = soup.find(
        "meta",
        property="og:title",
    )

    if og_title:

        value = og_title.get("content")

        if value:
            return clean_text(value)[:500]

    return ""


def extract_description(soup):

    meta = soup.find(
        "meta",
        attrs={
            "name": re.compile(
                r"^description$",
                re.I,
            )
        },
    )

    if meta:

        content = meta.get("content")

        if content:
            return clean_text(content)[:1000]

    og_description = soup.find(
        "meta",
        property="og:description",
    )

    if og_description:

        content = og_description.get("content")

        if content:
            return clean_text(content)[:1000]

    return ""


def extract_snippet(soup):

    description = extract_description(soup)

    if description:
        return description[:1000]

    for tag in soup(
        [
            "script",
            "style",
            "noscript",
            "svg",
            "nav",
            "footer",
            "header",
        ]
    ):
        tag.decompose()

    text = clean_text(
        soup.get_text(
            " ",
            strip=True,
        )
    )

    return text[:1000]


def extract_favicon(soup, page_url):

    icon = soup.find(
        "link",
        rel=lambda value: (
            value and
            any(
                "icon" in str(x).lower()
                for x in (
                    value
                    if isinstance(value, list)
                    else [value]
                )
            )
        ),
    )

    if icon:

        href = icon.get("href")

        if href:

            return urljoin(
                page_url,
                href,
            )[:1000]

    parsed = urlparse(page_url)

    return (
        f"{parsed.scheme}://"
        f"{parsed.netloc}/favicon.ico"
    )


def extract_thumbnail(soup, page_url):

    og_image = soup.find(
        "meta",
        property="og:image",
    )

    if og_image:

        content = og_image.get("content")

        if content:

            return urljoin(
                page_url,
                content,
            )[:2000]

    twitter_image = soup.find(
        "meta",
        attrs={
            "name": "twitter:image"
        },
    )

    if twitter_image:

        content = twitter_image.get("content")

        if content:

            return urljoin(
                page_url,
                content,
            )[:2000]

    return ""


def extract_language(soup):

    html = soup.find("html")

    if not html:
        return ""

    language = html.get("lang")

    if language:
        return clean_text(language)[:20]

    return ""


def extract_last_modified(soup, headers):

    header_value = headers.get(
        "Last-Modified"
    )

    if header_value:
        return header_value[:100]

    meta_names = [
        "article:modified_time",
        "last-modified",
        "dateModified",
    ]

    for name in meta_names:

        tag = soup.find(
            "meta",
            attrs={
                "property": name
            },
        )

        if not tag:

            tag = soup.find(
                "meta",
                attrs={
                    "name": name
                },
            )

        if tag:

            content = tag.get("content")

            if content:
                return content[:100]

    return ""


# ============================================================
# LINK EXTRACTION
# ============================================================

def extract_links(soup, base_url):

    links = set()

    base_domain = get_domain(base_url)

    for anchor in soup.find_all("a", href=True):

        href = anchor.get("href")

        if not href:
            continue

        href = href.strip()

        if href.startswith((
            "#",
            "javascript:",
            "mailto:",
            "tel:",
            "data:",
        )):
            continue

        absolute = urljoin(
            base_url,
            href,
        )

        normalized = normalize_url(
            absolute
        )

        if not normalized:
            continue

        if not is_valid_url(normalized):
            continue

        if is_spam_domain(normalized):
            continue

        if is_spider_trap(normalized):
            continue

        # Graph tetap boleh lintas domain.
        # Batas crawl domain diterapkan ketika URL benar-benar
        # dimasukkan ke queue.
        links.add(normalized)

    return links


# ============================================================
# CONTENT HASH
# ============================================================

def make_content_hash(
    title,
    snippet,
):

    raw = (
        f"{title}\n"
        f"{snippet}"
    )

    return hashlib.md5(
        raw.encode(
            "utf-8",
            errors="ignore",
        )
    ).hexdigest()


# ============================================================
# HTTP FETCH
# ============================================================

async def fetch_page(
    session,
    url,
):

    timeout = aiohttp.ClientTimeout(
        total=HTTP_TIMEOUT
    )

    try:

        async with session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "text/html,"
                    "application/xhtml+xml,"
                    "application/xml;q=0.9,"
                    "*/*;q=0.8"
                ),
            },
        ) as response:

            if response.status != 200:
                return None

            content_type = (
                response.headers.get(
                    "Content-Type",
                    "",
                ).lower()
            )

            if (
                "text/html" not in content_type
                and "application/xhtml+xml"
                not in content_type
            ):
                return None

            final_url = normalize_url(
                str(response.url)
            )

            if not final_url:
                return None

            body = await response.text(
                errors="ignore"
            )

            if not body:
                return None

            return {
                "url": final_url,
                "html": body,
                "headers": response.headers,
            }

    except Exception:
        return None


# ============================================================
# CRAWLER
# ============================================================

class ProfessionalSearchCrawler:

    def __init__(
        self,
        existing_urls,
    ):

        self.existing_urls = set(
            existing_urls
        )

        self.queue = asyncio.PriorityQueue()

        self.session = None

        self.counter = 0

        self.local_seen = set(
            existing_urls
        )

    async def add_url(
        self,
        url,
        priority=10,
    ):

        if time_exceeded():
            return

        normalized = normalize_url(url)

        if not normalized:
            return

        if normalized in self.local_seen:
            return

        if normalized in visited_urls:
            return

        if normalized in queued_urls:
            return

        if not is_valid_url(normalized):
            return

        if is_spam_domain(normalized):
            return

        if is_spider_trap(normalized):
            return

        domain = get_domain(normalized)

        count = domain_page_count.get(
            domain,
            0,
        )

        if count >= MAX_PAGES_PER_DOMAIN:
            return

        if not can_fetch_robots(normalized):
            return

        queued_urls.add(normalized)

        self.counter += 1

        await self.queue.put(
            (
                priority,
                self.counter,
                normalized,
            )
        )

    async def worker(self):

        while not time_exceeded():

            try:

                priority, _, url = (
                    await asyncio.wait_for(
                        self.queue.get(),
                        timeout=2,
                    )
                )

            except asyncio.TimeoutError:

                if self.queue.empty():
                    return

                continue

            try:

                queued_urls.discard(url)

                if url in visited_urls:
                    continue

                domain = get_domain(url)

                count = domain_page_count.get(
                    domain,
                    0,
                )

                if count >= MAX_PAGES_PER_DOMAIN:
                    continue

                visited_urls.add(url)

                page = await fetch_page(
                    self.session,
                    url,
                )

                if not page:
                    stats["errors"] += 1
                    continue

                final_url = page["url"]

                html = page["html"]

                headers = page["headers"]

                # Redirected URL may differ.
                visited_urls.add(
                    final_url
                )

                soup = BeautifulSoup(
                    html,
                    "html.parser",
                )

                title = extract_title(
                    soup
                )

                snippet = extract_snippet(
                    soup
                )

                if not title and not snippet:
                    continue

                favicon = extract_favicon(
                    soup,
                    final_url,
                )

                thumbnail = extract_thumbnail(
                    soup,
                    final_url,
                )

                language = extract_language(
                    soup
                )

                last_modified = (
                    extract_last_modified(
                        soup,
                        headers,
                    )
                )

                content_hash = (
                    make_content_hash(
                        title,
                        snippet,
                    )
                )

                final_domain = get_domain(
                    final_url
                )

                document = {
                    "url": final_url,
                    "domain": final_domain,
                    "title": title,
                    "snippet": snippet,
                    "favicon": favicon,
                    "thumbnail": thumbnail,
                    "last_modified": (
                        last_modified
                    ),
                    "content_hash": (
                        content_hash
                    ),
                    "language": language,
                }

                documents.append(
                    document
                )

                domain_page_count[
                    final_domain
                ] = (
                    domain_page_count.get(
                        final_domain,
                        0,
                    )
                    + 1
                )

                stats["crawled"] += 1

                # ==================================================
                # OUTGOING GRAPH
                # ==================================================

                links = extract_links(
                    soup,
                    final_url,
                )

                for target_url in links:

                    graph_edges.append(
                        {
                            "source_url": final_url,
                            "target_url": target_url,
                        }
                    )

                    stats["graph"] += 1

                # ==================================================
                # QUEUE NEW URLS
                # ==================================================

                for target_url in links:

                    if time_exceeded():
                        break

                    # URL dari domain yang sudah penuh
                    # tidak perlu dimasukkan.
                    target_domain = get_domain(
                        target_url
                    )

                    target_count = (
                        domain_page_count.get(
                            target_domain,
                            0,
                        )
                    )

                    if (
                        target_count
                        >= MAX_PAGES_PER_DOMAIN
                    ):
                        continue

                    await self.add_url(
                        target_url,
                        priority=20,
                    )

                stats["saved"] += 1

                if stats["crawled"] % 100 == 0:

                    print(
                        "[CRAWLER] "
                        f"crawled={stats['crawled']:,} "
                        f"queued={self.queue.qsize():,} "
                        f"graph={stats['graph']:,} "
                        f"time={elapsed_seconds()}s"
                    )

            except Exception as exc:

                stats["errors"] += 1

                print(
                    f"[CRAWLER ERROR] "
                    f"{url}: {exc}"
                )

            finally:

                self.queue.task_done()

    async def run(
        self,
        seeds,
    ):

        connector = aiohttp.TCPConnector(
            limit=CONCURRENCY,
            limit_per_host=3,
            ttl_dns_cache=300,
        )

        self.session = aiohttp.ClientSession(
            connector=connector
        )

        try:

            print(
                "[CRAWLER] Menambahkan seed..."
            )

            for seed in seeds:

                await self.add_url(
                    seed,
                    priority=0,
                )

            workers = [
                asyncio.create_task(
                    self.worker()
                )
                for _ in range(CONCURRENCY)
            ]

            await self.queue.join()

            for worker in workers:

                worker.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

        finally:

            await self.session.close()


# ============================================================
# D1 DOCUMENT SQL
# ============================================================

DOCUMENT_SQL = """
INSERT INTO documents (
    url,
    domain,
    title,
    snippet,
    favicon,
    thumbnail,
    last_modified,
    content_hash,
    language
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(url) DO UPDATE SET
    domain = excluded.domain,
    title = excluded.title,
    snippet = excluded.snippet,
    favicon = excluded.favicon,
    thumbnail = excluded.thumbnail,
    last_modified = excluded.last_modified,
    content_hash = excluded.content_hash,
    language = excluded.language
"""


GRAPH_SQL = """
INSERT INTO page_graph (
    source_url,
    target_url
)
VALUES (?, ?)
ON CONFLICT(source_url, target_url)
DO NOTHING
"""


# ============================================================
# D1 UPLOAD
# ============================================================

def upload_documents_to_d1():
    if not documents:
        print(
            "[D1] Tidak ada document baru."
        )
        return

    total = len(documents)

    print(
        f"[CLOUDFLARE PUSH] "
        f"Memulai upload {total:,} dokumen ke D1..."
    )

    success_count = 0

    for start in range(
        0,
        total,
        D1_BATCH_SIZE,
    ):

        if time_exceeded():

            print(
                "[D1] Batas waktu tercapai "
                "saat upload documents."
            )

            break

        chunk = documents[
            start:start + D1_BATCH_SIZE
        ]

        batch = []

        for doc in chunk:

            batch.append(
                {
                    "sql": DOCUMENT_SQL,
                    "params": [
                        doc["url"],
                        doc["domain"],
                        doc["title"],
                        doc["snippet"],
                        doc["favicon"],
                        doc["thumbnail"],
                        doc["last_modified"],
                        doc["content_hash"],
                        doc["language"],
                    ],
                }
            )

        try:

            d1_request(batch)

            success_count += len(chunk)

            print(
                "[D1 DOCUMENTS] "
                f"{success_count:,}/{total:,}"
            )

        except Exception as exc:

            print(
                f"[D1 DOCUMENT ERROR] "
                f"Batch "
                f"{start // D1_BATCH_SIZE + 1}"
                f"/"
                f"{(total + D1_BATCH_SIZE - 1) // D1_BATCH_SIZE}"
                f" gagal:"
            )

            print(exc)

            # Jangan terus menghabiskan quota
            # dengan retry batch yang sama berkali-kali.
            print(
                "[D1] Upload documents dihentikan "
                "setelah batch gagal."
            )

            return

    print(
        f"[CLOUDFLARE PUSH] "
        f"Documents berhasil diproses: "
        f"{success_count:,}/{total:,}"
    )


def upload_graph_to_d1():
    if not graph_edges:
        print(
            "[D1] Tidak ada graph edge baru."
        )
        return

    total = len(graph_edges)

    print(
        f"[CLOUDFLARE PUSH] "
        f"Memulai upload {total:,} graph edges..."
    )

    success_count = 0

    for start in range(
        0,
        total,
        D1_BATCH_SIZE,
    ):

        if time_exceeded():

            print(
                "[D1] Batas waktu tercapai "
                "saat upload graph."
            )

            break

        chunk = graph_edges[
            start:start + D1_BATCH_SIZE
        ]

        batch = []

        for edge in chunk:

            batch.append(
                {
                    "sql": GRAPH_SQL,
                    "params": [
                        edge["source_url"],
                        edge["target_url"],
                    ],
                }
            )

        try:

            d1_request(batch)

            success_count += len(chunk)

            print(
                "[D1 GRAPH] "
                f"{success_count:,}/{total:,}"
            )

        except Exception as exc:

            print(
                f"[D1 GRAPH ERROR] "
                f"Batch "
                f"{start // D1_BATCH_SIZE + 1}"
                f"/"
                f"{(total + D1_BATCH_SIZE - 1) // D1_BATCH_SIZE}"
                f" gagal:"
            )

            print(exc)

            print(
                "[D1] Upload graph dihentikan "
                "setelah batch gagal."
            )

            return

    print(
        f"[CLOUDFLARE PUSH] "
        f"Graph berhasil diproses: "
        f"{success_count:,}/{total:,}"
    )


# ============================================================
# SEEDS
# ============================================================

SEEDS = [
    "https://www.google.com",
    "https://www.google.co.id",
    "https://duckduckgo.com",
    "https://www.bing.com",

    "https://id.wikipedia.org",
    "https://en.wikipedia.org",

    "https://www.kompas.com",
    "https://www.detik.com",
    "https://www.liputan6.com",
    "https://www.tribunnews.com",
    "https://www.cnnindonesia.com",
    "https://www.tempo.co",
    "https://www.cnbcindonesia.com",
    "https://www.bbc.com",

    "https://www.theverge.com",
    "https://techcrunch.com",

    "https://github.com",
    "https://stackoverflow.com",
    "https://developer.mozilla.org",
    "https://dev.to",
    "https://news.ycombinator.com",

    "https://www.minecraft.net",
    "https://store.steampowered.com",
    "https://m.mobilelegends.com",
    "https://ff.garena.com",
    "https://www.hltv.org",
    "https://liquipedia.net",

    "https://id.quora.com",
    "https://www.reddit.com",

    "https://indonesia.go.id",
    "https://www.kemdikbud.go.id",
    "https://www.kominfo.go.id",
    "https://www.bps.go.id",

    "https://www.ui.ac.id",
    "https://www.itb.ac.id",

    "https://www.behance.net",
    "https://dribbble.com",
    "https://id.pinterest.com",
]


# ============================================================
# MAIN
# ============================================================

def main():

    global START_TIME

    START_TIME = time.monotonic()

    print("=" * 60)
    print("DEEVV SEARCH CRAWLER")
    print("=" * 60)

    print(
        f"MAX RUN      : {MAX_RUN_SECONDS}s"
    )

    print(
        f"CONCURRENCY  : {CONCURRENCY}"
    )

    print(
        f"D1 BATCH     : {D1_BATCH_SIZE}"
    )

    print("=" * 60)

    # --------------------------------------------------------
    # Validasi environment SAJA.
    # Tidak melakukan request ke Cloudflare.
    # --------------------------------------------------------

    required = [
        "CF_ACCOUNT_ID",
        "CF_D1_DATABASE_ID",
        "CF_API_TOKEN",
    ]

    missing = [
        key
        for key in required
        if not os.getenv(key)
    ]

    if missing:

        print(
            "[FATAL] Environment variable "
            "belum lengkap:"
        )

        for key in missing:
            print(f" - {key}")

        sys.exit(1)

    # --------------------------------------------------------
    # SATU-SATUNYA READ D1
    # --------------------------------------------------------

    existing_urls = (
        get_already_visited_urls_d1()
    )

    # --------------------------------------------------------
    # CRAWL
    # --------------------------------------------------------

    crawler = ProfessionalSearchCrawler(
        existing_urls
    )

    try:

        asyncio.run(
            crawler.run(SEEDS)
        )

    except KeyboardInterrupt:

        print(
            "[CRAWLER] Dihentikan."
        )

    except Exception as exc:

        print(
            f"[FATAL CRAWLER ERROR] {exc}"
        )

        return

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print()
    print("=" * 60)
    print("CRAWL SELESAI")
    print("=" * 60)

    print(
        f"Documents : {len(documents):,}"
    )

    print(
        f"Graph     : {len(graph_edges):,}"
    )

    print(
        f"Crawled   : {stats['crawled']:,}"
    )

    print(
        f"Errors    : {stats['errors']:,}"
    )

    print(
        f"Elapsed   : {elapsed_seconds()}s"
    )

    print("=" * 60)

    # --------------------------------------------------------
    # UPLOAD DOCUMENTS
    # --------------------------------------------------------

    if documents:

        upload_documents_to_d1()

    # --------------------------------------------------------
    # UPLOAD GRAPH
    # --------------------------------------------------------

    if graph_edges:

        upload_graph_to_d1()

    print()
    print(
        "[DONE] Semua proses selesai."
    )


if __name__ == "__main__":
    main()
