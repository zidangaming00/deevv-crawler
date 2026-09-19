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

# Maksimal waktu crawling: 1 JAM
MAX_RUN_SECONDS = 3600

# Jumlah worker crawler secara bersamaan
CONCURRENCY = 15

# Batas URL
MAX_URL_LENGTH = 200

# Maksimal kedalaman path URL
MAX_PATH_DEPTH = 6

# Maksimal halaman yang dicrawl dari satu domain
MAX_PAGES_PER_DOMAIN = 40

# Ukuran batch upload dokumen ke D1
D1_BATCH_SIZE = 50

# Ukuran batch graph ke D1
D1_GRAPH_BATCH_SIZE = 100

# Timeout HTTP
HTTP_TIMEOUT = 8

# Timeout robots.txt
ROBOTS_TIMEOUT = 4


# ============================================================
# CLOUDFLARE ENVIRONMENT VARIABLES
# ============================================================

CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID")
CF_D1_DATABASE_ID = os.getenv("CF_D1_DATABASE_ID")
CF_API_TOKEN = os.getenv("CF_API_TOKEN")


# ============================================================
# CLOUDFLARE D1 API
# ============================================================

def execute_d1_queries(queries):
    """
    Menjalankan query Cloudflare D1 melalui REST API.

    PENTING:
    Endpoint Cloudflare D1 /query membutuhkan object:

        {
            "queries": [...]
        }

    BUKAN:

        [...]
    """

    if not CF_ACCOUNT_ID:
        print("[CRITICAL ERROR] CF_ACCOUNT_ID tidak tersedia!")
        return {
            "success": False,
            "errors": [
                {
                    "message": "CF_ACCOUNT_ID tidak tersedia"
                }
            ]
        }

    if not CF_D1_DATABASE_ID:
        print("[CRITICAL ERROR] CF_D1_DATABASE_ID tidak tersedia!")
        return {
            "success": False,
            "errors": [
                {
                    "message": "CF_D1_DATABASE_ID tidak tersedia"
                }
            ]
        }

    if not CF_API_TOKEN:
        print("[CRITICAL ERROR] CF_API_TOKEN tidak tersedia!")
        return {
            "success": False,
            "errors": [
                {
                    "message": "CF_API_TOKEN tidak tersedia"
                }
            ]
        }

    if not isinstance(queries, list):
        print("[D1 ERROR] queries harus berupa list.")
        return {
            "success": False,
            "errors": [
                {
                    "message": "queries harus berupa list"
                }
            ]
        }

    if not queries:
        return {
            "success": True,
            "result": []
        }

    url = (
        f"https://api.cloudflare.com/client/v4/"
        f"accounts/{CF_ACCOUNT_ID}/"
        f"d1/database/{CF_D1_DATABASE_ID}/query"
    )

    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json"
    }

    # ========================================================
    # INI PERBAIKAN UTAMA
    #
    # SALAH:
    # json=queries
    #
    # BENAR:
    # json={"queries": queries}
    # ========================================================

    payload = {
        "queries": queries
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=60
        )

    except requests.exceptions.Timeout:
        print("[ERROR D1 API] Request timeout.")
        return {
            "success": False,
            "errors": [
                {
                    "message": "Cloudflare D1 API request timeout"
                }
            ]
        }

    except requests.exceptions.RequestException as e:
        print(f"[ERROR D1 API] Request exception: {e}")
        return {
            "success": False,
            "errors": [
                {
                    "message": str(e)
                }
            ]
        }

    # ========================================================
    # HTTP ERROR
    # ========================================================

    if response.status_code != 200:
        print(
            f"[ERROR D1 API] HTTP {response.status_code}: "
            f"{response.text[:2000]}"
        )

        try:
            return response.json()
        except ValueError:
            return {
                "success": False,
                "errors": [
                    {
                        "message": (
                            f"HTTP {response.status_code}: "
                            f"{response.text[:1000]}"
                        )
                    }
                ]
            }

    # ========================================================
    # PARSE JSON
    # ========================================================

    try:
        data = response.json()
    except ValueError:
        print(
            "[ERROR D1 API] Cloudflare mengembalikan "
            "response yang bukan JSON."
        )

        print(f"[ERROR D1 API RAW] {response.text[:2000]}")

        return {
            "success": False,
            "errors": [
                {
                    "message": "Response Cloudflare bukan JSON"
                }
            ]
        }

    # ========================================================
    # CLOUDFLARE API ERROR
    # ========================================================

    if not data.get("success", False):
        print(
            "[ERROR D1 API]",
            data
        )

    return data


# ============================================================
# CRAWLER
# ============================================================

class ProfessionalSearchCrawler:

    def __init__(
        self,
        seed_urls,
        max_run_seconds=MAX_RUN_SECONDS,
        concurrency=CONCURRENCY
    ):

        self.seed_urls = seed_urls

        self.max_run_seconds = max_run_seconds

        self.concurrency = concurrency

        # ----------------------------------------------------
        # QUEUE
        # ----------------------------------------------------

        self.queue = asyncio.PriorityQueue()

        # ----------------------------------------------------
        # URL STATE
        # ----------------------------------------------------

        self.visited_urls = set()

        self.visited_domains = set()

        self.domain_robots = {}

        # ----------------------------------------------------
        # DATA
        # ----------------------------------------------------

        self.pages_data = {}

        self.graph = {}

        self.domain_counts = {}

        # ----------------------------------------------------
        # LIMITS
        # ----------------------------------------------------

        self.MAX_PAGES_PER_DOMAIN = MAX_PAGES_PER_DOMAIN

        # ----------------------------------------------------
        # WORKER STATE
        # ----------------------------------------------------

        self.active_workers = 0

        self.worker_lock = asyncio.Lock()

        # ----------------------------------------------------
        # TIMER
        # ----------------------------------------------------

        self.start_time = None

        # ----------------------------------------------------
        # HTTP HEADERS
        # ----------------------------------------------------

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/123.0.0.0 "
                "Safari/537.36"
            ),

            "Accept": (
                "text/html,"
                "application/xhtml+xml,"
                "application/xml;q=0.9,"
                "image/avif,"
                "image/webp,"
                "*/*;q=0.8"
            ),

            "Accept-Language": (
                "id-ID,id;q=0.9,"
                "en-US;q=0.8,en;q=0.7"
            ),

            "Upgrade-Insecure-Requests": "1",

            "Sec-Fetch-Dest": "document",

            "Sec-Fetch-Mode": "navigate",

            "Sec-Fetch-Site": "none",

            "Sec-Fetch-User": "?1",
        }


    # ========================================================
    # TIME LIMIT
    # ========================================================

    def time_exceeded(self):
        """
        Mengecek apakah crawler sudah mencapai batas waktu.
        """

        if self.start_time is None:
            return False

        return (
            time.monotonic() - self.start_time
            >= self.max_run_seconds
        )


    def elapsed_seconds(self):
        if self.start_time is None:
            return 0

        return int(
            time.monotonic() - self.start_time
        )


    # ========================================================
    # SPAM DOMAIN
    # ========================================================

    def is_spam_domain(self, domain):

        spam_tlds = (
            ".cn",
            ".xyz",
            ".top",
            ".pw",
            ".tk",
            ".ml",
            ".ga",
            ".cf",
            ".gq",
            ".wang",
            ".icu",
            ".best",
            ".monster",
            ".work",
            ".click",
            ".loan"
        )

        domain = domain.lower().rstrip(".")

        return any(
            domain.endswith(tld)
            for tld in spam_tlds
        )


    # ========================================================
    # SPIDER TRAP
    # ========================================================

    def is_spider_trap(self, url):

        parsed = urlparse(url)

        path = parsed.path.lower()

        # URL terlalu panjang
        if len(url) > MAX_URL_LENGTH:
            return True

        # Path terlalu dalam
        path_segments = [
            p
            for p in path.split("/")
            if p
        ]

        if len(path_segments) > MAX_PATH_DEPTH:
            return True

        # Repeating path:
        # /abc/abc/
        if re.search(
            r"/(.+?)/\1/",
            path
        ):
            return True

        trap_keywords = (
            "login",
            "register",
            "signup",
            "signin",
            "logout",
            "cart",
            "checkout",
            "add-to-cart",
            "replytocom",
            "wp-json",
            "xmlrpc.php",
            "calendar",
            "event",
            "archive",
            "share.php",
            "print",
            "action=",
            "do=",
            "redirect=",
            "goto=",
            "feed/",
            "rss/",
            "trackback/"
        )

        lower_url = url.lower()

        return any(
            keyword in lower_url
            for keyword in trap_keywords
        )


    # ========================================================
    # CLEAN URL
    # ========================================================

    def clean_url_string(self, url):

        try:
            parsed = urlparse(url)
        except Exception:
            return ""

        scheme = parsed.scheme.lower()

        netloc = parsed.netloc.lower()

        path = parsed.path or "/"

        # Hilangkan fragment dan query.
        clean_url = (
            f"{scheme}://"
            f"{netloc}"
            f"{path}"
        )

        # Hilangkan trailing slash,
        # kecuali root "/".
        root_url = (
            f"{scheme}://"
            f"{netloc}/"
        )

        if (
            len(clean_url) > len(root_url)
            and clean_url.endswith("/")
        ):
            clean_url = clean_url[:-1]

        return clean_url


    # ========================================================
    # ROBOTS.TXT
    # ========================================================

    async def get_robots_rules(
        self,
        session,
        url
    ):

        parsed = urlparse(url)

        domain_base = (
            f"{parsed.scheme}://"
            f"{parsed.netloc}"
        )

        if domain_base in self.domain_robots:
            return self.domain_robots[
                domain_base
            ]

        robots_url = (
            f"{domain_base}/robots.txt"
        )

        rfp = RobotFileParser()

        rfp.set_url(robots_url)

        try:

            timeout = aiohttp.ClientTimeout(
                total=ROBOTS_TIMEOUT
            )

            async with session.get(
                robots_url,
                timeout=timeout,
                headers=self.headers
            ) as resp:

                if resp.status == 200:

                    content = await resp.text(
                        errors="ignore"
                    )

                    rfp.parse(
                        content.splitlines()
                    )

                else:

                    rfp.allow_all = True

        except Exception:

            rfp.allow_all = True

        self.domain_robots[
            domain_base
        ] = rfp

        return rfp


    # ========================================================
    # VALID URL
    # ========================================================

    def is_valid_url(self, url):

        try:
            parsed = urlparse(url)
        except Exception:
            return False

        if not parsed.netloc:
            return False

        if parsed.scheme not in (
            "http",
            "https"
        ):
            return False

        invalid_exts = (
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".pdf",
            ".zip",
            ".rar",
            ".7z",
            ".css",
            ".js",
            ".svg",
            ".mp4",
            ".mp3",
            ".webp",
            ".xml",
            ".json",
            ".ico",
            ".exe",
            ".dmg",
            ".iso",
            ".csv",
            ".xlsx",
            ".doc",
            ".docx"
        )

        path_lower = parsed.path.lower()

        if any(
            path_lower.endswith(ext)
            for ext in invalid_exts
        ):
            return False

        return True


    # ========================================================
    # CLEAN TEXT
    # ========================================================

    def clean_text(self, text):

        if not text:
            return ""

        return re.sub(
            r"\s+",
            " ",
            text
        ).strip()


    # ========================================================
    # FETCH PAGE
    # ========================================================

    async def fetch(
        self,
        session,
        url
    ):

        # Jangan mulai request baru
        # jika waktu sudah habis.
        if self.time_exceeded():
            return None, None, None

        try:

            timeout = aiohttp.ClientTimeout(
                total=HTTP_TIMEOUT
            )

            async with session.get(
                url,
                timeout=timeout,
                headers=self.headers,
                allow_redirects=True
            ) as response:

                # Setelah response datang,
                # cek lagi batas waktu.
                if self.time_exceeded():
                    return None, None, None

                content_type = (
                    response.headers
                    .get(
                        "Content-Type",
                        ""
                    )
                    .lower()
                )

                if (
                    response.status == 200
                    and "text/html"
                    in content_type
                ):

                    html = await response.text(
                        errors="ignore"
                    )

                    final_url = (
                        self.clean_url_string(
                            str(response.url)
                        )
                    )

                    last_mod = (
                        response.headers.get(
                            "Last-Modified"
                        )
                        or
                        response.headers.get(
                            "Date"
                        )
                    )

                    return (
                        final_url,
                        html,
                        last_mod
                    )

                return None, None, None

        except Exception:

            return None, None, None


    # ========================================================
    # PROCESS PAGE
    # ========================================================

    async def process_page(
        self,
        url,
        html,
        last_mod_header
    ):

        if self.time_exceeded():
            return

        if not html:
            return

        soup = BeautifulSoup(
            html,
            "html.parser"
        )

        domain_name = (
            urlparse(url).netloc
        )

        # ----------------------------------------------------
        # NOINDEX
        # ----------------------------------------------------

        robots_meta = soup.find(
            "meta",
            attrs={
                "name": re.compile(
                    r"^robots$",
                    re.I
                )
            }
        )

        if (
            robots_meta
            and robots_meta.get("content")
            and "noindex"
            in robots_meta["content"].lower()
        ):
            return

        # ----------------------------------------------------
        # LANGUAGE
        # ----------------------------------------------------

        html_tag = soup.find("html")

        language = (
            html_tag.get("lang")
            if html_tag
            and html_tag.get("lang")
            else "id"
        )

        language = (
            language
            .split("-")[0]
            .lower()[:5]
        )

        # ----------------------------------------------------
        # TITLE
        # ----------------------------------------------------

        title_tag = soup.find("title")

        title = (
            self.clean_text(
                title_tag.get_text()
            )
            if title_tag
            else domain_name
        )

        # ----------------------------------------------------
        # DESCRIPTION
        # ----------------------------------------------------

        snippet = ""

        meta_desc = (
            soup.find(
                "meta",
                attrs={
                    "name": re.compile(
                        r"^description$",
                        re.I
                    )
                }
            )
            or
            soup.find(
                "meta",
                attrs={
                    "property": re.compile(
                        r"^og:description$",
                        re.I
                    )
                }
            )
        )

        if (
            meta_desc
            and meta_desc.get("content")
        ):

            candidate = self.clean_text(
                meta_desc["content"]
            )

            if len(candidate) > 30:
                snippet = candidate

        # ----------------------------------------------------
        # FALLBACK TEXT EXTRACTION
        # ----------------------------------------------------

        if not snippet:

            for element in soup(
                [
                    "script",
                    "style",
                    "nav",
                    "header",
                    "footer",
                    "noscript",
                    "aside",
                    "form",
                    "button",
                    "svg"
                ]
            ):
                element.extract()

            main_content = (
                soup.find("main")
                or
                soup.find("article")
                or
                soup.find(
                    id=re.compile(
                        r"content|main",
                        re.I
                    )
                )
                or
                soup.body
            )

            if main_content:

                paragraphs = (
                    main_content.find_all("p")
                )

                valid_paragraphs = []

                for p in paragraphs:

                    text = self.clean_text(
                        p.get_text()
                    )

                    if len(text) > 35:
                        valid_paragraphs.append(
                            text
                        )

                if valid_paragraphs:

                    combined_text = (
                        " ... ".join(
                            valid_paragraphs
                        )
                    )

                    if len(combined_text) > 160:

                        snippet = (
                            combined_text[:160]
                            + "..."
                        )

                    else:

                        snippet = combined_text

                else:

                    raw_text = self.clean_text(
                        main_content.get_text(
                            separator=" "
                        )
                    )

                    if len(raw_text) > 160:

                        snippet = (
                            raw_text[:160]
                            + "..."
                        )

                    else:

                        snippet = raw_text

        if not snippet:
            snippet = title

        # ----------------------------------------------------
        # CONTENT HASH
        # ----------------------------------------------------

        content_hash = hashlib.md5(
            (
                title
                + snippet
            ).encode("utf-8")
        ).hexdigest()

        # ----------------------------------------------------
        # FAVICON
        # ----------------------------------------------------

        icon_tag = soup.find(
            "link",
            rel=lambda r:
                r
                and any(
                    "icon" in item.lower()
                    for item in (
                        r
                        if isinstance(r, list)
                        else [r]
                    )
                )
        )

        if (
            icon_tag
            and icon_tag.get("href")
        ):

            favicon = urljoin(
                url,
                icon_tag["href"]
            )

        else:

            favicon = (
                "https://www.google.com/"
                "s2/favicons"
                f"?domain={domain_name}"
                "&sz=64"
            )

        # ----------------------------------------------------
        # OG IMAGE
        # ----------------------------------------------------

        og_image = soup.find(
            "meta",
            attrs={
                "property": lambda x:
                    x
                    and x.lower()
                    == "og:image"
            }
        )

        if (
            og_image
            and og_image.get("content")
        ):

            thumbnail = urljoin(
                url,
                og_image["content"]
            )

        else:

            thumbnail = ""

        # ----------------------------------------------------
        # LAST MODIFIED
        # ----------------------------------------------------

        last_modified = last_mod_header

        if not last_modified:

            meta_mod = (
                soup.find(
                    "meta",
                    attrs={
                        "property":
                        lambda x:
                            x
                            and
                            "modified_time"
                            in x.lower()
                    }
                )
                or
                soup.find(
                    "meta",
                    attrs={
                        "property":
                        lambda x:
                            x
                            and
                            "published_time"
                            in x.lower()
                    }
                )
            )

            if (
                meta_mod
                and meta_mod.get("content")
            ):

                last_modified = (
                    meta_mod["content"]
                )

        # ----------------------------------------------------
        # OUTGOING LINKS
        # ----------------------------------------------------

        outgoing_links = set()

        for link in soup.find_all(
            "a",
            href=True
        ):

            # Jika waktu habis,
            # hentikan ekstraksi link.
            if self.time_exceeded():
                break

            rel_attr = link.get("rel")

            if rel_attr:

                rel_lower = [
                    r.lower()
                    for r in rel_attr
                ]

                if "nofollow" in rel_lower:
                    continue

            raw_url = urljoin(
                url,
                link["href"]
            )

            try:
                parsed_raw = urlparse(
                    raw_url
                )
            except Exception:
                continue

            # Query URL tidak dicrawl
            if parsed_raw.query:
                continue

            clean_url = (
                self.clean_url_string(
                    raw_url
                )
            )

            if not self.is_valid_url(
                clean_url
            ):
                continue

            target_domain = (
                urlparse(
                    clean_url
                ).netloc
            )

            if self.is_spam_domain(
                target_domain
            ):
                continue

            if self.is_spider_trap(
                clean_url
            ):
                continue

            outgoing_links.add(
                clean_url
            )

            # ------------------------------------------------
            # DISCOVER DOMAIN
            # ------------------------------------------------

            root_domain_url = (
                f"{urlparse(clean_url).scheme}"
                f"://"
                f"{target_domain}/"
            )

            if (
                target_domain
                not in self.visited_domains
                and
                root_domain_url
                not in self.visited_urls
            ):

                self.visited_domains.add(
                    target_domain
                )

                await self.queue.put(
                    (
                        0,
                        root_domain_url
                    )
                )

            # ------------------------------------------------
            # DOMAIN COUNT
            # ------------------------------------------------

            if (
                target_domain
                not in self.domain_counts
            ):

                self.domain_counts[
                    target_domain
                ] = 0

            # ------------------------------------------------
            # MAX PAGES PER DOMAIN
            # ------------------------------------------------

            if (
                self.domain_counts[
                    target_domain
                ]
                >= self.MAX_PAGES_PER_DOMAIN
            ):
                continue

            # ------------------------------------------------
            # ADD URL TO QUEUE
            # ------------------------------------------------

            if clean_url in self.visited_urls:
                continue

            self.visited_urls.add(
                clean_url
            )

            self.domain_counts[
                target_domain
            ] += 1

            path_segments = [
                p
                for p in urlparse(
                    clean_url
                ).path.split("/")
                if p
            ]

            priority_score = (
                len(path_segments) * 10
            )

            if (
                target_domain.count(".") > 1
                and "www"
                not in target_domain
            ):
                priority_score -= 5

            await self.queue.put(
                (
                    priority_score,
                    clean_url
                )
            )

        # ----------------------------------------------------
        # SAVE PAGE
        # ----------------------------------------------------

        self.pages_data[url] = {
            "url": url,
            "domain": domain_name,
            "title": title,
            "snippet": snippet,
            "favicon": favicon,
            "thumbnail": thumbnail,
            "last_modified": last_modified,
            "content_hash": content_hash,
            "language": language
        }

        self.graph[url] = list(
            outgoing_links
        )


    # ========================================================
    # WORKER
    # ========================================================

    async def worker(
        self,
        session
    ):

        while True:

            # ------------------------------------------------
            # HARD TIME LIMIT
            # ------------------------------------------------

            if self.time_exceeded():
                break

            try:

                priority, url = (
                    await asyncio.wait_for(
                        self.queue.get(),
                        timeout=2.0
                    )
                )

            except asyncio.TimeoutError:

                if self.time_exceeded():
                    break

                async with self.worker_lock:

                    if (
                        self.queue.empty()
                        and
                        self.active_workers == 0
                    ):
                        break

                continue

            # ------------------------------------------------
            # CHECK AGAIN AFTER QUEUE
            # ------------------------------------------------

            if self.time_exceeded():

                self.queue.task_done()

                break

            async with self.worker_lock:

                self.active_workers += 1

            try:

                # ------------------------------------------------
                # ROBOTS
                # ------------------------------------------------

                robots_rules = (
                    await self.get_robots_rules(
                        session,
                        url
                    )
                )

                if not robots_rules.can_fetch(
                    self.headers["User-Agent"],
                    url
                ):

                    continue

                # ------------------------------------------------
                # CRAWL DELAY
                # ------------------------------------------------

                crawl_delay = (
                    robots_rules.crawl_delay(
                        self.headers[
                            "User-Agent"
                        ]
                    )
                )

                if crawl_delay:

                    # Jangan tidur melewati batas
                    # crawler.
                    remaining = (
                        self.max_run_seconds
                        - self.elapsed_seconds()
                    )

                    if remaining <= 0:
                        break

                    await asyncio.sleep(
                        min(
                            crawl_delay,
                            remaining
                        )
                    )

                # ------------------------------------------------
                # CHECK TIME BEFORE REQUEST
                # ------------------------------------------------

                if self.time_exceeded():
                    break

                # ------------------------------------------------
                # LOG
                # ------------------------------------------------

                elapsed = (
                    self.elapsed_seconds()
                )

                if (
                    len(self.pages_data) % 10 == 0
                    and
                    len(self.pages_data) > 0
                ):

                    print(
                        f"[{elapsed}s/"
                        f"{self.max_run_seconds}s] "
                        f"[{len(self.pages_data)} "
                        f"terindeks] "
                        f"Merayapi: {url}"
                    )

                # ------------------------------------------------
                # FETCH
                # ------------------------------------------------

                final_url, html, last_mod = (
                    await self.fetch(
                        session,
                        url
                    )
                )

                if not html:
                    continue

                # ------------------------------------------------
                # PROCESS
                # ------------------------------------------------

                try:

                    await self.process_page(
                        final_url or url,
                        html,
                        last_mod
                    )

                except Exception as e:

                    print(
                        "[PAGE PROCESS ERROR] "
                        f"{url} -> {e}"
                    )

            except Exception as e:

                print(
                    "[WORKER ERROR] "
                    f"{url} -> {e}"
                )

            finally:

                async with self.worker_lock:

                    self.active_workers -= 1

                self.queue.task_done()


    # ========================================================
    # RUN CRAWLER
    # ========================================================

    async def run(self):

        self.start_time = time.monotonic()

        # ----------------------------------------------------
        # SEED
        # ----------------------------------------------------

        for url in self.seed_urls:

            if self.time_exceeded():
                break

            clean_seed = (
                self.clean_url_string(
                    url
                )
            )

            if not clean_seed:
                continue

            parsed = urlparse(
                clean_seed
            )

            self.visited_domains.add(
                parsed.netloc
            )

            self.visited_urls.add(
                clean_seed
            )

            await self.queue.put(
                (
                    0,
                    clean_seed
                )
            )

        # ----------------------------------------------------
        # HTTP CONNECTOR
        # ----------------------------------------------------

        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=10,
            ttl_dns_cache=300
        )

        async with aiohttp.ClientSession(
            connector=connector
        ) as session:

            tasks = [
                asyncio.create_task(
                    self.worker(session)
                )
                for _ in range(
                    self.concurrency
                )
            ]

            # ------------------------------------------------
            # WAIT FOR WORKERS
            # ------------------------------------------------

            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )

        elapsed = self.elapsed_seconds()

        print(
            "\n[CRAWLER FINISH]"
        )

        print(
            f"Waktu berjalan: "
            f"{elapsed} detik"
        )

        print(
            f"Halaman berhasil diproses: "
            f"{len(self.pages_data)}"
        )

        if elapsed >= self.max_run_seconds:

            print(
                "[TIME LIMIT] "
                "Crawler berhenti karena "
                "batas maksimum 1 jam tercapai."
            )

        else:

            print(
                "[QUEUE FINISH] "
                "Tidak ada pekerjaan tersisa."
            )


# ============================================================
# GET EXISTING URL FROM D1
# ============================================================

def get_already_visited_urls_d1():

    """
    Mengambil URL yang sudah ada di D1.

    Tujuannya agar crawler tidak mulai dari nol
    setiap kali GitHub Actions dijalankan.
    """

    visited = set()

    print(
        "\n[CLOUDFLARE SYNC] "
        "Mengambil daftar URL lama dari D1..."
    )

    queries = [
        {
            "sql": (
                "SELECT url "
                "FROM documents"
            )
        }
    ]

    try:

        data = execute_d1_queries(
            queries
        )

        if not isinstance(
            data,
            dict
        ):

            print(
                "[CLOUDFLARE SYNC ERROR] "
                "Response D1 tidak valid."
            )

            return visited

        if not data.get(
            "success",
            False
        ):

            print(
                "[CLOUDFLARE SYNC ERROR] "
                "Query D1 gagal:"
            )

            print(data)

            return visited

        results = []

        # ----------------------------------------------------
        # Response D1 biasanya:
        #
        # result: [
        #   {
        #       "results": [...]
        #   }
        # ]
        # ----------------------------------------------------

        raw_result = data.get(
            "result",
            []
        )

        if isinstance(
            raw_result,
            list
        ):

            for result_item in raw_result:

                if not isinstance(
                    result_item,
                    dict
                ):
                    continue

                rows = result_item.get(
                    "results",
                    []
                )

                if isinstance(
                    rows,
                    list
                ):

                    results.extend(
                        rows
                    )

        # ----------------------------------------------------
        # NORMALIZE URL
        # ----------------------------------------------------

        for row in results:

            if not isinstance(
                row,
                dict
            ):
                continue

            url_val = row.get(
                "url"
            )

            if not url_val:
                continue

            try:

                parsed = urlparse(
                    url_val
                )

                if (
                    parsed.scheme
                    not in (
                        "http",
                        "https"
                    )
                    or
                    not parsed.netloc
                ):
                    continue

                clean = (
                    f"{parsed.scheme.lower()}"
                    f"://"
                    f"{parsed.netloc.lower()}"
                    f"{parsed.path or '/'}"
                )

                root = (
                    f"{parsed.scheme.lower()}"
                    f"://"
                    f"{parsed.netloc.lower()}/"
                )

                if (
                    len(clean) > len(root)
                    and clean.endswith("/")
                ):

                    clean = clean[:-1]

                visited.add(
                    clean
                )

            except Exception:
                continue

        print(
            "[CLOUDFLARE SYNC DONE] "
            f"Terbaca: {len(visited)} "
            "URL lama berhasil "
            "disinkronisasi!"
        )

    except Exception as e:

        print(
            "[CLOUDFLARE SYNC WARNING] "
            f"Exception: {e}"
        )

    return visited


# ============================================================
# PUSH DOCUMENTS TO D1
# ============================================================

def push_to_d1(
    crawled_data,
    graph_data,
    batch_size=D1_BATCH_SIZE
):

    print(
        "\n[CLOUDFLARE PUSH] "
        f"Memulai upload "
        f"{len(crawled_data)} "
        "dokumen ke D1..."
    )

    if not crawled_data:

        print(
            "[CLOUDFLARE PUSH] "
            "Tidak ada dokumen baru."
        )

        return

    # --------------------------------------------------------
    # DOCUMENT CHUNKS
    # --------------------------------------------------------

    chunks = [
        crawled_data[
            i:i + batch_size
        ]
        for i in range(
            0,
            len(crawled_data),
            batch_size
        )
    ]

    success_docs = 0

    # --------------------------------------------------------
    # DOCUMENT BATCHES
    # --------------------------------------------------------

    for chunk_idx, chunk in enumerate(
        chunks
    ):

        queries = []

        for page in chunk:

            queries.append(
                {
                    "sql": """
                        INSERT INTO documents (
                            url,
                            domain,
                            title,
                            snippet,
                            favicon,
                            thumbnail,
                            created_at,
                            last_modified,
                            content_hash,
                            language
                        )
                        VALUES (
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            CURRENT_TIMESTAMP,
                            ?,
                            ?,
                            ?
                        )
                        ON CONFLICT(url)
                        DO UPDATE SET
                            title = excluded.title,
                            snippet = excluded.snippet,
                            favicon = excluded.favicon,
                            thumbnail = excluded.thumbnail,
                            language = excluded.language,
                            last_modified =
                                CASE
                                    WHEN
                                        excluded.content_hash
                                        != documents.content_hash
                                    THEN CURRENT_TIMESTAMP

                                    ELSE COALESCE(
                                        excluded.last_modified,
                                        documents.last_modified
                                    )
                                END,
                            content_hash =
                                excluded.content_hash;
                    """,

                    "params": [
                        page.get("url"),
                        page.get("domain"),
                        page.get("title"),
                        page.get("snippet"),
                        page.get("favicon"),
                        page.get("thumbnail"),
                        page.get("last_modified"),
                        page.get("content_hash"),
                        page.get("language")
                    ]
                }
            )

        try:

            res = execute_d1_queries(
                queries
            )

            if (
                isinstance(res, dict)
                and
                res.get(
                    "success",
                    False
                )
            ):

                success_docs += len(chunk)

                print(
                    "[CLOUDFLARE PUSH] "
                    f"Batch Dokumen "
                    f"{chunk_idx + 1}/"
                    f"{len(chunks)} OK "
                    f"({len(chunk)} dokumen)"
                )

            else:

                print(
                    "[CLOUDFLARE PUSH ERROR] "
                    f"Batch "
                    f"{chunk_idx + 1}/"
                    f"{len(chunks)} gagal:"
                )

                print(res)

        except Exception as e:

            print(
                "[CLOUDFLARE PUSH ERROR] "
                f"Batch Dokumen "
                f"{chunk_idx + 1} "
                f"exception: {e}"
            )

    # ========================================================
    # GRAPH
    # ========================================================

    print(
        "\n[CLOUDFLARE PUSH] "
        "Menyimpan struktur graph link..."
    )

    graph_queries = []

    for source, targets in (
        graph_data.items()
    ):

        if not source:
            continue

        if not isinstance(
            targets,
            list
        ):
            continue

        for target in targets:

            if not target:
                continue

            if source == target:
                continue

            graph_queries.append(
                {
                    "sql": """
                        INSERT INTO page_graph (
                            source_url,
                            target_url
                        )
                        VALUES (?, ?)
                        ON CONFLICT(
                            source_url,
                            target_url
                        )
                        DO NOTHING;
                    """,

                    "params": [
                        source,
                        target
                    ]
                }
            )

    # --------------------------------------------------------
    # GRAPH CHUNKS
    # --------------------------------------------------------

    graph_chunks = [
        graph_queries[
            i:i + D1_GRAPH_BATCH_SIZE
        ]
        for i in range(
            0,
            len(graph_queries),
            D1_GRAPH_BATCH_SIZE
        )
    ]

    graph_success = 0

    for i, g_chunk in enumerate(
        graph_chunks
    ):

        try:

            result = execute_d1_queries(
                g_chunk
            )

            if (
                isinstance(result, dict)
                and
                result.get(
                    "success",
                    False
                )
            ):

                graph_success += len(
                    g_chunk
                )

                print(
                    "[CLOUDFLARE GRAPH] "
                    f"Batch "
                    f"{i + 1}/"
                    f"{len(graph_chunks)} OK"
                )

            else:

                print(
                    "[CLOUDFLARE GRAPH ERROR] "
                    f"Batch "
                    f"{i + 1}/"
                    f"{len(graph_chunks)} gagal:"
                )

                print(result)

        except Exception as e:

            print(
                "[CLOUDFLARE GRAPH ERROR] "
                f"Batch {i + 1} "
                f"exception: {e}"
            )

    # --------------------------------------------------------
    # FINISH
    # --------------------------------------------------------

    print(
        "\n[CLOUDFLARE FINISH] "
        f"Selesai!"
    )

    print(
        f"Dokumen berhasil diproses: "
        f"{success_docs}/{len(crawled_data)}"
    )

    print(
        f"Relasi graph diproses: "
        f"{graph_success}/{len(graph_queries)}"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    # ========================================================
    # SEED URLS
    # ========================================================

    initial_seeds = [

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

    # ========================================================
    # CHECK ENVIRONMENT
    # ========================================================

    print("=" * 60)

    print(
        "DEEVV SEARCH - PROFESSIONAL WEB CRAWLER"
    )

    print("=" * 60)

    if not CF_ACCOUNT_ID:

        print(
            "[CRITICAL] "
            "CF_ACCOUNT_ID tidak ditemukan."
        )

        sys.exit(1)

    if not CF_D1_DATABASE_ID:

        print(
            "[CRITICAL] "
            "CF_D1_DATABASE_ID tidak ditemukan."
        )

        sys.exit(1)

    if not CF_API_TOKEN:

        print(
            "[CRITICAL] "
            "CF_API_TOKEN tidak ditemukan."
        )

        sys.exit(1)

    print(
        "[CONFIG] Cloudflare D1 credentials: OK"
    )

    print(
        "[CONFIG] Maximum crawler runtime: "
        "3600 seconds (1 hour)"
    )

    print(
        f"[CONFIG] Concurrency: "
        f"{CONCURRENCY}"
    )

    print(
        f"[CONFIG] Max pages/domain: "
        f"{MAX_PAGES_PER_DOMAIN}"
    )

    # ========================================================
    # SYNC OLD URLS
    # ========================================================

    existing_urls = (
        get_already_visited_urls_d1()
    )

    print(
        f"\n[SYNC] "
        f"{len(existing_urls)} URL "
        "lama ditemukan di D1."
    )

    # ========================================================
    # CREATE CRAWLER
    # ========================================================

    crawler = ProfessionalSearchCrawler(
        seed_urls=initial_seeds,

        # HARD LIMIT 1 JAM
        max_run_seconds=3600,

        concurrency=15
    )

    # ========================================================
    # IMPORTANT:
    #
    # URL yang sudah ada di D1 dimasukkan ke visited_urls.
    #
    # Dengan begitu crawler tidak mengcrawl ulang
    # halaman yang sudah tersimpan.
    # ========================================================

    crawler.visited_urls.update(
        existing_urls
    )

    print(
        "\n[START CRAWLER] "
        "Memulai perayapan web D1..."
    )

    print(
        "[START CRAWLER] "
        "Batas maksimum: 1 JAM"
    )

    print(
        "[START CRAWLER] "
        "URL lama yang dilewati: "
        f"{len(existing_urls)}"
    )

    # ========================================================
    # RUN
    # ========================================================

    try:

        asyncio.run(
            crawler.run()
        )

    except KeyboardInterrupt:

        print(
            "\n[STOP] "
            "Crawler dihentikan manual."
        )

    except Exception as e:

        print(
            "\n[CRITICAL CRAWLER ERROR]"
        )

        print(e)

    # ========================================================
    # COLLECT RESULTS
    # ========================================================

    crawled_results = list(
        crawler.pages_data.values()
    )

    graph_results = crawler.graph

    # ========================================================
    # UPLOAD TO D1
    # ========================================================

    if crawled_results:

        print(
            "\n[POST-CRAWL] "
            f"Ditemukan "
            f"{len(crawled_results)} "
            "halaman hasil crawling."
        )

        push_to_d1(
            crawled_results,
            graph_results
        )

    else:

        print(
            "\n[INFO] "
            "Tidak ada halaman baru "
            "yang berhasil dicrawl."
        )

        print(
            "[INFO] "
            "Upload D1 dilewati."
        )

    # ========================================================
    # FINAL
    # ========================================================

    print(
        "\n" + "=" * 60
    )

    print(
        "CRAWLER SELESAI"
    )

    print(
        "=" * 60
    )

    print(
        f"Total halaman baru: "
        f"{len(crawled_results)}"
    )

    print(
        f"Total graph source: "
        f"{len(graph_results)}"
    )

    print(
        f"Waktu crawler: "
        f"{crawler.elapsed_seconds()} detik"
    )

    print(
        "=" * 60
    )
