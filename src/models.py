import asyncio
import logging
from urllib.parse import urljoin, urlparse
import json
from bs4 import BeautifulSoup
from typing import Any, Dict, Optional
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError
import time
import heapq
import aiohttp
logger = logging.getLogger(__name__)

class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, timeout: Optional[aiohttp.ClientTimeout] = None, filter_external_links: bool = False, max_depth: int = 2,
        per_domain_limit: int = 2,):
        self.max_concurrent = max_concurrent
        self.timeout = timeout or aiohttp.ClientTimeout(
            total=30,       # Общий таймаут на весь запрос (от начала до чтения ответа)
            connect=5,      # Таймаут на получение соединения из пула и установку TCP/SSL
            sock_connect=3, # Таймаут исключительно на установку сокет-соединения
            sock_read=10,   # Таймаут на ожидание данных от сервера между пакетами
         )
        self.max_depth = max_depth
        self.sems = SemaphoreManager(
            global_limit=max_concurrent, per_domain_limit=per_domain_limit,
        )
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        self.parser = HTMLParser(filter_external=filter_external_links)
        

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
                ttl_dns_cache=300,
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=self.timeout,
            )
        return self._session

    async def _fetch_guarded(self, url: str) -> tuple[int, str]:
        """fetch_with_status под семафорами (глобальный + per-domain)."""
        async with self.sems.limit(url):
            return await self.fetch_with_status(url)
        
    async def fetch_with_status(self, url: str) -> tuple[int, str]:
        """Как fetch_url, но возвращает (status_code, body)."""
        session = await self._get_session()
        logger.info("Начало загрузки: %s", url)
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                logger.info(
                    "Успешно загружено: %s (status=%s, len=%d)",
                    url, response.status, len(text),
                )
                return response.status, text
        except aiohttp.ClientResponseError as e:
            logger.error("HTTP ошибка для %s: status=%s, message=%s", url, e.status, e.message)
            raise AsyncClientResponseError(f"{url}: HTTP {e.status}", status=e.status) from e
        except asyncio.TimeoutError as e:
            logger.error("Таймаут при загрузке %s", url)
            raise AsyncTimeoutError(f"{url}: timeout") from e
        except aiohttp.ClientError as e:
            logger.error("Сетевая ошибка для %s: %s", url, e)
            raise AsyncClientError(f"{url}: {e}") from e

    async def fetch_url(self, url: str) -> str:
        _, text = await self.fetch_with_status(url)
        return text

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        results: dict[str, str] = {}
        tasks = [
        self._fetch_guarded(url)
        for url in urls
            ]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for url, result in zip(urls, gathered):
            if isinstance(result, BaseException):
                logger.error(
                    "Не удалось загрузить %s: %s: %s",
                    url, type(result).__name__, result,
                )
                continue
            _, text = result
            results[url] = text
        return results


    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connector = None

    async def __aenter__(self) -> "AsyncCrawler":
        await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def fetch_and_parse(self, url: str) -> dict:
        base = {
            "url": url, "title": "", "text": "", "links": [],
            "metadata": {}, "images": [], "headings": {"h1": [], "h2": [], "h3": []},
            "tables": [], "lists": {"ul": [], "ol": []}, "error": None,
        }
        try:
            _, html = await self._fetch_guarded(url)
        except (AsyncClientError, AsyncTimeoutError, AsyncClientResponseError) as e:
            logger.error("fetch_and_parse: ошибка загрузки %s: %s", url, e)
            base["error"] = f"fetch: {type(e).__name__}: {e}"
            return base

        parsed = await self.parser.parse_html(html, url)
        return parsed
    
    async def fetch_and_parse_many(self, urls: list[str]) -> dict[str, dict]:
        tasks = [self.fetch_and_parse(u) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out: dict[str, dict] = {}
        for url, r in zip(urls, results):
            if isinstance(r, BaseException):
                logger.error("fetch_and_parse_many: %s: %s", url, r)
                out[url] = {"url": url, "error": f"{type(r).__name__}: {r}"}
            else:
                out[url] = r
        return out

    async def _process_one(
        self, queue: CrawlerQueue, url: str, depth: int, flt: URLFilter, start_host: str,) -> dict:
        result = {
            "url": url, "depth": depth, "title": "", "text": "",
            "links": [], "metadata": {}, "error": None,
        }
        try:
            _, html = await self._fetch_guarded(url)
            parsed = await self.parser.parse_html(html, url)

            result.update({
                "title": parsed["title"],
                "text": parsed["text"],
                "links": parsed["links"],
                "metadata": parsed["metadata"],
                "error": parsed.get("error"),
            })

            if depth < self.max_depth:
                for link in parsed["links"]:
                    if flt.allowed(link, start_host):
                        await queue.add_url(link, priority=-depth, depth=depth + 1)

            await queue.mark_processed(url, result)
        except AsyncClientResponseError as e:
            result["error"] = f"HTTP {e.status}"
            await queue.mark_failed(url, result["error"])
        except AsyncTimeoutError:
            result["error"] = "timeout"
            await queue.mark_failed(url, result["error"])
        except AsyncClientError as e:
            result["error"] = f"network: {e}"
            await queue.mark_failed(url, result["error"])
        except Exception as e:  # noqa: BLE001 — не роняем воркер
            logger.exception("Неожиданная ошибка при обработке %s", url)
            result["error"] = f"unexpected: {e}"
            await queue.mark_failed(url, result["error"])
        return result
    
    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = False,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        progress_every: int = 1,
    ) -> dict[str, dict]:
        queue = CrawlerQueue()
        self.queue = queue  

        start_host = urlparse(start_urls[0]).netloc #if start_urls else None
        flt = URLFilter(
            same_domain_only=same_domain_only,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

        results: dict[str, dict] = {}
        for u in start_urls:
            if flt.allowed(u, start_host):
                await queue.add_url(u, priority=0, depth=0)

        if not queue:
            logger.info("Нет URL для обхода после фильтрации")
            return results

        stop = asyncio.Event()
        pages_done = 0

        pages_lock = asyncio.Lock()
        reserved_pages = 0
        async def reserve_page() -> bool: 
            nonlocal reserved_pages

            async with pages_lock:
                if len(results) + reserved_pages >= max_pages:
                    return False

                reserved_pages += 1
                return True

        async def release_page(success: bool, url: str, result: dict):
            nonlocal reserved_pages

            async with pages_lock:
                reserved_pages -= 1

                if success:
                    results[url] = result

                if len(results) >= max_pages:
                    stop.set()
                    queue.close()
        
        async def progress():
            nonlocal pages_done
            while not stop.is_set():
                s = queue.get_stats()
                if s["total_done"] != pages_done:
                    pages_done = s["total_done"]
                    logger.info(
                        "Прогресс: done=%d queued=%d failed=%d rate=%.2f/s",
                        s["total_done"], s["queued"], s["failed"], s["rate_per_sec"],
                    )
                await asyncio.sleep(0.5)

        async def worker():
            while not stop.is_set():
                if not await reserve_page():
                    stop.set()
                    queue.close()
                    return
                item = await queue.get_next_with_depth()
                if item is None:
                    return
  
                url, depth = item
                
                result = await self._process_one(
                    queue,
                    url,
                    depth,
                    flt,
                    start_host,
                        )

                await release_page(
                    result["error"] is None,
                    url,
                    result,
                )

        n_workers = max(1, min(self.max_concurrent, 20))
        workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
        prog_task = asyncio.create_task(progress())

        try:
            await asyncio.gather(*workers)
        finally:
            stop.set()
            queue.close()
            prog_task.cancel()
            try:
                await prog_task
            except asyncio.CancelledError:
                pass

        logger.info("Краулинг завершён: %d страниц", len(results))
        return results
    

_NOISE_TAGS = ("script", "style", "noscript", "template", "iframe", "svg")

class HTMLParser:

    def __init__(self, filter_external: bool = False):
        self.filter_external = filter_external

    async def parse_html(self, html: str, url: str) -> dict:
        result: dict = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": {"h1": [], "h2": [], "h3": []},
            "tables": [],
            "lists": {"ul": [], "ol": []},
            "error": None,
        }

        if not html:
            logger.warning("Пустой HTML для %s", url)
            result["error"] = "empty html"
            return result

        try:
            soup = self._make_soup(html)
        except Exception as e:  
            logger.warning("Не удалось распарсить HTML %s: %s", url, e)
            result["error"] = f"parse error: {e}"
            return result

        for key, fn in (
            ("metadata", lambda: self.extract_metadata(soup)),
            ("links", lambda: self.extract_links(soup, url)),
            ("text", lambda: self.extract_text(soup)),
            ("images", lambda: self.extract_images(soup, url)),
            ("headings", lambda: self.extract_headings(soup)),
            ("tables", lambda: self.extract_tables(soup)),
            ("lists", lambda: self.extract_lists(soup)),
        ):
            try:
                result[key] = fn()
            except Exception as e:  
                logger.warning("Ошибка извлечения '%s' для %s: %s", key, url, e)
                result["error"] = f"{key}: {e}"

        result["title"] = result["metadata"].get("title", "")
        return result

    def _make_soup(self, html: str) -> BeautifulSoup:
        try:
            return BeautifulSoup(html, "lxml")
        except Exception: 
            logger.debug("lxml недоступен, используем html.parser")
            return BeautifulSoup(html, "html.parser")

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        if soup is None:
            return []

        base_host = urlparse(base_url).netloc
        seen: set[str] = set()
        links: list[str] = []

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue

            try:
                absolute = urljoin(base_url, href)
            except Exception as e: 
                logger.debug("Не удалось склеить URL %r с %r: %s", href, base_url, e)
                continue

            parsed = urlparse(absolute)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                continue  

            if self.filter_external and parsed.netloc != base_host:
                continue

            normalized = parsed._replace(fragment="").geturl()
            if normalized not in seen:
                seen.add(normalized)
                links.append(normalized)

        return links


    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:

        if soup is None:
            return ""

        root = soup.select_one(selector) if selector else soup
        if root is None:
            logger.debug("Селектор %r не найден", selector)
            return ""

        root = BeautifulSoup(str(root), "html.parser")

        for tag in root(_NOISE_TAGS):
            tag.decompose()

        text = root.get_text(separator=" ")
        return " ".join(text.split())


    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        meta: dict = {"title": "", "description": "", "keywords": []}
        if soup is None:
            return meta

        if soup.title and soup.title.string:
            meta["title"] = soup.title.string.strip()

        desc = soup.find("meta", attrs={"name": "description"})
        if desc and desc.get("content"):
            meta["description"] = desc["content"].strip()

        kw = soup.find("meta", attrs={"name": "keywords"})
        if kw and kw.get("content"):
            meta["keywords"] = [
                k.strip() for k in kw["content"].split(",") if k.strip()
            ]

        for prop in ("og:title", "og:description"):
            tag = soup.find("meta", attrs={"property": prop})
            if tag and tag.get("content"):
                meta.setdefault(prop, tag["content"].strip())

        return meta


    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        if soup is None:
            return []
        images: list[dict] = []
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            try:
                src = urljoin(base_url, src)
            except Exception:  
                continue
            images.append({"src": src, "alt": (img.get("alt") or "").strip()})
        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        if soup is None:
            return {"h1": [], "h2": [], "h3": []}
        out: dict[str, list[str]] = {}
        for level in ("h1", "h2", "h3"):
            out[level] = [
                " ".join(tag.get_text(" ").split())
                for tag in soup.find_all(level)
            ]
        return out

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        if soup is None:
            return []
        tables: list[list[list[str]]] = []
        for table in soup.find_all("table"):
            rows: list[list[str]] = []
            for tr in table.find_all("tr"):
                cells = [
                    " ".join(c.get_text(" ").split())
                    for c in tr.find_all(["th", "td"])
                ]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> dict[str, list[list[str]]]:
        if soup is None:
            return {"ul": [], "ol": []}
        out: dict[str, list[list[str]]] = {"ul": [], "ol": []}
        for tag_name in ("ul", "ol"):
            for lst in soup.find_all(tag_name):
                items = [
                    " ".join(li.get_text(" ").split())
                    for li in lst.find_all("li", recursive=False)
                ]
                if items:
                    out[tag_name].append(items)
        return out

class CrawlerQueue:
    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str, int]] = []
        self._seq = 0

        self._queued: set[str] = set()       # URL, лежащие в очереди
        self._processed: dict[str, dict] = {}  # url -> {depth, time}
        self._failed: dict[str, str] = {}     # url -> error

        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()
        self._in_progress = 0
        self._closed = False
        self._depth_cache: dict[str, int] = {}
        self._start_time = time.perf_counter()

    async def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        async with self._lock:
            if url in self._queued or url in self._processed or url in self._failed:
                return False
            self._seq += 1
            heapq.heappush(self._heap, (-priority, self._seq, url, depth))
            self._queued.add(url)
            self._not_empty.set()
            return True

    async def get_next_with_depth(self) -> tuple[str, int] | None:
        while True:
            async with self._lock:
                if self._heap:
                    _, _, url, depth = heapq.heappop(self._heap)
                    self._queued.discard(url)
                    self._in_progress += 1
                    return url, depth
                # куча пуста; если никто не работает — точно всё
                if self._in_progress == 0:
                    return None
                # кто-то работает — возможно, добавит ещё ссылки
                self._not_empty.clear()
            try:
                await asyncio.wait_for(self._not_empty.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

    async def get_next(self) -> str | None :
        item = await self.get_next_with_depth()
        return item[0] if item else None
    
    async def mark_processed(self, url: str, result: dict | None = None):
        async with self._lock:
            self._in_progress = max(0, self._in_progress - 1)
            self._processed[url] = {
                "processed_at": time.time(),
                "result_size": len(result or {}),
            }
            self._not_empty.set()
    
    async def mark_failed(self, url: str, error: str):
        async with self._lock:
            self._in_progress = max(0, self._in_progress - 1)
            self._failed[url] = error
            self._not_empty.set()
            logger.warning("URL помечен как failed: %s (%s)", url, error)
    
    def close(self) -> None:
        self._closed = True
        self._not_empty.set()
    
    @property
    def visited_urls(self) -> set[str]:
        return set(self._processed) | set(self._failed)
    
    @property
    def processed_urls(self) -> dict[str, dict]:
        return dict(self._processed)

    @property
    def failed_urls(self) -> dict[str, str]:
        return dict(self._failed)

    def __len__(self) -> int:
        return len(self._heap)
    def get_stats(self) -> dict: 
        elapsed = max(time.perf_counter() - self._start_time, 1e-9)
        done = len(self._processed) + len(self._failed)
        return {
            "queued": len(self._queued),
            "in_progress": self._in_progress,
            "processed": len(self._processed),
            "failed": len(self._failed),
            "total_done": done,
            "rate_per_sec": done / elapsed,
            "elapsed_sec": round(elapsed, 2),
        }
from collections import defaultdict
from contextlib import asynccontextmanager
class SemaphoreManager:
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 2) -> None:
        self.global_sem = asyncio.Semaphore(global_limit)
        self.per_domain_limit = per_domain_limit
        self._domain_sems: dict[str, asyncio.Semaphore] = {}
        self._active: dict[str, int] = defaultdict(int)
        self._active_total = 0
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def limit(self, url: str):
        domain = urlparse(url).netloc
        async with self._lock:
            if domain not in self._domain_sems:
                self._domain_sems[domain] = asyncio.Semaphore(self.per_domain_limit)
            domain_sem = self._domain_sems[domain]

        async with self.global_sem:
            async with domain_sem:
                async with self._lock:
                    self._active[domain] += 1
                    self._active_total += 1
                try:
                    yield
                finally:
                    async with self._lock:
                        self._active[domain] -= 1
                        self._active_total -= 1
    @property
    def active_total(self) -> int:
        return self._active_total

    def active_by_domain(self) -> dict[str, int]:
        return dict(self._active)

from typing import Iterable, Pattern
import re
class URLFilter:
    def __init__(
        self,
        same_domain_only: bool = False,
        include_patterns: Iterable[str] | None = None,
        exclude_patterns: Iterable[str] | None = None,
    ):
        self.same_domain_only = same_domain_only
        self._include: list[Pattern[str]] = [re.compile(p) for p in (include_patterns or [])]
        self._exclude: list[Pattern[str]] = [re.compile(p) for p in (exclude_patterns or [])]

    def allowed(self, url: str, start_host: str | None = None) -> bool:
  
        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return False

        if self.same_domain_only and start_host and parsed.netloc != start_host:
            return False

        if self._include and not any(p.search(url) for p in self._include):
            return False

        if any(p.search(url) for p in self._exclude):
            return False

        return True
    def filter(self, urls: Iterable[str], start_host: str | None = None) -> list[str]:
        return [u for u in urls if self.allowed(u, start_host)]
