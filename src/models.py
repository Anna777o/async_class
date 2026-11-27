from __future__ import annotations

import socket
import asyncio
import logging
from urllib.parse import urljoin, urlparse
import json
from bs4 import BeautifulSoup
from typing import Any, Dict, Optional
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError, TransientError, NetworkError, PermanentError, CircuitOpenError
import time
import heapq
import aiohttp
import re
import random
from typing import Iterable, Pattern
from urllib.robotparser import RobotFileParser
from collections import defaultdict
from typing import Awaitable, Callable, TypeVar
from dataclasses import dataclass, field
from circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504, 529}
class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, timeout: Optional[aiohttp.ClientTimeout] = None, filter_external_links: bool = False, max_depth: int = 2,
        per_domain_limit: int = 2, requests_per_second: float | None = None, rate_limit_per_domain: bool = True, respect_robots: bool = False,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        user_agent: str = "AsyncCrawler/1.0",
        backoff_base: float = 1.0,
        backoff_max: float = 30.0,
        backoff_factor: float = 2.0, 
        max_retries: int = 2,
        circuit_breaker_enabled: bool = True,
        cb_failure_threshold: int = 5,
        cb_window_seconds: float = 60.0,
        cb_recovery_timeout: float = 30.0,):
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
        self.requests_per_second = requests_per_second
        self.min_delay = max(0.0, min_delay)
        self.jitter = max(0.0, jitter)
        self._request_start_time = time.monotonic()
        self.user_agent = user_agent
        self._min_delay_lock = asyncio.Lock()
        self.backoff_base = max(0.0, backoff_base)
        self.backoff_max = max(
        self.backoff_base,
        backoff_max,
        )
        self.respect_robots = respect_robots
        self.rate_limiter = None
        if requests_per_second is not None:
            self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=rate_limit_per_domain,
            )
        self.robots = RobotsParser(
        user_agent=user_agent,
        )
        self._last_request: dict[str, float] = {}
        self._backoff: dict[str, float] = defaultdict(float)
        self._robots_delay: dict[str, float] = {}
        self._request_stats = {
            "requests": 0,
            "blocked_robots": 0,
            "total_delay": 0.0,
            "last_request": None,
        }
        self.retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            retry_on=[TransientError, NetworkError],
        )
        self.circuit_breaker: CircuitBreaker | None = (
            CircuitBreaker(
                failure_threshold=cb_failure_threshold,
                window_seconds=cb_window_seconds,
                recovery_timeout=cb_recovery_timeout,
            ) if circuit_breaker_enabled else None
        )
        # счётчик «сколько раз домен был заблокирован» — для crawl-статистики
        self._request_stats["circuit_open_skipped"] = 0
        
    async def _fetch_with_retry(self, url: str) -> tuple[int, str]:
        return await self.retry_strategy.execute_with_retry(
            self.fetch_with_status, url,
        )
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
                ttl_dns_cache=300,
                family=socket.AF_INET
            )
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=self.timeout,
                headers={
                "User-Agent": self.user_agent,
                },
            )
            self.robots.session = self._session
        return self._session

    async def _fetch_guarded(self, url: str) -> tuple[int, str]:
        await self._get_session()

        if not await self._check_robots(url):
            raise PermanentError(f"{url}: blocked by robots.txt")
        domain = urlparse(url).netloc
        if self.circuit_breaker is not None:
            if not await self.circuit_breaker.allow_request(domain):
                self._request_stats["circuit_open_skipped"] += 1
                logger.warning("Circuit OPEN — пропускаем %s", url)
                raise CircuitOpenError(f"{url}: circuit open for {domain}")

        await self._wait_before_request(url)

        async with self.sems.limit(url):
            try:
                result = await self._fetch_with_retry(url)   # retry внутри
            except PermanentError:
            # 404/403 — не считаем «поломкой домена»
                raise
            except (TransientError, NetworkError):
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_failure(domain)
                raise
            else:
                if self.circuit_breaker is not None:
                    await self.circuit_breaker.record_success(domain)
                return result
        """
        await self._get_session()
        if not await self._check_robots(url):
            self._request_stats["requests"] += 1 
            raise AsyncClientError(f"{url}: blocked by robots.txt")

        await self._wait_before_request(url)

        async with self.sems.limit(url):
            return await self._fetch_with_retry(url)
        
        async with self.sems.limit(url):
            try:
                result = await self.fetch_with_status(url)
                self._request_stats["requests"] += 1
                domain = urlparse(url).netloc
                self._backoff[domain] = 0.0        # ← единственный сброс
                return result
            except (AsyncClientError, AsyncTimeoutError, AsyncClientResponseError):
                domain = urlparse(url).netloc
                previous = self._backoff.get(domain, 0.0)
            # первая ошибка: base; дальше: ×2, но не больше backoff_max
                self._backoff[domain] = (
                self.backoff_base if previous <= 0
                else min(previous * 2, self.backoff_max)
                )
                raise
"""
    def get_retry_stats(self) -> dict:
        return self.retry_strategy.get_stats()
    
    

    async def fetch_with_status(self, url: str) -> tuple[int, str]:
        session = await self._get_session()
        self._request_stats["requests"] += 1
        logger.info("Начало загрузки: %s", url)
        try:
            async with session.get(url) as response:
                if response.status >= 400:
                    if response.status in RETRYABLE_STATUS:
                        logger.error(
                        "HTTP ошибка для %s: status=%s (retryable)",
                        url, response.status,
                        )
                        raise TransientError(
                        f"{url}: HTTP {response.status}",
                        status=response.status,
                        )
                    logger.error(
                    "HTTP ошибка для %s: status=%s",
                    url, response.status,
                    )
                    raise PermanentError(
                    f"{url}: HTTP {response.status}",
                    status=response.status,
                    )

                text = await response.text()
                logger.info(
                "Успешно загружено: %s (status=%s, len=%d)",
                url, response.status, len(text),
                )
                return response.status, text

        except asyncio.TimeoutError as e:
            logger.error("Таймаут при загрузке %s", url)
            raise AsyncTimeoutError(f"{url}: timeout") from e
        except aiohttp.ClientConnectorError as e:
            logger.error("Сетевая ошибка для %s: %s", url, e)
            raise NetworkError(f"{url}: {e}") from e
        except aiohttp.ClientError as e:
            logger.error("Сетевая ошибка для %s: %s", url, e)
            raise NetworkError(f"{url}: {e}") from e

    async def fetch_url(self, url: str) -> str:
        _, text = await self._fetch_guarded(url)
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

    async def _wait_before_request(self, url: str) -> None:
        domain = urlparse(url).netloc
        total_delay = 0.0

        robots_delay = self._robots_delay.get(domain, 0.0)
        required_delay = max(self.min_delay, robots_delay)

        if self.rate_limiter is not None:
            before = time.monotonic()
            await self.rate_limiter.acquire(domain)
            total_delay += time.monotonic() - before

        backoff = self._backoff.get(domain, 0.0)
        if backoff > 0:
            await asyncio.sleep(backoff)
            total_delay += backoff
        if required_delay > 0:
            async with self._min_delay_lock:
                now = time.monotonic()
                last = self._last_request.get(domain)
                if last is None:
                    wait_for = 0.0
                else:
                    wait_for = max(0.0, required_delay - (now - last))
                self._last_request[domain] = now + wait_for

            if wait_for > 0:
                await asyncio.sleep(wait_for)
                total_delay += wait_for

        if self.jitter > 0:
            jitter_delay = random.uniform(0.0, self.jitter)
            await asyncio.sleep(jitter_delay)
            total_delay += jitter_delay

        self._request_stats["total_delay"] += total_delay

    async def _check_robots(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        await self._get_session()
        await self.robots.fetch_robots(url, rate_limiter=self.rate_limiter)

        if not self.robots.can_fetch(url, self.user_agent):
            self._request_stats["blocked_robots"] += 1
            logger.warning("robots.txt запрещает URL: %s", url)
            return False

        domain = urlparse(url).netloc
        self._robots_delay[domain] = self.robots.get_crawl_delay(
            self.user_agent, base_url=url,
        )
        return True
    
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
        except CircuitOpenError as e:
            result["error"] = "circuit_open"
            result["error_type"] = "circuit_open"
            await queue.mark_failed(url, result["error"])
        except PermanentError as e:
            result["error"] = f"permanent: {e}"
            result["error_type"] = "permanent"
            await queue.mark_failed(url, result["error"])
        except TransientError as e:
            result["error"] = f"transient: {e}"
            result["error_type"] = "transient"
            await queue.mark_failed(url, result["error"])
        except NetworkError as e:
            result["error"] = f"network: {e}"
            result["error_type"] = "network"
            await queue.mark_failed(url, result["error"])
        except AsyncClientResponseError as e:
            result["error"] = f"HTTP {e.status}"
            await queue.mark_failed(url, result["error"])
        except AsyncTimeoutError:
            result["error"] = "timeout"
            await queue.mark_failed(url, result["error"])
        except AsyncClientError as e:
            msg = str(e)
            if "blocked by robots.txt" in msg:
                result["error"] = "blocked_by_robots"
            else:
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

    def get_request_stats(self) -> dict:
        stats = dict(self._request_stats)

        elapsed = max(
        time.monotonic() -
        getattr(self, "_request_start_time", time.monotonic()),
        1e-9,
        )

        requests = stats["requests"]
        stats["requests_per_sec"] = requests / elapsed if elapsed else 0.0
        stats["average_delay"] = elapsed / requests if requests else 0.0
        stats["elapsed_sec"] = elapsed

   
        total_wait = stats.pop("total_delay", 0.0)
        stats["total_wait_sum"] = total_wait
        stats["avg_wait_per_attempt"] = (
            total_wait / requests if requests else 0.0
        )

        stats["blocked_ratio"] = (
            stats["blocked_robots"] / requests if requests else 0.0
        )
        if self.circuit_breaker is not None:
            stats["circuit_breaker"] = self.circuit_breaker.get_stats()
        
        return stats

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
        self._seen: set[str] = set()
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()
        self._in_progress = 0
        self._closed = False
        self._depth_cache: dict[str, int] = {}
        self._start_time = time.perf_counter()

    async def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        async with self._lock:
            if url in self._seen:
                return False

            self._seen.add(url)
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

class RateLimiter:

    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second должен быть > 0")

        self.requests_per_second = requests_per_second
        self.interval = 1.0 / requests_per_second
        self.per_domain = per_domain

        self._lock = asyncio.Lock()
        self._next_allowed: dict[str, float] = {}

    async def acquire(self, domain: str | None = None) -> None:
        key = domain if self.per_domain else "__global__"

        async with self._lock:
            now = time.monotonic()

            next_allowed = self._next_allowed.get(key, now)

            wait_for = max(0.0, next_allowed - now)
            self._next_allowed[key] = (
                max(now, next_allowed) + self.interval
            )

        if wait_for > 0:
            await asyncio.sleep(wait_for)

class RobotsParser:
    def __init__(
        self,
        session: aiohttp.ClientSession | None = None,
        user_agent: str = "*",
    ) -> None:
        self.session = session
        self.user_agent = user_agent
        self._crawl_delays: dict[str, float] = {}
        self._cache: dict[str, RobotFileParser] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._cache_lock = asyncio.Lock()

    def _domain_key(self, base_url: str) -> str:
        parsed = urlparse(base_url)
        return parsed.netloc.lower()

    def _robots_url(self, base_url: str) -> str:
        parsed = urlparse(base_url)

        return (
            f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        )
    
    async def fetch_robots(
        self,
        base_url: str,
        rate_limiter: "RateLimiter | None" = None,
    ) -> dict:
        if not base_url:
            raise ValueError("base_url обязателен")

        domain = self._domain_key(base_url)
        async with self._cache_lock:
            cached = self._cache.get(domain)
            if cached is not None:
                return self._build_result(domain, cached)
            lock = self._locks.setdefault(domain, asyncio.Lock())

        async with lock:
            # double-check: пока ждали лок, кто-то мог уже загрузить
            async with self._cache_lock:
                cached = self._cache.get(domain)
                if cached is not None:
                    return self._build_result(domain, cached)

            session = self.session
            if session is None or session.closed:
                raise RuntimeError(
                    "RobotsParser: aiohttp-сессия не инициализирована. "
                    "Вызовите AsyncCrawler._get_session() или передайте "
                    "session в конструктор."
                )

            robots_url = self._robots_url(base_url)
            logger.debug("Загрузка robots.txt: %s", robots_url)

            if rate_limiter is not None:
                await rate_limiter.acquire(domain)

            parser = RobotFileParser()
            parser.set_url(robots_url)
            delay_value: float = 0.0

            try:
                async with session.get(
                    robots_url,
                    headers={"User-Agent": self.user_agent},
                ) as response:
                    if response.status == 404:
                        logger.debug(
                            "robots.txt %s не найден (404) — разрешаем всё",
                            robots_url,
                        )
                        parser.parse([])
                        delay_value = 0.0

                    elif response.status >= 400:
 
                        logger.warning(
                            "robots.txt %s: HTTP %s — разрешаем всё",
                            robots_url, response.status,
                        )
                        parser.parse([])
                        delay_value = 0.0

                    else:
                        text = await response.text()
                        parser.parse(text.splitlines())
                        delay_value = self._parse_crawl_delay(text, self.user_agent)
                        logger.debug(
                            "robots.txt %s загружен (%d строк)",
                            robots_url, len(text.splitlines()), delay_value, 
                        )
                       # print(f"DEBUG [else]: text={text!r}")
                       # print(f"DEBUG [else]: delay_value={delay_value!r} type={type(delay_value).__name__}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                # Сеть отвалилась / таймаут — не блокируем обход
               # print(f"DEBUG [except]: {type(e).__name__}: {e}")
                logger.warning(
                    "Ошибка загрузки robots.txt %s: %s — разрешаем всё",
                    robots_url, e,
                )
                parser.parse([])
                delay_value = 0.0
            #print(f"DEBUG [cache]: domain={domain} delay_value={delay_value!r}")
            async with self._cache_lock:
                self._cache[domain] = parser
                self._crawl_delays[domain] = delay_value

            return self._build_result(domain, parser)

    def _normalize_ua_token(self, ua: str) -> str:
        ua = (ua or "").strip().lower()
        if not ua:
            return ""
        for sep in ("/", " "):
            if sep in ua:
                ua = ua.split(sep, 1)[0]
        return ua
    def _parse_crawl_delay(self, text: str, user_agent: str) -> float:
        ua_token = self._normalize_ua_token(user_agent)

        current_agents: list[str] = []
        current_delay: float | None = None
        in_rules = False          

        best_delay: float = 0.0
        best_level: int = 0       # 2 = точное совпадение, 1 = wildcard, 0 = нет

        def _flush() -> None:
       
            nonlocal best_delay, best_level
            if not in_rules or current_delay is None:
                return

            if ua_token and ua_token in current_agents:
                level = 2                       # точное совпадение product-token
            elif "*" in current_agents:
                level = 1                       # wildcard
            else:
                return                          # группа не про нас

            if level > best_level or (level == best_level and current_delay > best_delay):
                best_delay = current_delay
                best_level = level

        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue

            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()

            if key == "user-agent":
                if in_rules:
                # Новая группа: сохраняем предыдущую, начинаем с чистого листа
                    _flush()
                    current_agents = []
                    current_delay = None
                    in_rules = False
                current_agents.append(self._normalize_ua_token(value))

            elif key == "crawl-delay":
                in_rules = True
                try:
                    d = float(value)
                except ValueError:
                    logger.debug("Некорректный Crawl-delay: %r", value)
                    continue
            # В одной группе max — безопаснее (более вежливо)
                if current_delay is None or d > current_delay:
                    current_delay = d

            else:
            
                in_rules = True

        _flush()
        return best_delay


    
    def get_crawl_delay(
        self,
        user_agent: str = "*",
        base_url: str | None = None,
        ) -> float:
        if base_url is not None:
            domain = self._domain_key(base_url)
            d = self._crawl_delays.get(domain)
            if d is not None:
                return d
            parser = self._cache.get(domain)
            if parser is None:
                return 0.0
            delay = parser.crawl_delay(user_agent)
            return float(delay) if delay is not None else 0.0

        if self._crawl_delays:
            return max(self._crawl_delays.values())
        return 0.0
    
    def _build_result(self, domain: str, parser: RobotFileParser) -> dict:
        delay = self._crawl_delays.get(domain)        
        if delay is None:                                # только если не нашли
            raw = parser.crawl_delay(self.user_agent)
            delay = float(raw) if raw is not None else 0.0
        #print(f"DEBUG [_build_result]: returning crawl_delay={delay}")
        return {
            "domain": domain,
            "url": parser.url,
            "parser": parser,
            "crawl_delay": delay,
        }

    def can_fetch(
        self,
        url: str,
        user_agent: str = "*",
    ) -> bool:

        domain = self._domain_key(url)
        parser = self._cache.get(domain)

        if parser is None:
            return True

        return parser.can_fetch(user_agent, url)


T = TypeVar("T")

@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int
    backoff_factor: float
    initial_delay: float
    max_delay: float
    jitter: float


DEFAULT_POLICIES: dict[type[BaseException], RetryPolicy] = {
    TransientError: RetryPolicy(
        max_retries=3, backoff_factor=2.0,
        initial_delay=2.0, max_delay=30.0, jitter=0.3,
    ),
    NetworkError: RetryPolicy(
        max_retries=2, backoff_factor=1.5,
        initial_delay=0.5, max_delay=5.0, jitter=0.2,
    ),
}

@dataclass
class RetryStats:
    attempts_total: int = 0
    successes: int = 0
    failures: int = 0
    retries: int = 0
    errors_by_type: dict[str, int] = field(default_factory=dict)
    total_wait: float = 0.0
    permanent_failures: list[str] = field(default_factory=list)

class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        initial_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: float = 0.3,
        retry_on: list[type[BaseException]] | None = None,
        timeout_escalation: float = 1.5,
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.retry_on = tuple(
            retry_on or (TransientError, NetworkError, AsyncTimeoutError)
        )
        self.timeout_escalation = timeout_escalation

        self.stats = RetryStats()
    def get_stats(self) -> dict:
        s = self.stats
        avg_wait = s.total_wait / s.retries if s.retries else 0.0
        return {
            "attempts_total": s.attempts_total,
            "successes": s.successes,
            "failures": s.failures,
            "retries": s.retries,
            "errors_by_type": dict(s.errors_by_type),
            "avg_wait_per_retry": round(avg_wait, 3),
            "permanent_failures": list(s.permanent_failures),
        }
    def _is_retryable(self, exc: BaseException) -> bool:
        return isinstance(exc, self.retry_on)
    
    def _timeout_for_attempt(self, attempt: int, base: aiohttp.ClientTimeout) -> aiohttp.ClientTimeout:
        factor = self.timeout_escalation ** attempt
        return aiohttp.ClientTimeout(
            total=base.total * factor if base.total else None,
            connect=base.connect * factor if base.connect else None,
            sock_read=base.sock_read * factor if base.sock_read else None,
        )
    def _delay_for_attempt(self, attempt: int, exc: BaseException | None = None) -> float:
        policy = DEFAULT_POLICIES.get(type(exc)) if exc else None
        if policy is None:
            return self.initial_delay * (self.backoff_factor ** attempt)
        base = min(
        policy.initial_delay * (policy.backoff_factor ** attempt),
        policy.max_delay,
    )
        return base + random.uniform(0, base * policy.jitter)

    async def execute_with_retry(
        self,
        coro: Callable[..., Awaitable[T]],
        *args,
        **kwargs,
    ) -> T:
        last_exc: BaseException | None = None

        for attempt in range(self.max_retries + 1):
            self.stats.attempts_total += 1
            try:
                result = await coro(*args, **kwargs)
                self.stats.successes += 1
                return result
            except PermanentError as e:
                self.stats.failures += 1
                self.stats.errors_by_type["PermanentError"] = (
                    self.stats.errors_by_type.get("PermanentError", 0) + 1
                )
                self.stats.permanent_failures.append(str(e))
                logger.error(
                    "PermanentError (без повтора), попытка %d: %s",
                    attempt + 1, e,
                )
                raise

            except BaseException as e:
                last_exc = e
                err_name = type(e).__name__
                self.stats.errors_by_type[err_name] = (
                    self.stats.errors_by_type.get(err_name, 0) + 1
                )

                if not self._is_retryable(e):
                    self.stats.failures += 1
                    logger.error(
                        "Неповторяемая ошибка %s, попытка %d: %s",
                        err_name, attempt + 1, e,
                    )
                    raise

                if attempt >= self.max_retries:
                    self.stats.failures += 1
                    logger.error(
                        "Исчерпаны повторы (%d/%d) для %s: %s",
                        attempt + 1, self.max_retries + 1, err_name, e,
                    )
                    raise

                delay = self._delay_for_attempt(attempt)
                self.stats.retries += 1
                self.stats.total_wait += delay
                logger.warning(
                    "Повтор %d/%d через %.2fs после %s: %s",
                    attempt + 1, self.max_retries, delay, err_name, e,
                )
                await asyncio.sleep(delay)
            logger.info(
            "Успех после %d попыток: %s",
            attempt + 1, getattr(coro, "__name__", "coro"),
            )
        # сюда не должны попасть
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("execute_with_retry: неожиданный выход из цикла")

