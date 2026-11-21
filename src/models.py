import asyncio
import logging
from urllib.parse import urljoin, urlparse
import json
from bs4 import BeautifulSoup
from typing import Any, Dict, Optional
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError

import aiohttp
logger = logging.getLogger(__name__)

class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, timeout: Optional[aiohttp.ClientTimeout] = None, filter_external_links: bool = False,):
        self.max_concurrent = max_concurrent
        self.timeout = timeout or aiohttp.ClientTimeout(
            total=30,       # Общий таймаут на весь запрос (от начала до чтения ответа)
            connect=5,      # Таймаут на получение соединения из пула и установку TCP/SSL
            sock_connect=3, # Таймаут исключительно на установку сокет-соединения
            sock_read=10,   # Таймаут на ожидание данных от сервера между пакетами
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
        tasks = [self.fetch_url(url) for url in urls]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for url, result in zip(urls, gathered):
            if isinstance(result, BaseException):
                logger.error(
                    "Не удалось загрузить %s: %s: %s",
                    url, type(result).__name__, result,
                )
                continue
            results[url] = result
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
            _, html = await self.fetch_with_status(url)
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