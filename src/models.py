import asyncio
import logging

from typing import Any, Dict, Optional
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError

import aiohttp
logger = logging.getLogger(__name__)

class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10, timeout: Optional[aiohttp.ClientTimeout] = None,):
        self.max_concurrent = max_concurrent
        self.timeout = timeout or aiohttp.ClientTimeout(
            total=30,       # Общий таймаут на весь запрос (от начала до чтения ответа)
            connect=5,      # Таймаут на получение соединения из пула и установку TCP/SSL
            sock_connect=3, # Таймаут исключительно на установку сокет-соединения
            sock_read=10,   # Таймаут на ожидание данных от сервера между пакетами
         )
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None

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

