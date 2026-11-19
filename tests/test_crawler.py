import asyncio
import time

import aiohttp
import pytest
from aiohttp import web

from models import AsyncCrawler
from utils import (
    AsyncClientError,
    AsyncTimeoutError,
    AsyncClientResponseError,
)


# ---------- локальный сервер ----------

@pytest.fixture
async def test_server(aiohttp_server):
    async def ok(request):
        return web.Response(text="hello")

    async def slow(request):
        delay = float(request.query.get("delay", "1"))
        await asyncio.sleep(delay)
        return web.Response(text=f"slept {delay}")

    async def not_found(request):
        raise web.HTTPNotFound(text="nope")

    async def boom(request):
        raise web.HTTPInternalServerError(text="server is dead")

    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/slow", slow)
    app.router.add_get("/missing", not_found)
    app.router.add_get("/boom", boom)
    return await aiohttp_server(app)


# ---------- 1. Валидные URL ----------

async def test_fetch_url_valid(test_server):
    async with AsyncCrawler() as c:
        url = str(test_server.make_url("/ok"))
        text = await c.fetch_url(url)
    assert text == "hello"


async def test_fetch_urls_parallel_all_ok(test_server):
    urls = [str(test_server.make_url("/ok")) for _ in range(5)]
    async with AsyncCrawler(max_concurrent=5) as c:
        results = await c.fetch_urls(urls)
    assert len(results) == 5
    assert all(v == "hello" for v in results.values())


# ---------- 2. Несуществующие URL / HTTP ошибки ----------

async def test_404_raises_client_response_error(test_server):
    async with AsyncCrawler() as c:
        url = str(test_server.make_url("/missing"))
        with pytest.raises(AsyncClientResponseError) as exc_info:
            await c.fetch_url(url)
    assert exc_info.value.status == 404


async def test_500_raises_client_response_error(test_server):
    async with AsyncCrawler() as c:
        url = str(test_server.make_url("/boom"))
        with pytest.raises(AsyncClientResponseError) as exc_info:
            await c.fetch_url(url)
    assert exc_info.value.status == 500


async def test_unreachable_host_raises_client_error():
    """Несуществующий хост → сетевая ошибка."""
    timeout = aiohttp.ClientTimeout(total=3, connect=1, sock_connect=1, sock_read=1)
    async with AsyncCrawler(timeout=timeout) as c:
        with pytest.raises(AsyncClientError):
            await c.fetch_url("http://nonexistent-domain-xyz-12345.invalid")


async def test_fetch_urls_skips_failures(test_server):
    """fetch_urls не падает, если часть URL сломана, и логирует ошибки (caplog)."""
    good = str(test_server.make_url("/ok"))
    bad = str(test_server.make_url("/missing"))
    urls = [good, bad, good]

    async with AsyncCrawler() as c:
        results = await c.fetch_urls(urls)

    assert len(results) == 2          # только 2 успешных
    assert bad not in results
    assert results[good] == "hello"


# ---------- 3. Таймауты ----------

async def test_timeout_raises(test_server):
    timeout = aiohttp.ClientTimeout(total=1, connect=1, sock_connect=1, sock_read=1)
    async with AsyncCrawler(timeout=timeout) as c:
        url = str(test_server.make_url("/slow?delay=2"))  # сервер спит 2с
        with pytest.raises(AsyncTimeoutError):
            await c.fetch_url(url)


async def test_timeout_in_fetch_urls_is_logged(test_server, caplog):
    caplog.set_level("ERROR")
    timeout = aiohttp.ClientTimeout(total=1, connect=1, sock_connect=1, sock_read=1)
    slow = str(test_server.make_url("/slow?delay=2"))
    fast = str(test_server.make_url("/ok"))

    async with AsyncCrawler(timeout=timeout) as c:
        results = await c.fetch_urls([slow, fast])

    assert fast in results
    assert slow not in results
    assert any("Таймаут" in r.message or "timeout" in r.message.lower()
               for r in caplog.records)


# ---------- 4. Параллельно быстрее, чем последовательно ----------

async def test_parallel_faster_than_sequential(test_server):
    # 3 URL по 0.4с каждый. Последовательно ≈ 1.2с, параллельно ≈ 0.4с.
    urls = [str(test_server.make_url("/slow?delay=0.4")) for _ in range(3)]

    async with AsyncCrawler(max_concurrent=10) as c:
        t0 = time.perf_counter()
        await asyncio.gather(*(c.fetch_url(u) for u in urls))
        parallel_time = time.perf_counter() - t0

    async with AsyncCrawler(max_concurrent=10) as c:
        t0 = time.perf_counter()
        for u in urls:
            await c.fetch_url(u)
        sequential_time = time.perf_counter() - t0

    # Параллельно должно быть минимум в 2 раза быстрее
    assert parallel_time < sequential_time / 2, (
        f"parallel={parallel_time:.2f}s, sequential={sequential_time:.2f}s"
    )


async def test_max_concurrent_respected(test_server):
    """С limit_per_host=N 6 запросов по 0.3с должны идти волнами по N."""
    urls = [str(test_server.make_url("/slow?delay=0.3")) for _ in range(6)]

    async with AsyncCrawler(max_concurrent=2) as c:
        t0 = time.perf_counter()
        await asyncio.gather(*(c.fetch_url(u) for u in urls))
        elapsed = time.perf_counter() - t0

    # 6 задач / лимит 2 = 3 волны * 0.3с ≈ 0.9с (с запасом < 1.6с)
    assert elapsed < 1.6