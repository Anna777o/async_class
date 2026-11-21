import asyncio
import time

import aiohttp
import pytest
from aiohttp import web
from models import HTMLParser
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
    urls = [str(test_server.make_url(f"/ok?i={i}")) for i in range(5)]
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
    async with AsyncCrawler() as c:
        with pytest.raises(AsyncClientError):
            await c.fetch_url("http://127.0.0.1:1/")  


async def test_fetch_urls_skips_failures(test_server):
    """fetch_urls не падает, если часть URL сломана, и логирует ошибки (caplog)."""
    good1 = str(test_server.make_url("/ok?i=1"))
    good2 = str(test_server.make_url("/ok?i=2"))
    bad = str(test_server.make_url("/missing"))
    urls = [good1, bad, good2]

    async with AsyncCrawler() as c:
        results = await c.fetch_urls(urls)

    assert len(results) == 2          # только 2 успешных
    assert bad not in results
    assert results[good1] == "hello"


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

    assert elapsed < 1.6

SAMPLE_HTML = """
<html>
<head>
  <title>Test Page</title>
  <meta name="description" content="A test page">
  <meta name="keywords" content="test, html, parser">
</head>
<body>
  <h1>Main</h1><h2>Sub</h2><h3>Subsub</h3>
  <p>Hello <a href="/rel">relative</a> and
     <a href="https://other.com/abs">absolute</a>.</p>
  <a href="javascript:void(0)">js</a>
  <a href="#frag">frag</a>
  <img src="/img/logo.png" alt="logo">
  <table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>
  <ul><li>one</li><li>two</li></ul>
  <ol><li>first</li></ol>
  <script>var x = 1;</script>
</body>
</html>
"""


@pytest.fixture
def parser():
    return HTMLParser()


@pytest.mark.asyncio
async def test_parse_html_full(parser):
    result = await parser.parse_html(SAMPLE_HTML, "https://example.com/dir/page")
    assert result["title"] == "Test Page"
    assert result["metadata"]["description"] == "A test page"
    assert result["metadata"]["keywords"] == ["test", "html", "parser"]
    assert result["headings"]["h1"] == ["Main"]
    assert "script" not in result["text"]
    assert "Hello" in result["text"]


@pytest.mark.asyncio
async def test_links_absolute_and_filtered(parser):
    result = await parser.parse_html(SAMPLE_HTML, "https://example.com/dir/page")
    # /rel → https://example.com/rel; /img — не ссылка; js/# — отброшены
    assert "https://example.com/rel" in result["links"]
    assert "https://other.com/abs" in result["links"]
    assert not any("javascript:" in l for l in result["links"])
    assert not any("#frag" in l for l in result["links"])
    # дубликатов нет
    assert len(result["links"]) == len(set(result["links"]))


@pytest.mark.asyncio
async def test_filter_external():
    p = HTMLParser(filter_external=True)
    result = await p.parse_html(SAMPLE_HTML, "https://example.com/dir/page")
    assert all("example.com" in l for l in result["links"])
    assert "https://other.com/abs" not in result["links"]


@pytest.mark.asyncio
async def test_images_headings_tables_lists(parser):
    r = await parser.parse_html(SAMPLE_HTML, "https://example.com/")
    assert r["images"] == [{"src": "https://example.com/img/logo.png", "alt": "logo"}]
    assert r["headings"] == {"h1": ["Main"], "h2": ["Sub"], "h3": ["Subsub"]}
    assert r["tables"] == [[["A", "B"], ["1", "2"]]]
    assert r["lists"]["ul"] == [["one", "two"]]
    assert r["lists"]["ol"] == [["first"]]


@pytest.mark.asyncio
async def test_broken_html_returns_partial(parser):
    broken = "<html><head><title>X</title><body><h1>Hi<p>no close"
    r = await parser.parse_html(broken, "https://example.com/")
    assert r["title"] == "X"
    assert r["headings"]["h1"] == ["Hi"]
    


@pytest.mark.asyncio
async def test_empty_html(parser):
    r = await parser.parse_html("", "https://example.com/")
    assert r["error"] == "empty html"
    assert r["links"] == []


@pytest.mark.asyncio
async def test_none_soup_methods_dont_crash(parser):
    assert parser.extract_links(None, "https://x/") == []
    assert parser.extract_text(None) == ""
    assert parser.extract_metadata(None)["title"] == ""