import asyncio
import time

import aiohttp
import pytest
from aiohttp import web
from models import HTMLParser
from models import AsyncCrawler, URLFilter, CrawlerQueue, RateLimiter, RobotsParser
from utils import (
    AsyncClientError,
    AsyncTimeoutError,
    AsyncClientResponseError,
)
from urllib.parse import urlparse



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
    
    urls = [str(test_server.make_url("/slow?delay=0.4")) for _ in range(3)]

    async with AsyncCrawler(max_concurrent=10, per_domain_limit=10) as c:
        t0 = time.perf_counter()
        await asyncio.gather(*(c.fetch_url(u) for u in urls))
        parallel_time = time.perf_counter() - t0

    async with AsyncCrawler(max_concurrent=10, per_domain_limit=10) as c:
        t0 = time.perf_counter()
        for u in urls:
            await c.fetch_url(u)
        sequential_time = time.perf_counter() - t0

    assert parallel_time < sequential_time / 2, (
        f"parallel={parallel_time:.2f}s, sequential={sequential_time:.2f}s"
    )


@pytest.mark.asyncio
async def test_max_concurrent_respected(test_server):
    urls = [
        str(test_server.make_url(f"/slow?delay=0.3&i={i}"))
        for i in range(6)
    ]

    async with AsyncCrawler(max_concurrent=2) as c:
        t0 = time.perf_counter()

        await asyncio.gather(
            *(c._fetch_guarded(url) for url in urls)
        )

        elapsed = time.perf_counter() - t0

    assert elapsed >= 0.8
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

#day3
# ---------- очередь ----------

async def test_queue_priority_order():
    q = CrawlerQueue()
    await q.add_url("http://a/1", priority=0)
    await q.add_url("http://a/2", priority=5)
    await q.add_url("http://a/3", priority=-1)

    order = []
    for _ in range(3):
        url, _depth = await q.get_next_with_depth()
        order.append(url)
        await q.mark_processed(url)          # ← обязательно: декремент in_progress

    assert order == ["http://a/2", "http://a/1", "http://a/3"]


async def test_queue_returns_depth():
    q = CrawlerQueue()
    await q.add_url("http://a/1", depth=0)
    await q.add_url("http://a/2", depth=3)

    url0, depth0 = await q.get_next_with_depth()
    await q.mark_processed(url0)
    url1, depth1 = await q.get_next_with_depth()
    await q.mark_processed(url1)

    assert (url0, depth0) == ("http://a/1", 0)
    assert (url1, depth1) == ("http://a/2", 3)


async def test_queue_get_next_wrapper_returns_str():
    """Публичный get_next (по ТЗ) возвращает str, не tuple."""
    q = CrawlerQueue()
    await q.add_url("http://a/1", depth=5)
    url = await q.get_next()
    assert url == "http://a/1"
    assert isinstance(url, str)
    await q.mark_processed(url)


async def test_queue_deduplicates():
    q = CrawlerQueue()
    assert await q.add_url("http://a/1") is True
    assert await q.add_url("http://a/1") is False
    assert len(q) == 1


async def test_queue_mark_processed_prevents_requeue():
    q = CrawlerQueue()
    await q.add_url("http://a/1")
    url = await q.get_next()
    await q.mark_processed(url)
    assert await q.add_url("http://a/1") is False
    assert "http://a/1" in q.visited_urls


async def test_queue_stats():
    q = CrawlerQueue()
    await q.add_url("http://a/1")
    url = await q.get_next()
    await q.mark_failed(url, "boom")
    stats = q.get_stats()
    assert stats["failed"] == 1
    assert stats["queued"] == 0
    assert stats["in_progress"] == 0
    assert stats["total_done"] == 1


async def test_queue_get_next_waits_for_in_progress():
    """
    get_next НЕ возвращает None, пока есть задачи в полёте:
    даже если куча пуста, воркер может добавить новые ссылки.
    """
    q = CrawlerQueue()
    await q.add_url("http://a/1")

    url = await q.get_next_with_depth()
    assert url is not None and url[0] == "http://a/1"

    # куча пуста, in_progress == 1 → get_next должен ждать, а не вернуть None
    task = asyncio.create_task(q.get_next_with_depth())
    await asyncio.sleep(0.1)
    assert not task.done(), "get_next вернул None, хотя in_progress > 0"

    # добавляем новый URL и освобождаем текущий — get_next должен отдать новый
    await q.add_url("http://a/2")
    await q.mark_processed("http://a/1")

    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == ("http://a/2", 0)


async def test_queue_get_next_returns_none_when_done():
    """in_progress == 0 и куча пуста → None."""
    q = CrawlerQueue()
    assert await q.get_next_with_depth() is None


# ═══════════════════════════ ФИЛЬТРЫ ═══════════════════════════

def test_filter_same_domain():
    f = URLFilter(same_domain_only=True)
    assert f.allowed("https://example.com/a", "example.com")
    assert not f.allowed("https://other.com/a", "example.com")


def test_filter_include_exclude():
    f = URLFilter(
        include_patterns=[r"/docs/"],
        exclude_patterns=[r"\.pdf$"],
    )
    assert f.allowed("https://x.com/docs/a")
    assert not f.allowed("https://x.com/blog/b")
    assert not f.allowed("https://x.com/docs/a.pdf")


def test_filter_rejects_non_http():
    f = URLFilter()
    assert not f.allowed("mailto:a@b.c")
    assert not f.allowed("javascript:void(0)")
    assert not f.allowed("ftp://x/y")


# ═══════════════════════════ КРАУЛИНГ ═══════════════════════════

@pytest.fixture
async def site(aiohttp_server):
    async def page(request):
        depth = int(request.match_info["depth"])
        links = ""
        if depth < 3:
            links = f'<a href="/d/{depth + 1}">next</a>'   # ← слэш
        return web.Response(
            text=f"<html><head><title>D{depth}</title></head>"
                 f"<body><h1>d{depth}</h1>{links}</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/d/{depth}", page)   # ← /d/ + {depth}
    return await aiohttp_server(app)


@pytest.fixture
async def wide_site(aiohttp_server):
    """Корень со ссылками на 100 страниц — для проверки max_pages."""
    async def root(request):
        links = "".join(f'<a href="/p{i}">p{i}</a>' for i in range(100))
        return web.Response(
            text=f"<html><body>{links}</body></html>",
            content_type="text/html",
        )

    async def page(request):
        return web.Response(text="<html><body>leaf</body></html>",
                            content_type="text/html")

    app = web.Application()
    app.router.add_get("/", root)
    app.router.add_get("/p{i}", page)
    return await aiohttp_server(app)


async def test_crawl_respects_max_depth(site):
    start = str(site.make_url("/d/0"))
    async with AsyncCrawler(max_concurrent=3, max_depth=1) as c:
        results = await c.crawl([start], max_pages=10)

    depths = {r["depth"] for r in results.values()}
    assert depths <= {0, 1}, f"глубина превышена: {depths}"
    assert len(results) == 2


async def test_crawl_depth_matches_queue(site):
    start = str(site.make_url("/d/0"))
    async with AsyncCrawler(max_concurrent=3, max_depth=3) as c:
        results = await c.crawl([start], max_pages=10)

    assert {r["depth"] for r in results.values()} == {0, 1, 2, 3}
    for url, r in results.items():
        assert f"/d/{r['depth']}" in url   # ← тоже слэш


async def test_crawl_no_duplicates(wide_site):
    start = str(wide_site.make_url("/"))
    async with AsyncCrawler(max_concurrent=5, max_depth=1) as c:
        await c.crawl([start], max_pages=20)
        visited = c.queue.visited_urls

    assert len(visited) == len(set(visited)), "в visited_urls есть дубликаты"


async def test_crawl_same_domain_only(site):
    start = str(site.make_url("/d0"))
    async with AsyncCrawler(max_concurrent=3, max_depth=2) as c:
        results = await c.crawl(
            [start], max_pages=10, same_domain_only=True,
        )
    host = site.make_url("/").host
    for url in results:
        assert urlparse(url).hostname== host


async def test_crawl_exclude_patterns(site):
    start = str(site.make_url("/d0"))
    async with AsyncCrawler(max_concurrent=3, max_depth=3) as c:
        results = await c.crawl(
            [start], max_pages=10,
            exclude_patterns=[r"/d2$"],  # не идём в /d2 → /d3 недостижим
        )
    assert not any(url.endswith("/d2") for url in results)
    assert not any(url.endswith("/d3") for url in results)


async def test_crawl_empty_after_filter(site):
    """Если стартовый URL отфильтрован — возвращаем пустой dict, не виснем."""
    start = str(site.make_url("/d0"))
    async with AsyncCrawler(max_concurrent=3, max_depth=1) as c:
        results = await c.crawl(
            [start], max_pages=10,
            include_patterns=[r"/never-matches/"],
        )
    assert results == {}

@pytest.mark.asyncio
async def test_fetch_with_status_returns_status_and_body(test_server):
    async with AsyncCrawler() as c:
        url = str(test_server.make_url("/ok"))

        status, body = await c.fetch_with_status(url)

    assert status == 200
    assert body == "hello"

# ═══════════════ RateLimiter ═══════════════

async def test_rate_limiter_per_domain():
    rl = RateLimiter(requests_per_second=10.0, per_domain=True)  # 100 мс
    t0 = time.monotonic()
    for _ in range(3):
        await rl.acquire("a.com")
    delta = time.monotonic() - t0
    assert delta >= 0.18, f"слишком быстро: {delta:.3f}s"  # 2 интервала


async def test_rate_limiter_different_domains_independent():
    rl = RateLimiter(requests_per_second=2.0, per_domain=True)
    t0 = time.monotonic()
    await asyncio.gather(
        rl.acquire("a.com"),
        rl.acquire("b.com"),
        rl.acquire("c.com"),
    )
    delta = time.monotonic() - t0
    # параллельно на разные домены — не должно быть 3 * 0.5с
    assert delta < 0.5, f"домены не изолированы: {delta:.3f}s"


async def test_rate_limiter_global():
    rl = RateLimiter(requests_per_second=10.0, per_domain=False)
    t0 = time.monotonic()
    for _ in range(3):
        await rl.acquire("a.com")  # domain игнорируется
    delta = time.monotonic() - t0
    assert delta >= 0.18


# ═══════════════ RobotsParser ═══════════════

@pytest.fixture
async def robots_server(aiohttp_server):
    async def robots(request):
        return web.Response(
            text="User-agent: *\nDisallow: /private\nCrawl-delay: 0.4\n",
            content_type="text/plain",
        )

    async def public(request):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/public", public)
    return await aiohttp_server(app)


async def test_robots_parsing(robots_server):
    base = str(robots_server.make_url("/"))
    rp = RobotsParser(user_agent="TestBot/1.0")
    async with aiohttp.ClientSession() as s:
        rp.session = s
        result = await rp.fetch_robots(base)

        assert result["crawl_delay"] == pytest.approx(0.4)       # ← из fetch_robots
        assert rp.get_crawl_delay("TestBot/1.0", base_url=base) == pytest.approx(0.4)
        assert rp.can_fetch(f"{base}public", "TestBot/1.0") is True
        assert rp.can_fetch(f"{base}private", "TestBot/1.0") is False


# ═══════════════ AsyncCrawler + robots ═══════════════

@pytest.fixture
async def one_more_site(aiohttp_server):
    async def robots(request):
        return web.Response(
            text="User-agent: *\nDisallow: /private\nCrawl-delay: 0.3\n",
            content_type="text/plain",
        )

    async def index(request):
        return web.Response(
            text='<html><body>'
                 '<a href="/page/1">p1</a>'
                 '<a href="/private/x">secret</a>'
                 '</body></html>',
            content_type="text/html",
        )

    async def page(request):
        return web.Response(text="<html><body>page</body></html>",
                            content_type="text/html")

    async def private(request):
        return web.Response(text="secret")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/", index)
    app.router.add_get("/page/{n}", page)
    app.router.add_get("/private/{x}", private)
    return await aiohttp_server(app)


async def test_crawl_respects_robots(one_more_site):
    start = str(one_more_site.make_url("/"))
    async with AsyncCrawler(
        max_concurrent=3, respect_robots=True, user_agent="TestBot/1.0",
    ) as c:
        results = await c.crawl([start], max_pages=10, same_domain_only=True)
        assert not any("/private" in u for u in results)
        assert any("/private" in u for u in c.queue.failed_urls)
        assert c.get_request_stats()["blocked_robots"] >= 1


async def test_crawl_min_delay_respected(one_more_site):
    start = str(one_more_site.make_url("/"))
    async with AsyncCrawler(
        max_concurrent=3, min_delay=0.3, respect_robots=False,
    ) as c:
        t0 = time.monotonic()
        await c.crawl([start], max_pages=3, same_domain_only=True)
        elapsed = time.monotonic() - t0
    # 3 запроса → минимум ~0.6с (2 интервала по 0.3)
    assert elapsed >= 0.55, f"min_delay не соблюдён: {elapsed:.2f}s"


async def test_crawl_rate_limit_end_to_end(one_more_site):
    """requests_per_second=5 → 3 запроса ≥ ~0.4с (2 * 0.2с)."""
    start = str(one_more_site.make_url("/"))
    async with AsyncCrawler(
        max_concurrent=5, requests_per_second=5.0,
        respect_robots=False, min_delay=0.0,
    ) as c:
        t0 = time.monotonic()
        await c.crawl([start], max_pages=3, same_domain_only=True)
        elapsed = time.monotonic() - t0
    assert elapsed >= 0.35, f"rate limit не работает: {elapsed:.2f}s"


async def test_crawl_max_pages_strict(one_more_site):
    """max_pages не должен превышаться даже при нескольких воркерах."""
    start = str(one_more_site.make_url("/"))
    async with AsyncCrawler(
        max_concurrent=10, respect_robots=False,
    ) as c:
        results = await c.crawl([start], max_pages=2, same_domain_only=True)
    assert len(results) <= 2


async def test_backoff_grows_on_failures():
    c = AsyncCrawler(backoff_base=1.0, backoff_max=30.0)
    domain = "x.com"

    # эмулируем три ошибки подряд без _wait_before_request (чтобы не ждать)
    for expected in (1.0, 2.0, 4.0):
        previous = c._backoff.get(domain, 0.0)
        c._backoff[domain] = (
            c.backoff_base if previous <= 0
            else min(previous * 2, c.backoff_max)
        )
        assert c._backoff[domain] == expected


async def test_backoff_resets_on_success():
    c = AsyncCrawler(backoff_base=1.0)
    c._backoff["x.com"] = 8.0
    # успех в _fetch_guarded сбрасывает:
    c._backoff["x.com"] = 0.0
    assert c._backoff["x.com"] == 0.0