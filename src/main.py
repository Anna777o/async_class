import asyncio
import logging
import time
import json
from models import AsyncCrawler, HTMLParser
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError
import aiohttp
from aiohttp import web

URLS = [
    "https://www.python.org",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/3",
    "https://example.com",
    "https://httpbin.org/status/404",
    "https://httpbin.org/status/200",
    "https://nonexistent-domain-xyz-12345.invalid",
    "https://httpbin.org/html",
    
]


async def probe(crawler: AsyncCrawler, url: str) -> dict:
    try:
        status, text = await crawler.fetch_with_status(url)
        return {"url": url, "status": status, "size": len(text), "error": None}
    except AsyncClientResponseError as e:
        return {"url": url, "status": e.status, "size": 0, "error": "http"}
    except AsyncTimeoutError:
        return {"url": url, "status": None, "size": 0, "error": "timeout"}
    except AsyncClientError:
        return {"url": url, "status": None, "size": 0, "error": "network"}


def print_report(title: str, rows: list[dict], elapsed: float) -> None:
    print(f"\n=== {title} ===")
    print(f"{'URL':<55} {'STATUS':>7} {'BYTES':>8} {'ERROR':>10}")
    print("-" * 85)
    for r in rows:
        status = r["status"] if r["status"] is not None else "-"
        err = r["error"] or ""
        print(f"{r['url']:<55} {str(status):>7} {r['size']:>8} {err:>10}")
    ok = sum(1 for r in rows if r["error"] is None)
    print("-" * 85)
    print(f"Итого: {ok}/{len(rows)} успешно, общее время: {elapsed:.2f} сек")


async def run_parallel(urls: list[str]) -> tuple[list[dict], float]:
    async with AsyncCrawler(max_concurrent=10) as crawler:
        t0 = time.perf_counter()
        rows = await asyncio.gather(*(probe(crawler, u) for u in urls))
        return list(rows), time.perf_counter() - t0


async def run_sequential(urls: list[str]) -> tuple[list[dict], float]:
    async with AsyncCrawler(max_concurrent=10) as crawler:
        t0 = time.perf_counter()
        rows = [await probe(crawler, u) for u in urls]
        return rows, time.perf_counter() - t0



def summarize(parsed: dict) -> dict:

    return {
        "url": parsed["url"],
        "title": parsed["title"],
        "text_length": len(parsed["text"]),
        "links_count": len(parsed["links"]),
        "links": parsed["links"][:5],         
        "images_count": len(parsed["images"]),
        "h1_count": len(parsed["headings"]["h1"]),
        "tables_count": len(parsed["tables"]),
        "error": parsed.get("error"),
    }


async def main2() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async with AsyncCrawler(max_concurrent=5, filter_external_links=False) as crawler:
        results = await crawler.fetch_and_parse_many(URLS)

    print("=" * 70)
    print(f"Обработано URL: {len(results)}")
    ok = sum(1 for r in results.values() if not r.get("error"))
    print(f"Успешно: {ok} / {len(results)}")
    print("=" * 70)

    total_links = total_text = total_images = 0
    for url, parsed in results.items():
        s = summarize(parsed)
        total_links += s["links_count"]
        total_text += s["text_length"]
        total_images += s["images_count"]

        print(f"\n--- {s['url']} ---")
        print(f"  title:       {s['title']!r}")
        print(f"  text_length: {s['text_length']}")
        print(f"  links:       {s['links_count']} (пример: {s['links']})")
        print(f"  images:      {s['images_count']}")
        print(f"  h1:          {s['h1_count']}")
        print(f"  tables:      {s['tables_count']}")
        if s["error"]:
            print(f"  ERROR:       {s['error']}")

    print("\n" + "=" * 70)
    print("СТАТИСТИКА:")
    print(f"  всего ссылок:     {total_links}")
    print(f"  всего изображений:{total_images}")
    print(f"  суммарный текст:  {total_text} символов")

    # пример полного результата для одной страницы 
    first_ok = next((r for r in results.values() if not r.get("error")), None)
    if first_ok:
        print("\n" + "=" * 70)
        print("ПРИМЕР ПОЛНОГО РЕЗУЛЬТАТА (json):")
        slim = {
            "url": first_ok["url"],
            "title": first_ok["title"],
            "text_length": len(first_ok["text"]),
            "links": first_ok["links"][:10],
            "metadata": first_ok["metadata"],
            "images": first_ok["images"][:5],
            "headings": first_ok["headings"],
        }
        print(slim)

async def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,  
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    print(f"URL-ов в наборе: {len(URLS)}")

    par_rows, par_time = await run_parallel(URLS)
    print_report("Параллельно (max_concurrent=10)", par_rows, par_time)

    seq_rows, seq_time = await run_sequential(URLS)
    print_report("Последовательно", seq_rows, seq_time)

    speedup = seq_time / par_time if par_time else float("inf")
    print(f"\n>>> Ускорение параллельной загрузки: x{speedup:.2f}")
    print(f"    последовательно: {seq_time:.2f} сек, параллельно: {par_time:.2f} сек")

async def main3() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async def d0(request):
        return web.Response(
            text="""
            <html>
                <body>
                    <h1>Depth 0</h1>
                    <a href="/d1">Go to depth 1</a>
                </body>
            </html>
            """,
            content_type="text/html",
        )

    async def d1(request):
        return web.Response(
            text="""
            <html>
                <body>
                    <h1>Depth 1</h1>
                    <a href="/d2">Go to depth 2</a>
                </body>
            </html>
            """,
            content_type="text/html",
        )

    async def d2(request):
        return web.Response(
            text="""
            <html>
                <body>
                    <h1>Depth 2</h1>
                    <a href="/d3">Go to depth 3</a>
                </body>
            </html>
            """,
            content_type="text/html",
        )

    async def d3(request):
        return web.Response(
            text="""
            <html>
                <body>
                    <h1>Depth 3</h1>
                    <p>Последний уровень.</p>
                </body>
            </html>
            """,
            content_type="text/html",
        )

    app = web.Application()

    app.router.add_get("/", d0)
    app.router.add_get("/d1", d1)
    app.router.add_get("/d2", d2)
    app.router.add_get("/d3", d3)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()

    # Получаем фактический порт.
    port = site._server.sockets[0].getsockname()[1]
    start = f"http://127.0.0.1:{port}/"

    print(f"\nСтартовый URL: {start}")

    t0 = time.perf_counter()

    try:
        async with AsyncCrawler(
            max_concurrent=10,
            max_depth=3,
            per_domain_limit=2,
        ) as crawler:
            results = await crawler.crawl(
                start_urls=[start],
                max_pages=20,
                same_domain_only=True,
            )

            stats = crawler.queue.get_stats()

            elapsed = time.perf_counter() - t0
            speed = len(results) / elapsed if elapsed else 0

            print("\n" + "=" * 70)
            print(f"Обработано страниц: {len(results)}")
            print(
                f"Время: {elapsed:.2f} сек, "
                f"скорость: {speed:.2f} стр/сек"
            )
            print(f"Статистика очереди: {stats}")
            print(
                f"Активно: total={crawler.sems.active_total}, "
                f"by_domain={crawler.sems.active_by_domain()}"
            )
            print("=" * 70)

            by_depth: dict[int, list[str]] = {}

            for url, result in results.items():
                by_depth.setdefault(result["depth"], []).append(url)

            for depth in sorted(by_depth):
                urls = by_depth[depth]

                print(
                    f"\n--- Глубина {depth}: "
                    f"{len(urls)} стр. ---"
                )

                for url in urls:
                    result = results[url]

                    print(f"  {url}")
                    print(
                        f"    title={result['title']!r} "
                        f"links={len(result['links'])} "
                        f"text_len={len(result['text'])}"
                    )

    finally:
        await runner.cleanup()

async def start_site(port: int = 8766) -> web.AppRunner:
    async def robots(request):
        return web.Response(
            text=(
                "User-agent: *\n"
                "Disallow: /private\n"
                "Crawl-delay: 0.4\n"
            ),
            content_type="text/plain",
        )

    async def index(request):
        links = "".join(
            f'<a href="/page/{i}">page {i}</a> ' for i in range(1, 6)
        )
        links += '<a href="/private/secret">secret</a>'
        return web.Response(
            text=f"<html><head><title>Index</title></head>"
                 f"<body><h1>Index</h1>{links}</body></html>",
            content_type="text/html",
        )

    async def page(request):
        n = int(request.match_info["n"])
        # немного серверной задержки, чтобы rate limiting был виден
        await asyncio.sleep(0.05)
        return web.Response(
            text=f"<html><head><title>Page {n}</title></head>"
                 f"<body><h1>Page {n}</h1></body></html>",
            content_type="text/html",
        )

    async def private(request):
        return web.Response(text="secret")

    app = web.Application()
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/", index)
    app.router.add_get("/page/{n}", page)
    app.router.add_get("/private/{x}", private)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner


# ───────────────────── main ─────────────────────

async def main4() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    PORT = 8766
    runner = await start_site(PORT)
    start = f"http://127.0.0.1:{PORT}/"
    print(f"Локальный сайт: {start}")

    t0 = time.perf_counter()
    try:
        async with AsyncCrawler(
            max_concurrent=5,
            max_depth=2,
            per_domain_limit=2,
            requests_per_second=2.0,       # 2 запроса/сек на домен
            rate_limit_per_domain=True,
            respect_robots=True,           # соблюдаем robots.txt
            min_delay=0.3,
            jitter=0.1,
            user_agent="MyBot/1.0",
        ) as crawler:
            results = await crawler.crawl(
                start_urls=[start],
                max_pages=20,
                same_domain_only=True,
                exclude_patterns=[r"\.(?:jpg|png|gif|svg|zip|pdf)$"],
            )

            queue_stats = crawler.queue.get_stats()
            request_stats = crawler.get_request_stats()
            failed = crawler.queue.failed_urls
    finally:
        await runner.cleanup()

    elapsed = time.perf_counter() - t0
    
    # ─────────── 1. Сводка ───────────
    print("\n" + "=" * 70)
    print(f"Обработано страниц: {len(results)}")
    print(f"Время: {elapsed:.2f} сек")
    print(f"Скорость crawler: {len(results) / elapsed:.2f} стр/сек")
    print(f"Запросов (fetch): {request_stats['requests']}")
    print(f"Текущая скорость: {request_stats['requests_per_sec']:.2f} req/sec")
    print(f"Средняя задержка: {request_stats['average_delay']*1000:.0f} мс")
    print(f"Заблокировано robots.txt: {request_stats['blocked_robots']}")
    print(f"Статистика очереди: {queue_stats}")
    print(f"Активно: total={crawler.sems.active_total}, "
          f"by_domain={crawler.sems.active_by_domain()}")
    print("=" * 70)

    # ─────────── 2. По глубине ───────────
    by_depth: dict[int, list[str]] = {}
    for url, result in results.items():
        by_depth.setdefault(result["depth"], []).append(url)

    for depth in sorted(by_depth):
        print(f"\n--- Глубина {depth}: {len(by_depth[depth])} стр. ---")
        for url in by_depth[depth]:
            r = results[url]
            print(f"  {url}")
            print(f"    title={r['title']!r} "
                  f"links={len(r['links'])} text_len={len(r['text'])}")

    # ─────────── 3. Заблокированные robots.txt ───────────
    print(f"\n--- Заблокировано robots.txt: {len(failed)} ---")
    for url, err in failed.items():
        print(f"  {url}")
        print(f"    reason: {err}")

    # ─────────── 4. Соблюдение robots.txt: контроль ───────────
    # Не едим ли мы /private, хотя он есть на индексе?
    assert not any("/private" in u for u in results), \
        "ОШИБКА: /private попал в результаты, robots.txt не соблюдён"
    assert any("/private" in u for u in failed), \
        "ОШИБКА: /private не заблокирован robots.txt"
    assert request_stats["blocked_robots"] >= 1, \
        f"ОШИБКА: blocked_robots={request_stats['blocked_robots']}"

    # Rate limit: фактическая скорость не должна превышать лимит заметно
    actual_rate = request_stats["requests_per_sec"]
    print(f"\nПроверка rate limiting: "
          f"факт {actual_rate:.2f} req/sec ≤ 2.0 + запас")
    assert actual_rate <= 3.0, \
        f"ОШИБКА: rate limit не соблюдён, {actual_rate:.2f} req/sec"

    # ─────────── 5. Сохранить результаты ───────────
    with open("crawl_results.json", "w", encoding="utf-8") as f:
        json.dump(
            {"results": results, "failed": failed,
             "request_stats": request_stats, "queue_stats": queue_stats},
            f, ensure_ascii=False, indent=2,
        )
    print("\nРезультаты сохранены: crawl_results.json")

async def start_site2(port: int = 8767) -> web.AppRunner:
    state = {"503_count": 0, "429_count": 0}

    async def ok(request):
        return web.Response(text="<html><body>OK</body></html>", content_type="text/html")

    async def not_found(request):
        raise web.HTTPNotFound(text="nope")

    async def service_unavailable(request):
        state["503_count"] += 1
        if state["503_count"] <= 2:          # первые 2 раза — 503
            raise web.HTTPServiceUnavailable(text="try later")
        return web.Response(text="<html><body>Finally OK</body></html>",
                            content_type="text/html")

    async def too_many(request):
        state["429_count"] += 1
        if state["429_count"] <= 1:
            raise web.HTTPTooManyRequests(
                text="slow down", headers={"Retry-After": "1"},
            )
        return web.Response(text="<html><body>OK after 429</body></html>",
                            content_type="text/html")

    async def slow(request):
        await asyncio.sleep(5)                # гарантированный таймаут
        return web.Response(text="never")

    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/404", not_found)
    app.router.add_get("/503", service_unavailable)
    app.router.add_get("/429", too_many)
    app.router.add_get("/slow", slow)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner


async def main5():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    runner = await start_site2()
    base = "http://127.0.0.1:8767"
    urls = [f"{base}/ok", f"{base}/404", f"{base}/503", f"{base}/429", f"{base}/slow"]

    try:
        async with AsyncCrawler(
            max_concurrent=5,
            per_domain_limit=5,
            timeout=aiohttp.ClientTimeout(total=2, connect=1, sock_read=1),
            max_retries=3,
            backoff_factor=2.0,
            respect_robots=False,             # для демо robots не нужен
        ) as crawler:
            t0 = time.perf_counter()
            results = await crawler.fetch_urls(urls)
            elapsed = time.perf_counter() - t0

            retry_stats = crawler.get_retry_stats()
    finally:
        await runner.cleanup()

    print("\n" + "=" * 70)
    print(f"Обработано URL: {len(results)} из {len(urls)}")
    print(f"Время: {elapsed:.2f} сек")
    print(f"Статистика RetryStrategy: {retry_stats}")
    print("=" * 70)
    for url in urls:
        ok = "OK" if url in results else "FAIL"
        print(f"  {ok:4} {url}")

if __name__ == "__main__":
    print("day1")
   # asyncio.run(main())
    print("day2")
  #  asyncio.run(main2())
    print("day3")
   # asyncio.zun(main3())
    print("day4")
    asyncio.run(main4())
    print("day5")
    asyncio.run(main5())
