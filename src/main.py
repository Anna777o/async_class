import asyncio
import logging
import time
import json
from models import AsyncCrawler, HTMLParser
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError
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

if __name__ == "__main__":
    #asyncio.run(main())
    #asyncio.run(main2())
    asyncio.run(main3())