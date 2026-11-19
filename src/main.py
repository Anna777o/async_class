import asyncio
import logging
import time

from models import AsyncCrawler
from utils import AsyncClientError, AsyncTimeoutError, AsyncClientResponseError


URLS = [
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/3",
    "https://example.com",
    "https://httpbin.org/status/404",
    "https://httpbin.org/status/200",
    "https://nonexistent-domain-xyz-12345.invalid",
    "https://httpbin.org/delay/1",
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


if __name__ == "__main__":
    asyncio.run(main())