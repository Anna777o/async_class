# Async Crawler

Асинхронный загрузчик страниц на `aiohttp`: параллельные запросы, ограничение
конкурентности, таймауты, connection pooling, обработка сетевых/HTTP-ошибок,
логирование. Детали реализации — в [ALGORITHM.md](ALGORITHM.md).

## Возможности

- Параллельная загрузка (`asyncio.gather`)
- Лимит соединений (`max_concurrent`)
- Таймауты (`connect`, `sock_read`, `total`)
- Единый пул соединений (`aiohttp.TCPConnector`)
- Ошибки → `AsyncClientError` / `AsyncTimeoutError` / `AsyncClientResponseError`
- Логирование старта, успеха и ошибок с URL и типом

## Установка

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt                 # для тестов
```

## Быстрый старт

```python
import asyncio
from models import AsyncCrawler

async def main():
    urls = ["https://example.com", "https://httpbin.org/delay/1"]
    async with AsyncCrawler(max_concurrent=5) as crawler:
        results = await crawler.fetch_urls(urls)
    print(f"Загружено {len(results)} страниц")

if __name__ == "__main__":
    asyncio.run(main())
```

## API

| Метод | Возвращает | Назначение |
|---|---|---|
| `AsyncCrawler(max_concurrent=10, timeout=None)` | — | Ленивая сессия, переиспользуется |
| `await fetch_url(url)` | `str` | Тело страницы |
| `await fetch_with_status(url)` | `(int, str)` | Тело + статус (для демо/тестов) |
| `await fetch_urls(urls)` | `dict[str, str]` | Параллельно; ошибки логируются, не пробрасываются |
| `await close()` | — | Закрыть сессию (или `async with`) |

## Запуск

```bash
python demo.py     # сравнение параллельной и последовательной загрузки
pytest -v          # тесты на локальном aiohttp-сервере, без сети
```

## Тесты

`pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
testpaths = tests
```

Покрытие: валидные URL, 404/500, несуществующий хост, таймауты,
`fetch_urls` пропускает ошибки, параллельно быстрее последовательного,
лимит `max_concurrent` соблюдается.

