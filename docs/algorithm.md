# Async Crawler — алгоритм и детали реализации

## 1. Структура проекта

```
.
├── models.py            # AsyncCrawler
├── utils.py             # кастомные исключения
├── main.py              # параллельно vs последовательно
├── tests/
│   └── tests.py
├── requirements.txt
├── docs/
└── └── ALGORITHM.md
```

## 2. Жизненный цикл сессии

Сессия и коннектор создаются **лениво, один раз** и переиспользуются:

```
fetch_* → _get_session()
              │
              ├─ если _session отсутствует или закрыта:
              │     создаём TCPConnector(limit, limit_per_host, ttl_dns_cache)
              │     создаём ClientSession(connector, timeout)
              │
              └─ возвращаем существующую сессию
```



- **Connection pooling** работает только при переиспользовании `ClientSession`.
  Создание сессии на каждый запрос убивает весь смысл `TCPConnector(limit=...)`
  и приводит к утечке сокетов.
- **Ленивая инициализация** позволяет передать `AsyncCrawler` туда, где ещё нет
  запущенного event loop (сессия `aiohttp` создаётся строго внутри корутины).
- `async with AsyncCrawler(...) as c` гарантирует `close()` даже при исключении.

## 3. Таймауты

`aiohttp.ClientTimeout` разбивает общий лимит на этапы:

| Параметр | Что ограничивает |
|---|---|
| `total` | Весь запрос: соединение + чтение |
| `connect` | Получение соединения из пула + TCP/SSL handshake |
| `sock_connect` | Только установка сокета |
| `sock_read` | Ожидание данных между пакетами |

Значения по умолчанию: `total=30, connect=5, sock_connect=3, sock_read=10`.
Переопределяются через `AsyncCrawler(timeout=ClientTimeout(...))` — используется
в тестах на таймауты.

## 4. Обработка ошибок

`raise_for_status()` превращает 4xx/5xx в `aiohttp.ClientResponseError`.
Порядок `except` **критичен**:

```
ClientResponseError   # HTTP 4xx/5xx — самый узкий
    ↓
TimeoutError          # asyncio.TimeoutError
    ↓
ClientError           # сеть — базовый класс, самый широкий
```

Если поставить `ClientError` выше — он «съест» HTTP-ошибки, и снаружи будет
невозможно отличить 404 от разрыва соединения.

Маппинг:

| aiohttp | наш класс | Доп. поля |
|---|---|---|
| `aiohttp.ClientResponseError` | `AsyncClientResponseError` | `.status` |
| `asyncio.TimeoutError` | `AsyncTimeoutError` | — |
| `aiohttp.ClientError` | `AsyncClientError` | — |

Все три наследуются от `AsyncClassException`, ловятся одним `except` при
необходимости.

## 5. Параллельность

`fetch_urls` использует `asyncio.gather(..., return_exceptions=True)`:

```python
tasks = [self.fetch_url(u) for u in urls]
gathered = await asyncio.gather(*tasks, return_exceptions=True)
```

- `return_exceptions=True` — одна упавшая задача **не отменяет** остальные.
- Проверка `isinstance(result, BaseException)` (не `Exception`!) — иначе
  `CancelledError`/`KeyboardInterrupt`/`SystemExit` не отфильтруются и попадут
  в `dict[str, str]` как значение.
- Сломанные URL **логируются**, но отсутствуют в результате. Это осознанный
  выбор: `fetch_urls` — «дай то, что удалось». Если нужны все URL (включая
  ошибки), используем `fetch_with_status` + свой `gather` (см. `demo.py::probe`).

### `fetch_url` vs `fetch_with_status`

Одна реализация — `fetch_with_status` (вся логика, ошибки, логи). `fetch_url` —
тонкая обёртка `_, text = await self.fetch_with_status(url); return text`.
Это соблюдает контракт ТЗ (`-> str`) и не дублирует код.

Сливать в один метод с флагом `return_status: bool` — **антипаттерн**:
тип возврата `str | tuple[int, str]` ломает типизацию и автодополнение.

## 6. Логирование

Уровни:

- `INFO` — старт загрузки и успешное завершение (`status`, длина тела)
- `ERROR` — ошибка с URL и типом (`HTTP 404`, `timeout`, `network`)

Конфигурируется стандартно через `logging.basicConfig(...)` в точке входа.
В `demo.py` уровень `WARNING` (чистый вывод), в проде — `INFO`.

## 7. Демонстрация (`demo.py`)

1. Набор из 10 URL: медленные (`/delay/N`), валидные, 404, несуществующий хост.
2. `probe()` — обёртка над `fetch_with_status`, **никогда** не бросает:
   возвращает `dict(url, status, size, error)`.
3. Два прогона: `run_parallel` (`gather`) и `run_sequential` (цикл `await`).
4. Печать таблицы + расчёт ускорения `seq_time / par_time`.

Ожидаемый результат на httpbin:

```
>>> Ускорение параллельной загрузки: x4.63
    последовательно: 14.85 сек, параллельно: 3.21 сек
```

Цифры зависят от сети — это иллюстрация, а не бенчмарк.

## 8. Тесты

Локальный `aiohttp`-сервер через `aiohttp_server` (`pytest-aiohttp`) — никаких
внешних запросов, тесты быстрые и детерминированные. Три эндпоинта:
`/ok`, `/slow?delay=N`, `/missing` (404), `/boom` (500).

| Сценарий | Тест | Что проверяет |
|---|---|---|
| Валидные URL | `test_fetch_url_valid`, `test_fetch_urls_parallel_all_ok` | `str` возвращается, пул работает |
| HTTP 404/500 | `test_404_...`, `test_500_...` | `AsyncClientResponseError.status` |
| Несуществующий хост | `test_unreachable_host_raises_client_error` | `AsyncClientError` |
| `fetch_urls` терпит сбои | `test_fetch_urls_skips_failures` | результат только успешные |
| Таймаут | `test_timeout_raises` | `AsyncTimeoutError` |
| Таймаут в `fetch_urls` | `test_timeout_in_fetch_urls_is_logged` | есть `ERROR` в `caplog` |
| Параллельно быстрее | `test_parallel_faster_than_sequential` | `parallel < sequential / 2` |
| Лимит соединений | `test_max_concurrent_respected` | волны по `max_concurrent` |

`asyncio_mode = auto` в `pytest.ini` убирает `@pytest.mark.asyncio`.

## 9. Известные ограничения

- `fetch_urls` возвращает только успешные URL (см. §5).
- `fetch_url` бросает на любом 4xx/5xx (`raise_for_status`).
- Нет ретраев и backoff — при необходимости обернуть `fetch_with_status` в
  `tenacity`/ручной цикл.
- Нет rate limiting по времени (`asyncio.Semaphore` + sleep) — сейчас только
  лимит соединений.
- `close()` без `asyncio.sleep`: современные версии `aiohttp` корректно
  закрывают сокеты; «мягкое» закрытие — через `connector.close()`.