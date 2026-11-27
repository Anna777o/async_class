class AsyncClassExeption(Exception):
    """Base class."""
    pass
class AsyncClientError(AsyncClassExeption):
    """Raised when network mistakes happen"""
    pass
class AsyncTimeoutError(AsyncClientError):
    pass
class AsyncClientResponseError(AsyncClassExeption):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status

class TransientError(AsyncClientResponseError):
    """Временная ошибка: таймаут, 429, 5xx, временная недоступность."""
    pass


class NetworkError(AsyncClientError):
    """Сетевая ошибка: connection refused, DNS, reset."""
    pass


class PermanentError(AsyncClientResponseError):
    """Постоянная ошибка: 404, 401, 403, неверный URL."""
    pass


class ParseError(AsyncClassExeption):
    """Ошибка парсинга HTML (не влияет на повтор загрузки)."""
    pass


# ── маппинг HTTP-статусов ──
RETRYABLE_STATUS = {429, 500, 502, 503, 504, 529}
PERMANENT_STATUS = {400, 401, 403, 404, 405, 410}


def classify_http_status(status: int) -> type[AsyncClassExeption] | None:
    """Возвращает класс ошибки для HTTP-статуса или None, если статус OK."""
    if 200 <= status < 300:
        return None
    if status in RETRYABLE_STATUS:
        return TransientError
    if status in PERMANENT_STATUS:
        return PermanentError
    if status >= 500:
        return TransientError       # прочие 5xx — временные
    if 400 <= status < 500:
        return PermanentError       # прочие 4xx — постоянные
    return 

class CircuitOpenError(PermanentError):
    """Цепь открыта — домен временно заблокирован."""
    pass