class AsyncClassExeption(Exception):
    """Base class."""
    pass
class AsyncClientError(AsyncClassExeption):
    """Raised when network mistakes happen"""
    pass
class AsyncTimeoutError(AsyncClassExeption):
    pass
class AsyncClientResponseError(AsyncClassExeption):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status