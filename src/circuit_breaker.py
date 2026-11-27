import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from utils import CircuitOpenError
import logging



@dataclass
class _DomainState:
    failures: deque = field(default_factory=lambda: deque(maxlen=100))
    opened_until: float = 0.0
    half_open_in_flight: bool = False
    total_failures: int = 0        
    total_successes: int = 0       
    total_opens: int = 0           


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        window_seconds: float = 60.0,
        recovery_timeout: float = 30.0,
    ):
        self.failure_threshold = failure_threshold
        self.window_seconds = window_seconds
        self.recovery_timeout = recovery_timeout
        self._states: dict[str, _DomainState] = {}
        self._lock = asyncio.Lock()

    def _get(self, domain: str) -> _DomainState:
        return self._states.setdefault(domain, _DomainState())

    async def allow_request(self, domain: str) -> bool:
        async with self._lock:
            st = self._get(domain)
            now = time.monotonic()

            if st.opened_until > now:
                return False            # цепь открыта

            if st.opened_until > 0 and st.opened_until <= now:
                # half-open: пропускаем один запрос
                if st.half_open_in_flight:
                    return False
                st.half_open_in_flight = True
                return True

            return True                 # closed

    async def record_success(self, domain: str) -> None:
        async with self._lock:
            st = self._get(domain)
            st.failures.clear()
            st.opened_until = 0.0
            st.half_open_in_flight = False
            st.total_successes += 1

    async def record_failure(self, domain: str) -> None:
        async with self._lock:
            st = self._get(domain)
            now = time.monotonic()
            st.failures.append(now)
            st.total_failures += 1  

            # чистим старые
            while st.failures and now - st.failures[0] > self.window_seconds:
                st.failures.popleft()

            if len(st.failures) >= self.failure_threshold:
                was_open = st.opened_until > now
                st.opened_until = now + self.recovery_timeout
                st.half_open_in_flight = False
                if not was_open:
                    st.total_opens += 1 
                    logger.warning(
                    "Circuit OPEN для %s на %.0fs (%d ошибок за %.0fs)",
                    domain, self.recovery_timeout,
                    len(st.failures), self.window_seconds,
                    )
    def get_stats(self) -> dict:
    
        now = time.monotonic()
        out: dict = {}

        for domain, st in self._states.items():
            if st.opened_until > now:
                state = "open"
            elif st.half_open_in_flight:
                state = "half_open"
            else:
                state = "closed"

            out[domain] = {
            "state": state,
            "recent_failures": len(st.failures),
            "total_failures": st.total_failures,
            "total_successes": st.total_successes,
            "total_opens": st.total_opens,
            "reopens_in": round(max(0.0, st.opened_until - now), 2),
        }
        return out