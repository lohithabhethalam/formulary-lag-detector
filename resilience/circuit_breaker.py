"""
resilience/circuit_breaker.py

Circuit breaker for external API calls. Tracks consecutive
failures and stops calling a struggling service entirely
for a cooldown period — preventing a failing API from being
hammered with retries that make things worse.

States:
- CLOSED: normal operation, requests pass through
- OPEN: too many failures, requests blocked for cooldown period
- HALF-OPEN: cooldown expired, one test request allowed through
"""

import time
from enum import Enum


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        cooldown_seconds: int = 60,
    ):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED

    def call(self, func, *args, **kwargs):
        if self.state == CircuitState.OPEN:
            elapsed = time.time() - self.last_failure_time
            if elapsed < self.cooldown_seconds:
                raise RuntimeError(
                    f"Circuit is OPEN — service unavailable. "
                    f"Retry in {self.cooldown_seconds - elapsed:.0f}s"
                )
            self.state = CircuitState.HALF_OPEN

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        self.failure_count = 0
        self.state = CircuitState.CLOSED

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN