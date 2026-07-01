"""
resilience/retry.py

Retry decorator with exponential backoff and jitter.
Jitter prevents multiple retries from hitting the server
at exactly the same moment, which would make rate limiting
worse rather than better.
"""

import random
import time
from functools import wraps


def retry(max_attempts: int = 3, base_delay: float = 1.0, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    delay = base_delay * (2 ** (attempt - 1)) * random.uniform(0.5, 1.0)
                    time.sleep(delay)
        return wrapper
    return decorator