import asyncio
from abc import ABC, abstractmethod
import time


class BaseRateLimiter(ABC):
    """Абстрактный класс для ограничителя запросов"""

    @abstractmethod
    async def acquire(self):
        """Получить разрешение на выполнение запроса"""
        pass


class SimpleRateLimiter(BaseRateLimiter):
    """Простой ограничитель запросов для контроля RPS"""

    def __init__(self, requests_per_second: int):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time

            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                await asyncio.sleep(wait_time)

            self.last_request_time = time.time()
