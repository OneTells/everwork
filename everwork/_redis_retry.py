import asyncio
from typing import Awaitable, Callable, Any

from loguru import logger
from redis.asyncio.retry import Retry
from redis.backoff import AbstractBackoff
from redis.exceptions import RedisError

from ._utils import _wait_for_or_cancel


class _RetryShutdownException(BaseException):
    pass


class _GracefulShutdownRetry(Retry):

    def __init__(self, backoff: AbstractBackoff, shutdown_event: asyncio.Event) -> None:
        super().__init__(backoff, -1)
        self._shutdown_event = shutdown_event

        logger.debug(f'{4, id(asyncio.get_running_loop()), id(self._shutdown_event)}')

    def __eq__(self, other: Any) -> bool:
        return False

    def __hash__(self) -> int:
        return hash((self._backoff, self._retries, frozenset(self._supported_errors), id(self._shutdown_event)))

    async def call_with_retry[T](self, do: Callable[[], Awaitable[T]], fail: Callable[[RedisError], Any]) -> T:
        logger.debug(f'{5, id(asyncio.get_running_loop()), id(self._shutdown_event)}')

        self._backoff.reset()
        failures = 0

        while True:
            try:
                return await do()
            except (*self._supported_errors, OSError) as error:
                failures += 1
                await fail(error)

                logger.warning(f'Redis не доступен или не отвечает. Попытка повторения {failures}. Ошибка: {error}')

            if self._shutdown_event.is_set():
                raise _RetryShutdownException()

            backoff = self._backoff.compute(failures)

            try:
                await _wait_for_or_cancel(asyncio.sleep(backoff), self._shutdown_event)
            except asyncio.CancelledError:
                continue
