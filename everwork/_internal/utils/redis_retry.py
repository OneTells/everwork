import asyncio
import copy
from typing import Any, Awaitable, Callable, Self

from loguru import logger
from redis.asyncio.retry import Retry
from redis.backoff import AbstractBackoff
from redis.exceptions import RedisError

from .task_utils import OperationCancelled, wait_for_or_cancel


class RetryShutdownException(BaseException):
    pass


class GracefulShutdownRetry(Retry):

    def __init__(self, backoff: AbstractBackoff, shutdown_event: asyncio.Event) -> None:
        super().__init__(backoff, 0)
        self._shutdown_event = shutdown_event

    def __copy__(self) -> Self:
        return type(self)(self._backoff, self._shutdown_event)

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        new_instance = type(self)(copy.deepcopy(self._backoff, memo), self._shutdown_event)
        memo[id(self)] = new_instance
        return new_instance

    async def call_with_retry[T](self, do: Callable[[], Awaitable[T]], fail: Callable[[RedisError], Any]) -> T:
        self._backoff.reset()
        failures = 0

        while True:
            try:
                return await do()
            except (*self._supported_errors, OSError) as error:
                failures += 1
                await fail(error)

                logger.warning(f'Redis недоступен или не отвечает. Попытка {failures}. Ошибка: {error}')

            if self._shutdown_event.is_set():
                raise RetryShutdownException()

            backoff = self._backoff.compute(failures)

            try:
                await wait_for_or_cancel(asyncio.sleep(backoff), self._shutdown_event)
            except OperationCancelled:
                continue
