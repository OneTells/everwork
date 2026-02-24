import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, Self

from loguru import logger

from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel


class _AbstractBackoff(ABC):

    @abstractmethod
    def compute(self, failures: int) -> float:
        raise NotImplementedError


class ExponentialBackoff(_AbstractBackoff):
    _POWERS_OF_TWO = [1 << i for i in range(14)]

    def __init__(self, base: float = 0.5, max_seconds: float = 60.0) -> None:
        self._base = base
        self._max_seconds = max_seconds

        self._max_table_failures = 0

        if base <= 0:
            return

        for i in range(len(self._POWERS_OF_TWO)):
            if base * self._POWERS_OF_TWO[i] >= max_seconds:
                break

            self._max_table_failures = i

    def compute(self, failures: int) -> float:
        if failures <= self._max_table_failures:
            return self._base * self._POWERS_OF_TWO[failures]

        return self._max_seconds


class _AbstractMiddleware[T](ABC):

    @abstractmethod
    async def __call__(self, next_call: Callable[..., Coroutine[Any, Any, T]]) -> T:
        raise NotImplementedError


class _CacheMiddleware[T](_AbstractMiddleware[T]):
    _cache: dict[Any, tuple[T, float]] = {}
    _lock = asyncio.Lock()

    def __init__(self, ttl: float, key: Any) -> None:
        self._ttl = ttl
        self._key = key

    async def __call__(self, next_call: Callable[..., Coroutine[Any, Any, T]]) -> T:
        async with self._lock:
            if self._key in self._cache:
                result, timestamp = self._cache[self._key]

                if (time.monotonic() - timestamp) < self._ttl:
                    return result

                del self._cache[self._key]

        result = await next_call()

        async with self._lock:
            self._cache[self._key] = (result, time.monotonic())

        return result


class _RetryMiddleware[T](_AbstractMiddleware[T]):

    def __init__(self, retries: int | None, backoff: _AbstractBackoff) -> None:
        self._retries = retries
        self._backoff = backoff

    async def __call__(self, next_call: Callable[..., Coroutine[Any, Any, T]]) -> T:
        failures = 0

        while True:
            try:
                return await next_call()
            except Exception as error:
                failures += 1

                logger.warning(f'Не удалось выполнить операцию. Попытка {failures}. Ошибка: {error}')

                if self._retries is not None and failures >= self._retries:
                    raise error

            backoff = self._backoff.compute(failures)
            await asyncio.sleep(backoff)


class _LimitInTimeMiddleware[T](_AbstractMiddleware[T]):

    def __init__(self, event: asyncio.Event, min_timeout: float, max_timeout: float | None) -> None:
        self._event = event
        self._min_timeout = min_timeout
        self._max_timeout = max_timeout

    async def __call__(self, next_call: Callable[..., Coroutine[Any, Any, T]]) -> T:
        return await wait_for_or_cancel(
            next_call(),
            event=self._event,
            min_timeout=self._min_timeout,
            max_timeout=self._max_timeout
        )


class _Caller[T, **P]:

    def __init__(self, function: Callable[P, Coroutine[Any, Any, T]], *args: P.args, **kwargs: P.kwargs) -> None:
        self._function = function
        self._args = args
        self._kwargs = kwargs

        self._use_cache: bool = False
        self._use_wait_for_or_cancel: bool = False
        self._retry: bool = False

        self._data: dict[str, Any] = {}

    def cache(self, *, ttl: float) -> Self:
        self._use_cache = True
        self._data.update(
            {
                'ttl': ttl,
            }
        )
        return self

    def wait_for_or_cancel(self, event: asyncio.Event, *, min_timeout: float = 5, max_timeout: float | None = 10) -> Self:
        self._use_wait_for_or_cancel = True
        self._data.update(
            {
                'event': event,
                'min_timeout': min_timeout,
                'max_timeout': max_timeout
            }
        )
        return self

    def retry(self, *, retries: int | None, backoff: _AbstractBackoff | None = None) -> Self:
        self._retry = True

        if backoff is None:
            backoff = ExponentialBackoff()

        self._data.update(
            {
                'retries': retries,
                'backoff': backoff,
            }
        )
        return self

    async def execute(
        self,
        *,
        on_error_return: Any | Exception,
        on_timeout_return: Any | Exception = asyncio.TimeoutError,
        on_cancel_return: Any | Exception = OperationCancelled,
        log_cancellation: bool = True
    ) -> T | Any:
        app = lambda: self._function(*self._args, **self._kwargs)

        middlewares: list[_AbstractMiddleware] = []

        if self._retry:
            middlewares.append(
                _RetryMiddleware(
                    self._data['retries'],
                    self._data['backoff'],
                )
            )

        if self._use_cache:
            middlewares.append(
                _CacheMiddleware(
                    self._data['ttl'],
                    self._args + tuple(self._kwargs.values())
                )
            )

        if self._use_wait_for_or_cancel:
            middlewares.append(
                _LimitInTimeMiddleware(
                    self._data['event'],
                    self._data['min_timeout'],
                    self._data['max_timeout']
                )
            )

        for middleware in middlewares:
            app = lambda next_call=app: middleware(next_call)

        try:
            return await app()
        except OperationCancelled:
            if log_cancellation:
                logger.exception(f'Отмена по ивенту при выполнении {self._function.__name__}')

            if issubclass(on_cancel_return, Exception):
                raise on_cancel_return

            return on_cancel_return
        except asyncio.TimeoutError:
            logger.exception(f'Прервано по таймауту при выполнении {self._function.__name__}')

            if issubclass(on_timeout_return, Exception):
                raise on_timeout_return

            return on_timeout_return
        except Exception as error:
            logger.opt(exception=True).critical(f'Ошибка при выполнении {self._function.__name__}: {error}')

            if issubclass(on_error_return, Exception):
                raise on_error_return

            return on_error_return


def call[T, **P](function: Callable[P, Coroutine[Any, Any, T]], *args: P.args, **kwargs: P.kwargs) -> _Caller:
    return _Caller(function, *args, **kwargs)
