import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, Self

from loguru import logger

from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel


class _Middleware[T](ABC):

    @abstractmethod
    async def __call__(self, next_call: Callable[..., Coroutine[Any, Any, T]]) -> T:
        raise NotImplementedError


class _CacheMiddleware[T](_Middleware[T]):
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


class _LimitInTimeMiddleware[T](_Middleware[T]):

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

    async def execute(
        self,
        *,
        on_error_return: Any | Exception,
        on_timeout_return: Any | Exception = asyncio.TimeoutError,
        on_cancel_return: Any | Exception = OperationCancelled,
        log_context: str = '',
        log_cancellation: bool = True
    ) -> T | Any:
        app = lambda: self._function(*self._args, **self._kwargs)

        middlewares: list[_Middleware] = []

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
                logger.exception(f'{log_context} | Отменён {self._function.__name__}')

            if isinstance(on_cancel_return, Exception):
                raise on_cancel_return

            return on_cancel_return
        except asyncio.TimeoutError:
            logger.exception(f'{log_context} | Прерван по таймауту {self._function.__name__}')

            if isinstance(on_timeout_return, Exception):
                raise on_timeout_return

            return on_timeout_return
        except Exception as error:
            logger.opt(exception=True).critical(f'{log_context} | Не удалось выполнить {self._function.__name__}: {error}')

            if isinstance(on_error_return, Exception):
                raise on_error_return

            return on_error_return


def call[T, **P](function: Callable[P, Coroutine[Any, Any, T]], *args: P.args, **kwargs: P.kwargs) -> _Caller:
    return _Caller(function, *args, **kwargs)
