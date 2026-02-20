import asyncio
import time
from functools import wraps
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar

T = TypeVar('T')
P = ParamSpec('P')

A = Callable[P, Coroutine[Any, Any, T]]


#
# def limit_in_time(
#     *,
#     default: T | Exception | None,
#     min_timeout: int,
#     max_timeout: int | None = None,
#     log_cancellation: bool = True,
# ) -> Callable[[A], A]:
#
#     def decorator(function: A) -> A:
#
#         @wraps(function)
#         async def wrapper(event: asyncio.Event, *args: P.args, **kwargs: P.kwargs) -> T:
#             try:
#                 return await wait_for_or_cancel(
#                     function(*args, **kwargs),
#                     event=event,
#                     min_timeout=min_timeout,
#                     max_timeout=max_timeout
#                 )
#             except asyncio.TimeoutError:
#                 raise
#             except OperationCancelled:
#                 if log_cancellation:
#                     logger.opt(exception=True).warning(f'Было прервано "{function.__name__}"')
#
#                 raise
#             except Exception as error:
#                 logger.opt(exception=True).critical(f'Не удалось выполнить "{function.__name__}": {error}')
#
#                 if isinstance(default, Exception):
#                     raise default
#
#                 return default
#
#         return wrapper
#
#     return decorator


def ttl_cache(function: A) -> A:
    cache: dict[Any, tuple[Any, float]] = {}
    lock = asyncio.Lock()

    @wraps(function)
    async def wrapper(ttl: float | None = None, *args: P.args, **kwargs: P.kwargs) -> T:
        if ttl is None:
            return await function(**kwargs)

        key = args + tuple(kwargs.values())

        async with lock:
            if key in cache:
                result, timestamp = cache[key]

                if (time.monotonic() - timestamp) < ttl:
                    return result

                del cache[key]

        result = await function(**kwargs)

        async with lock:
            cache[key] = (result, time.monotonic())

        return result

    return wrapper

# class WrapperBackend:
#
#     def __init__(self, backend: AbstractBackend, event: asyncio.Event) -> None:
#         self._backend = backend
#         self._event = event
#
#         self._cache = Cache()
#
#     async def get_worker_status(self, worker_id: str, ttl: float | None) -> Literal['on', 'off']:
#         try:
#             return await wait_for_or_cancel(
#                 self._cache(
#                     self._backend.get_worker_status,
#                     ttl=ttl,
#                     worker_id=worker_id
#                 ),
#                 event=self._event,
#                 min_timeout=5,
#                 max_timeout=None
#             )
#         except OperationCancelled:
#             logger.error(f'Было прервано "получение статуса"')
#             raise
#         except Exception as error:
#             logger.opt(exception=True).critical(f'Не удалось выполнить "получение статуса": {error}')
#             return 'off'
