import asyncio
from threading import Thread
from typing import Any, Callable, Coroutine

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class AsyncThread:

    def __init__(self, target: Callable[..., Coroutine[Any, Any, None]], kwargs: dict[str, Any]) -> None:
        self._shutdown_event = asyncio.Event()
        self._loop = new_event_loop()

        self._thread = Thread(target=self._run, kwargs={'target': target, 'kwargs': kwargs})

    def start(self) -> None:
        self._thread.start()

    def join(self) -> None:
        self._thread.join()

    def cancel(self) -> None:
        self._loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

    def _run(self, target: Callable[..., Coroutine[Any, Any, None]], kwargs: dict[str, Any]) -> None:
        with asyncio.Runner(loop_factory=lambda: self._loop) as runner:
            runner.run(target(**kwargs, shutdown_event=self._shutdown_event))
