import asyncio
from asyncio import get_running_loop
from threading import Lock


class ChannelClosed(Exception):
    pass


class SingleValueChannel[T]:

    def __init__(self) -> None:
        self._lock = Lock()
        self._is_closed = False
        self._data: T | None = None
        self._waiter: asyncio.Future[bool] | None = None

    async def receive(self) -> T:
        with self._lock:
            if self._is_closed:
                raise ChannelClosed()

            if (item := self._data) is not None:
                self._data = None
                return item

            loop = get_running_loop()
            self._waiter = loop.create_future()

        try:
            await self._waiter
        except asyncio.CancelledError:
            self._waiter = None
            raise ChannelClosed()

        self._waiter = None

        item = self._data
        self._data = None
        return item

    def send(self, data: T) -> None:
        with self._lock:
            self._data = data

            if self._waiter is None:
                return

            loop = self._waiter.get_loop()
            loop.call_soon_threadsafe(lambda: None if self._waiter.done() else self._waiter.set_result(True))  # type: ignore

    def close(self) -> None:
        with self._lock:
            if self._is_closed:
                return

            self._is_closed = True

            if self._waiter is None:
                return

            loop = self._waiter.get_loop()
            loop.call_soon_threadsafe(lambda: None if self._waiter.done() else self._waiter.cancel())  # type: ignore
