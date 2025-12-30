import asyncio


class ChannelClosed(Exception):
    pass


class SingleValueChannel[T]:

    def __init__(self) -> None:
        self.__loop: asyncio.AbstractEventLoop | None = None
        self.__pending_data: T | None = None
        self.__waiter: asyncio.Future[bool] | None = None
        self.__is_closed = False

    def __notify_waiter(self) -> None:
        if (future := self.__waiter) is None:
            return

        self.__waiter = None
        if not future.done():
            future.set_result(True)

    def __cancel_waiter(self) -> None:
        future = self.__waiter

        if not future or future.done():
            return

        future.cancel()

    def bind_to_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.__loop = loop

    def send(self, data: T) -> None:
        self.__pending_data = data
        self.__loop.call_soon_threadsafe(self.__notify_waiter)  # type: ignore

    async def receive(self) -> T:
        if self.__is_closed:
            raise ChannelClosed

        if (item := self.__pending_data) is not None:
            self.__pending_data = None
            return item

        future = self.__loop.create_future()
        self.__waiter = future

        try:
            await future
        except asyncio.CancelledError:
            self.__waiter = None
            raise ChannelClosed

        item = self.__pending_data
        self.__pending_data = None
        return item

    def close(self) -> None:
        if self.__is_closed:
            return

        self.__is_closed = True
        self.__loop.call_soon_threadsafe(self.__cancel_waiter)  # type: ignore
