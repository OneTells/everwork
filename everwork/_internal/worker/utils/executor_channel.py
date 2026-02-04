import asyncio
from asyncio import get_running_loop
from threading import Lock

from everwork._internal.schemas import Request, Response


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


type RequestType = tuple[str, Request]
type ResponseType = Response


class ExecutorTransmitter:

    def __init__(
        self,
        request_channel: SingleValueChannel[RequestType],
        response_channel: SingleValueChannel[ResponseType]
    ) -> None:
        self._request_channel = request_channel
        self._response_channel = response_channel

    async def execute(self, worker_slug: str, request: Request) -> ResponseType:
        self._request_channel.send((worker_slug, request))
        return await self._response_channel.receive()

    def close(self) -> None:
        self._request_channel.close()
        self._response_channel.close()


class ExecutorReceiver:

    def __init__(
        self,
        request_channel: SingleValueChannel[RequestType],
        response_channel: SingleValueChannel[ResponseType]
    ) -> None:
        self._request_channel = request_channel
        self._response_channel = response_channel

    async def get_request(self) -> RequestType:
        return await self._request_channel.receive()

    def send_response(self, response: Response) -> None:
        self._response_channel.send(response)


def create_executor_channel() -> tuple[ExecutorTransmitter, ExecutorReceiver]:
    request_channel = SingleValueChannel[RequestType]()
    response_channel = SingleValueChannel[ResponseType]()

    transmitter = ExecutorTransmitter(request_channel, response_channel)
    receiver = ExecutorReceiver(request_channel, response_channel)

    return transmitter, receiver
