import asyncio
from asyncio import get_running_loop
from threading import Lock

from everwork._internal.utils.event_storage import AbstractReader
from everwork.schemas import Event


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


type ResponseType = tuple[str, Event]
type AnswerType = AbstractReader | BaseException


class ExecutorTransmitter:

    def __init__(
        self,
        response_channel: SingleValueChannel[ResponseType],
        answer_channel: SingleValueChannel[AnswerType]
    ) -> None:
        self._response_channel = response_channel
        self._answer_channel = answer_channel

    async def execute(self, worker_name: str, event: Event) -> AnswerType:
        self._response_channel.send((worker_name, event))
        return await self._answer_channel.receive()

    def close(self) -> None:
        self._response_channel.close()
        self._answer_channel.close()


class ExecutorReceiver:

    def __init__(
        self,
        response_channel: SingleValueChannel[ResponseType],
        answer_channel: SingleValueChannel[AnswerType]
    ) -> None:
        self._response_channel = response_channel
        self._answer_channel = answer_channel

    async def get_response(self) -> ResponseType:
        return await self._response_channel.receive()

    def send_answer(self, answer: AnswerType) -> None:
        self._answer_channel.send(answer)


def create_executor_channel() -> tuple[ExecutorTransmitter, ExecutorReceiver]:
    response_channel = SingleValueChannel[ResponseType]()
    answer_channel = SingleValueChannel[AnswerType]()

    transmitter = ExecutorTransmitter(response_channel, answer_channel)
    receiver = ExecutorReceiver(response_channel, answer_channel)

    return transmitter, receiver
