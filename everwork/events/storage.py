from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop, get_running_loop, Queue
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryFile
from typing import Any, AsyncIterator, final, Iterable, Self

from orjson import dumps, loads

from schemas import WorkerEvent


class AbstractEventStorage(ABC):

    @abstractmethod
    async def write(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def read_all(self) -> AsyncIterator[WorkerEvent]:
        if False:
            yield

        raise NotImplementedError

    @abstractmethod
    async def clear(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()


@final
class MemoryEventStorage(AbstractEventStorage):

    def __init__(self) -> None:
        self._events: list[WorkerEvent] = []

    async def write(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        if isinstance(event, WorkerEvent):
            self._events.append(event)
        else:
            self._events.extend(event)

    async def read_all(self) -> AsyncIterator[WorkerEvent]:
        for event in self._events:
            yield event

    async def clear(self) -> None:
        self._events.clear()

    async def close(self) -> None:
        await self.clear()


@final
class FileEventStorage(AbstractEventStorage):

    def __init__(self, *, loop: AbstractEventLoop | None = None, queue_maxsize: int = 1024) -> None:
        self._loop = loop or get_running_loop()

        self._file = TemporaryFile(mode="w+b")
        self._thread_pool = ThreadPoolExecutor(max_workers=1)
        self._queue_maxsize = queue_maxsize

    def _write_sync(self, events: Iterable[WorkerEvent]) -> None:
        data = b"\n".join(dumps(event.model_dump()) for event in events) + b"\n"

        self._file.write(data)
        self._file.flush()

    def _reader_sync(self, queue: Queue[WorkerEvent | None]) -> None:
        self._file.seek(0)

        for line in self._file:
            payload = line.removesuffix(b"\n")

            if not payload:
                continue

            obj = loads(payload)
            event = WorkerEvent.model_validate(obj)

            self._loop.call_soon_threadsafe(queue.put, event)

        self._loop.call_soon_threadsafe(queue.put, None)

    def _clear_sync(self) -> None:
        self._file.seek(0)
        self._file.truncate(0)
        self._file.flush()

    def _close_sync(self) -> None:
        self._file.close()

    async def write(self, event: WorkerEvent | Iterable[WorkerEvent]) -> None:
        events = [event] if isinstance(event, WorkerEvent) else event

        if not events:
            return

        await self._loop.run_in_executor(self._thread_pool, self._write_sync, events)

    async def read_all(self) -> AsyncIterator[WorkerEvent]:
        queue: Queue[WorkerEvent | None] = Queue(maxsize=self._queue_maxsize)

        self._loop.run_in_executor(self._thread_pool, self._reader_sync, queue)

        while True:
            event = await queue.get()

            if event is None:
                break

            yield event

    async def clear(self) -> None:
        await self._loop.run_in_executor(self._thread_pool, self._clear_sync)  # type: ignore

    async def close(self) -> None:
        await self._loop.run_in_executor(self._thread_pool, self._close_sync)  # type: ignore
        self._thread_pool.shutdown(wait=False)


@final
class HybridEventStorage(AbstractEventStorage):

    def __init__(self, max_events_in_memory: int = 1024) -> None:
        self.max_events_in_memory = max_events_in_memory

        self._storage: MemoryEventStorage | FileEventStorage = MemoryEventStorage()
        self._count = 0

    async def _switch_to_file_storage(self) -> None:
        file_storage = FileEventStorage(queue_maxsize=self.max_events_in_memory)

        async for event in self._storage.read_all():
            await file_storage.write(event)

        await self._storage.close()
        self._storage = file_storage

    async def write(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        events = [event] if isinstance(event, WorkerEvent) else event

        if not events:
            return

        await self._storage.write(events)
        self._count += len(events)

        if self._count >= self.max_events_in_memory and isinstance(self._storage, MemoryEventStorage):
            await self._switch_to_file_storage()

    async def read_all(self) -> AsyncIterator[WorkerEvent]:
        async for event in self._storage.read_all():
            yield event

    async def clear(self) -> None:
        await self._storage.clear()
        self._count = 0

        if isinstance(self._storage, FileEventStorage):
            await self._storage.close()
            self._storage = MemoryEventStorage()

    async def close(self) -> None:
        await self._storage.close()
