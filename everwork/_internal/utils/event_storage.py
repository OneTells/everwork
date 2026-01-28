import tempfile
from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator, Self

from orjson import dumps, loads

from everwork.schemas import WorkerEvent


class AbstractReader(ABC):

    @abstractmethod
    def __iter__(self) -> Iterator[WorkerEvent]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class AbstractStorage(ABC):

    @abstractmethod
    def write(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        raise NotImplementedError

    @abstractmethod
    def export(self) -> AbstractReader:
        raise NotImplementedError

    @abstractmethod
    def recreate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class MemoryReader(AbstractReader):

    def __init__(self, events: list[WorkerEvent]) -> None:
        self._events = events

    def __iter__(self) -> Iterator[WorkerEvent]:
        for event in self._events:
            yield event

    def close(self) -> None:
        self._events.clear()


class MemoryStorage(AbstractStorage):

    def __init__(self) -> None:
        self._events: list[WorkerEvent] = []

    def write(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        if isinstance(event, WorkerEvent):
            self._events.append(event)
        else:
            self._events.extend(event)

    def export(self) -> MemoryReader:
        return MemoryReader(self._events)

    def recreate(self) -> None:
        self._events = []

    def close(self) -> None:
        self._events.clear()


class FileReader(AbstractReader):

    def __init__(self, file: tempfile._TemporaryFileWrapper[bytes]) -> None:
        self._file = file

    def __iter__(self) -> Iterator[WorkerEvent]:
        self._file.seek(0)

        for line in self._file:
            payload = line.removesuffix(b"\n")

            if not payload:
                continue

            obj = loads(payload)
            event = WorkerEvent.model_validate(obj)

            yield event

    def close(self) -> None:
        self._file.close()


class FileStorage(AbstractStorage):

    def __init__(self) -> None:
        self._file = tempfile.TemporaryFile(mode="w+b")

    def write(self, event: WorkerEvent | Iterable[WorkerEvent]) -> None:
        events = [event] if isinstance(event, WorkerEvent) else event

        if not events:
            return

        data = b"\n".join(dumps(event.model_dump()) for event in events) + b"\n"

        self._file.write(data)
        self._file.flush()

    def export(self) -> FileReader:
        return FileReader(self._file)

    def recreate(self) -> None:
        self._file = tempfile.TemporaryFile(mode="w+b")

    def close(self) -> None:
        self._file.close()


class HybridStorage(AbstractStorage):

    def __init__(self, max_events_in_memory: int = 1024) -> None:
        self.max_events_in_memory = max_events_in_memory

        self._storage: MemoryStorage | FileStorage = MemoryStorage()
        self._count = 0

    def _switch_to_file_writer(self) -> None:
        file_storage = FileStorage()

        for event in self._storage.export():
            file_storage.write(event)

        self._storage.close()
        self._storage = file_storage

    def write(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        events = [event] if isinstance(event, WorkerEvent) else event

        if not events:
            return

        self._storage.write(events)
        self._count += len(events)

        if self._count >= self.max_events_in_memory and isinstance(self._storage, MemoryStorage):
            self._switch_to_file_writer()

    def export(self) -> AbstractReader:
        return self._storage.export()

    def recreate(self) -> None:
        self._count = 0

        if isinstance(self._storage, FileStorage):
            self._storage.close()
            self._storage = MemoryStorage()
        else:
            self._storage.recreate()

    def close(self) -> None:
        self._storage.close()
