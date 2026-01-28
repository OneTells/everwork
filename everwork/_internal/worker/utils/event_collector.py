from typing import Any, final

from everwork._internal.utils.event_storage import AbstractStorage
from everwork.schemas import WorkerEvent


@final
class EventCollector:

    def __init__(self, storage: AbstractStorage) -> None:
        self._storage = storage

    def add(self, stream: str, data: dict[str, Any]) -> None:
        event = WorkerEvent(stream=stream, data=data)
        self._storage.write(event)

    def add_event(self, event: WorkerEvent) -> None:
        self._storage.write(event)

    def add_events(self, events: list[WorkerEvent]) -> None:
        self._storage.write(events)
