from typing import Any, final

from everwork.schemas import WorkerEvent
from .storage import AbstractEventStorage


@final
class EventCollector:

    def __init__(self, storage: AbstractEventStorage) -> None:
        self._storage = storage

    async def add(self, stream: str, data: dict[str, Any]) -> None:
        event = WorkerEvent(stream=stream, data=data)
        await self._storage.write(event)

    async def add_event(self, event: WorkerEvent) -> None:
        await self._storage.write(event)

    async def add_events(self, events: list[WorkerEvent]) -> None:
        await self._storage.write(events)
