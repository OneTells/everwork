from typing import final

from schemas.worker import WorkerEvent
from .storage import AbstractEventStorage


@final
class EventCollector:

    def __init__(self, storage: AbstractEventStorage) -> None:
        self._storage = storage

    async def add(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        await self._storage.write(event)
