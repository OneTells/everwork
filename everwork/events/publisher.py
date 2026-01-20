from typing import final

from everwork.broker import AbstractBroker
from .storage import AbstractEventStorage


@final
class EventPublisher:

    def __init__(self, storage: AbstractEventStorage, broker: AbstractBroker) -> None:
        self._storage = storage
        self._broker = broker

    async def push_events(self, max_batch_size: int = 1024) -> None:
        batch = []

        async for event in self._storage.read_all():
            batch.append(event)

            if len(batch) >= max_batch_size:
                await self._broker.push_event(batch)
                batch.clear()

        if batch:
            await self._broker.push_event(batch)
            batch.clear()
