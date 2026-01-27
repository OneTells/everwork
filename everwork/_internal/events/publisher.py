from typing import Iterable

from everwork.broker import AbstractBroker
from everwork.schemas import WorkerEvent


class EventPublisher:

    def __init__(self, broker: AbstractBroker, max_batch_size: int = 1024) -> None:
        self._broker = broker
        self._max_batch_size = max_batch_size

    async def push_events(self, iterator: Iterable[WorkerEvent]) -> None:
        batch = []

        for event in iterator:
            batch.append(event)

            if len(batch) >= self._max_batch_size:
                await self._broker.push_event(batch)
                batch.clear()

        if batch:
            await self._broker.push_event(batch)
            batch.clear()
