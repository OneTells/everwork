from typing import AsyncIterator, final, Iterator

from everwork.broker import AbstractBroker
from everwork.schemas import WorkerEvent


@final
class EventPublisher:

    def __init__(self, broker: AbstractBroker) -> None:
        self._broker = broker

    async def push_events_from_iterator(self, iterator: Iterator[WorkerEvent], max_batch_size: int = 1024) -> None:
        batch = []

        for event in iterator:
            batch.append(event)

            if len(batch) >= max_batch_size:
                await self._broker.push_event(batch)
                batch.clear()

        if batch:
            await self._broker.push_event(batch)
            batch.clear()

    async def push_events_from_async_iterator(self, iterator: AsyncIterator[WorkerEvent], max_batch_size: int = 1024) -> None:
        batch = []

        async for event in iterator:
            batch.append(event)

            if len(batch) >= max_batch_size:
                await self._broker.push_event(batch)
                batch.clear()

        if batch:
            await self._broker.push_event(batch)
            batch.clear()
