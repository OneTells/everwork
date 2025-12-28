from typing import final

from .storage import AbstractEventStorage
from .stream_client import StreamClient


@final
class EventPublisher:

    def __init__(self, stream_client: StreamClient, storage: AbstractEventStorage) -> None:
        self._stream_client = stream_client
        self._storage = storage

    async def publish(self, max_batch_size: int = 1024) -> None:
        batch = []

        async for event in self._storage.read_all():
            batch.append(event)

            if len(batch) >= max_batch_size:
                await self._stream_client.push_event(batch)
                batch.clear()

        if batch:
            await self._stream_client.push_event(batch)
            batch.clear()
