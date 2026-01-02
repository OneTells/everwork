from itertools import chain
from typing import AsyncIterator, final, Iterator

from orjson import dumps
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from everwork.schemas import WorkerEvent


@final
class EventPublisher:

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._script: str | None = None

    async def _load_push_event_script(self) -> None:
        self._script = await self._redis.script_load(
            """
            for i = 1, #ARGV, 2 do
                local stream_key = ARGV[i]
                local event_data = ARGV[i+1]
                redis.call('XADD', stream_key, '*', 'data', event_data)
            end
            """
        )

    async def push_event(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        events = [event] if isinstance(event, WorkerEvent) else event

        if not events:
            return

        if self._script is None:
            await self._load_push_event_script()

        args = list(chain.from_iterable((event.stream, dumps(to_jsonable_python(event.data))) for event in events))

        try:
            await self._redis.evalsha(self._script, 0, *args)
        except NoScriptError:
            await self._load_push_event_script()
            await self._redis.evalsha(self._script, 0, *args)

    async def push_events_from_iterator(self, iterator: Iterator[WorkerEvent], max_batch_size: int = 1024) -> None:
        batch = []

        for event in iterator:
            batch.append(event)

            if len(batch) >= max_batch_size:
                await self.push_event(batch)
                batch.clear()

        if batch:
            await self.push_event(batch)
            batch.clear()

    async def push_events_from_async_iterator(self, iterator: AsyncIterator[WorkerEvent], max_batch_size: int = 1024) -> None:
        batch = []

        async for event in iterator:
            batch.append(event)

            if len(batch) >= max_batch_size:
                await self.push_event(batch)
                batch.clear()

        if batch:
            await self.push_event(batch)
            batch.clear()
