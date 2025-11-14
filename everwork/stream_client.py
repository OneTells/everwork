from itertools import chain

from orjson import dumps
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from .schemas import WorkerEvent


class StreamClient:

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._scripts: dict[str, str] = {}

    async def _load_push_event_script(self) -> None:
        self._scripts['push_event'] = await self._redis.script_load(
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

        if 'push_event' not in self._scripts:
            await self._load_push_event_script()

        args = list(chain.from_iterable((event.target_stream, dumps(to_jsonable_python(event.data))) for event in events))

        try:
            await self._redis.evalsha(self._scripts['push_event'], 0, *args)
        except NoScriptError:
            await self._load_push_event_script()
            await self._redis.evalsha(self._scripts['push_event'], 0, *args)
