from orjson import dumps
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from everwork.worker_base import WorkerEvent


class WorkerAPI:

    def __init__(self, redis: Redis) -> None:
        self.__redis = redis

        self.__scripts: dict[str, str] = {}

    async def __register_push_events_script(self) -> None:
        self.__scripts['push_events'] = await self.__redis.script_load(
            """
            for i = 1, #ARGV, 2 do
                local stream_key = ARGV[i]
                local event_data = ARGV[i+1]
                redis.call('XADD', stream_key, '*', 'data', event_data)
            end
            """
        )

    async def push_events(self, events: WorkerEvent | list[WorkerEvent]) -> None:
        if isinstance(events, WorkerEvent):
            events = [events]

        if 'push_events' not in self.__scripts:
            await self.__register_push_events_script()

        args = [item for event in events for item in (event.target_stream, dumps(to_jsonable_python(event.data)))]

        try:
            await self.__redis.evalsha(self.__scripts['push_events'], 0, *args)
        except NoScriptError:
            await self.__register_push_events_script()
            await self.__redis.evalsha(self.__scripts['push_events'], 0, *args)

    async def get_all_stream_names(self) -> list[str]:
        pass

    async def trim_stream_by_timestamp(self, stream_name: str, timestamp: int):
        pass
