from typing import Any

from redis.asyncio import Redis


class WorkerAPI:

    def __init__(self, redis: Redis) -> None:
        self.__redis = redis

    async def create_event(self, stream_name: str, events: list[dict[str, Any]]):
        pass

    async def get_all_stream_names(self) -> list[str]:
        pass

    async def trim_stream_by_timestamp(self, stream_name: str, timestamp: int):
        pass
