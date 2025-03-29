from orjson import dumps
from redis.asyncio import Redis

from everwork.worker import Event


async def push_events(redis: Redis, events: list[Event] | None) -> None:
    if not events:
        return None

    pipeline = redis.pipeline()

    for event in events:
        await pipeline.rpush(f'worker:{event.target}:events', dumps(event))

    await pipeline.execute()
