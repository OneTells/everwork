from redis.asyncio import Redis


class WorkerStateManager:

    def __init__(self, redis: Redis, worker_name: str) -> None:
        self._redis = redis
        self._worker_name = worker_name

    async def is_enabled(self) -> bool:
        value = await self._redis.get(f'workers:{self._worker_name}:is_worker_on')
        return value == '1'
