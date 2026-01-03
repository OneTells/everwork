from asyncio import Event
from itertools import chain

from loguru import logger
from orjson import dumps, loads
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.backoff import AbstractBackoff
from redis.exceptions import RedisError

from everwork._internal.utils.redis_retry import GracefulShutdownRetry
from everwork.schemas import Process
from everwork.workers.base import WorkerSettings


class RedisInitializer:

    def __init__(
        self,
        manager_uuid: str,
        processes: list[Process],
        redis_dsn: str,
        redis_backoff_strategy: AbstractBackoff,
        shutdown_event: Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._processes = processes
        self._redis_dsn = redis_dsn
        self._redis_backoff_strategy = redis_backoff_strategy
        self._shutdown_event = shutdown_event

    async def _init_workers(self, redis: Redis) -> None:
        old_data = await redis.get(f'managers:{self._manager_uuid}')

        old_workers: dict[str, WorkerSettings] = (
            {} if old_data is None else {
                k: WorkerSettings.model_validate(v) for k, v in loads(old_data).items()
            }
        )

        workers: dict[str, WorkerSettings] = {
            worker.settings.name: worker.settings
            for process in self._processes
            for worker in process.workers
        }

        async with redis.pipeline() as pipe:
            if old_worker_names := (old_workers.keys() - workers.keys()):
                await pipe.delete(
                    *(f'workers:{worker_name}:is_worker_on' for worker_name in old_worker_names),
                    *(f'workers:{worker_name}:last_time' for worker_name in old_worker_names),
                )

            if new_worker_names := (workers.keys() - old_workers.keys()):
                await pipe.mset({f'workers:{worker_name}:is_worker_on': 0 for worker_name in new_worker_names})

            await pipe.set(f'managers:{self._manager_uuid}', dumps(to_jsonable_python(workers)))
            await pipe.sadd('managers', self._manager_uuid)

            if workers:
                await pipe.sadd(
                    'streams',
                    *chain.from_iterable(settings.source_streams for settings in workers.values())
                )

            await pipe.execute()

    async def _init_stream_groups(self, redis: Redis) -> None:
        stream_groups = {
            (stream, worker.settings.name)
            for process in self._processes
            for worker in process.workers
            for stream in worker.settings.source_streams
        }

        existing_groups: dict[str, set[str]] = {}

        for stream, _ in stream_groups:
            if stream in existing_groups:
                continue

            if not (await redis.exists(stream)):
                continue

            groups = await redis.xinfo_groups(stream)
            existing_groups[stream] = {group['name'] for group in groups}

        async with redis.pipeline() as pipe:
            for stream, group_name in stream_groups:
                if group_name in existing_groups.get(stream, set()):
                    continue

                await pipe.xgroup_create(stream, group_name, mkstream=True)

            await pipe.execute()

    async def initialize(self) -> None:
        retry = GracefulShutdownRetry(self._redis_backoff_strategy, self._shutdown_event)

        try:
            async with Redis.from_url(self._redis_dsn, retry=retry, protocol=3, decode_responses=True) as redis:
                await self._init_workers(redis)
                await self._init_stream_groups(redis)
        except RedisError as error:
            logger.critical(f'Ошибка при работе с redis в наблюдателе процессов: {error}')
            raise
