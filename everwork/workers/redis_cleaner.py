import time
from abc import ABC, abstractmethod
from typing import Annotated, Any, ClassVar

from loguru import logger
from orjson import loads
from pydantic import BaseModel, ConfigDict, Field, RedisDsn
from redis.asyncio import Redis

from everwork.schemas import AbstractTrigger, IntervalTrigger, WorkerSettings
from everwork.trigger_utils import timer
from .base import AbstractWorker


class RedisCleanerWorkerConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    redis_dns: RedisDsn

    triggers: Annotated[list[AbstractTrigger], Field(max_length=1000)] = [IntervalTrigger(days=1)]
    max_age_seconds: Annotated[float, Field(gt=0)] = timer(weeks=4)


class AbstractRedisCleanerWorker(AbstractWorker, ABC):
    _config: ClassVar[RedisCleanerWorkerConfig]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._config = cls._get_config()
        super().__init_subclass__(**kwargs)

    @staticmethod
    @abstractmethod
    def _get_config() -> RedisCleanerWorkerConfig:
        raise NotImplementedError

    @classmethod
    def _get_settings(cls) -> WorkerSettings:
        return WorkerSettings(
            name="base:redis:cleaner",
            triggers=cls._config.triggers
        )

    async def _trim_streams(self, redis: Redis, streams: set[str]) -> dict[str, int]:
        threshold_id = int(time.time() - self._config.max_age_seconds) * 1000

        async with redis.pipeline() as pipe:
            for stream in streams:
                await pipe.xtrim(stream, minid=threshold_id)
                await pipe.xlen(stream)

            results = await pipe.execute()

        return dict(zip(streams, results[1::2]))

    @staticmethod
    async def _collect_active_streams(redis: Redis) -> set[str]:
        managers: set[str] = await redis.smembers('managers')

        if not managers:
            return set()

        async with redis.pipeline(transaction=False) as pipe:
            for manager_id in managers:
                await pipe.get(f'managers:{manager_id}')

            raw_settings = await pipe.execute()

        active_streams: set[str] = set()

        for raw_data in raw_settings:
            if raw_data is None:
                continue

            data = loads(raw_data)

            for settings_data in data.values():
                worker_settings = WorkerSettings.model_validate(settings_data)
                active_streams.update(worker_settings.source_streams)

        return active_streams

    @staticmethod
    async def _load_remove_streams_script(redis: Redis) -> str:
        return await redis.script_load(
            """
            for _, stream_key in ipairs(KEYS) do
                if redis.call('XLEN', stream_key) == 0 then
                    redis.call('SREM', 'streams', stream_key)
                    redis.call('DEL', stream_key)
                end
            end
            """
        )

    async def _cleanup_streams(self, redis: Redis) -> None:
        all_streams: set[str] = await redis.smembers('streams')

        if not all_streams:
            logger.debug(f'({self.settings.name}) Нет стримов для очистки')
            return

        stream_lengths = await self._trim_streams(redis, all_streams)
        empty_streams = [stream for stream, length in stream_lengths.items() if length == 0]

        if not empty_streams:
            logger.debug(f'({self.settings.name}) Нет пустых стримов для удаления')
            return

        active_streams = await self._collect_active_streams(redis)
        streams_to_delete = set(empty_streams) - active_streams

        if not streams_to_delete:
            logger.debug(f'({self.settings.name}) Нет неактивных пустых стримов для удаления')
            return

        script_sha = await self._load_remove_streams_script(redis)
        await redis.evalsha(script_sha, len(streams_to_delete), *streams_to_delete)

        logger.info(
            f'({self.settings.name}) Удалено {len(streams_to_delete)} неактивных пустых стримов. '
            f'Всего обработано: {len(all_streams)}. '
            f'Удалены: {sorted(streams_to_delete)}'
        )

    async def __call__(self) -> None:
        logger.debug(f'({self.settings.name}) Начато очищение стримов')

        start_time = time.perf_counter()

        async with Redis.from_url(self._config.redis_dns.encoded_string(), protocol=3, decode_responses=True) as redis:
            await self._cleanup_streams(redis)

        logger.info(
            f'({self.settings.name}) Очистка стримов завершена. '
            f'Время выполнения: {time.perf_counter() - start_time:.4f} сек'
        )
