import time
from abc import ABC, abstractmethod
from itertools import chain
from typing import ClassVar, Annotated

from loguru import logger
from orjson import loads
from pydantic import BaseModel, RedisDsn, Field
from redis.asyncio import Redis

from ..base_worker import BaseWorker
from ..schemas import WorkerSettings, TriggerMode
from ..utils import timer


class RetentionWorkerConfig(BaseModel):
    redis_dns: RedisDsn

    execution_interval: Annotated[float, Field(gt=0)] = timer(days=1)
    max_age_seconds: Annotated[float, Field(gt=0)] = timer(weeks=4)


class BaseRetentionWorker(BaseWorker, ABC, init_settings=False):
    _config: ClassVar[RetentionWorkerConfig]

    def __init_subclass__(cls, **kwargs) -> None:
        cls._config = cls._get_config()
        super().__init_subclass__(**kwargs)

    @staticmethod
    @abstractmethod
    def _get_config() -> RetentionWorkerConfig:
        raise NotImplementedError

    @classmethod
    def _get_settings(cls) -> WorkerSettings:
        return WorkerSettings(
            name="base:retention",
            mode=TriggerMode(
                execution_interval=cls._config.execution_interval
            )
        )

    async def __cleanup_streams(self, redis: Redis) -> None:
        streams: set[str] = await redis.smembers('streams')

        if not streams:
            logger.debug(f'({self.settings.name}) Нет стримов для очистки')
            return

        threshold_id = int(time.time() - self._config.max_age_seconds) * 1000

        async with redis.pipeline() as pipe:
            for stream in streams:
                await pipe.xtrim(stream, minid=threshold_id)
                await pipe.xlen(stream)

            results = await pipe.execute()

        stream_lengths = dict(zip(streams, results[1::2]))
        empty_streams = [s for s in streams if stream_lengths[s] == 0]

        if not empty_streams:
            logger.debug(f'({self.settings.name}) Нет пустых стримов для удаления')
            return

        managers: set[str] = await redis.smembers('managers')

        async with redis.pipeline(transaction=False) as pipe:
            for manager_id in managers:
                await pipe.get(f'managers:{manager_id}')

            results = await pipe.execute()

        all_active_streams: set[str] = set(
            chain.from_iterable(
                WorkerSettings.model_validate(x).source_streams
                for data in results if data is not None
                for x in loads(data).values()
            )
        )

        if not (streams_to_delete := (set(empty_streams) - all_active_streams)):
            logger.debug(f'({self.settings.name}) Нет стримов для удаления')
            return

        await redis.srem('streams', *streams_to_delete)
        await redis.delete(*streams_to_delete)

        logger.debug(
            f'({self.settings.name}) Удалены {len(streams_to_delete)} стримов. '
            f'Всего обработано: {len(streams)}. '
            f'Удалены следующие стримы: {streams_to_delete}'
        )

    async def __call__(self) -> None:
        logger.debug(f'({self.settings.name}) Начато очищение стримов')

        start_time = time.perf_counter()

        async with Redis.from_url(self._config.redis_dns.encoded_string(), protocol=3, decode_responses=True) as redis:
            await self.__cleanup_streams(redis)

        logger.debug(
            f'({self.settings.name}) Стримы успешно очищены. '
            f'Время выполнения: {time.perf_counter() - start_time:.6f} секунд'
        )
