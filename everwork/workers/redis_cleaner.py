import time
from abc import ABC, abstractmethod
from typing import Annotated, Any, ClassVar, Sequence

from loguru import logger
from pydantic import BaseModel, Field, RedisDsn
from redis.asyncio import Redis

from everwork.schemas.trigger import Cron, Trigger
from everwork.schemas.worker import WorkerSettings
from everwork.utils.time import to_seconds
from everwork.workers.base import AbstractWorker


class RedisCleanerWorkerConfig(BaseModel):
    broker_redis_dns: RedisDsn

    triggers: Sequence[Trigger] = [
        Trigger(
            title='Каждый день в полночь',
            schedule=Cron(expression='0 0 * * *')
        )
    ]
    max_age_seconds: Annotated[float, Field(gt=0)] = to_seconds(weeks=4)


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
            title="base:redis_cleaner",
            triggers=cls._config.triggers
        )

    async def __call__(self) -> None:
        async with Redis.from_url(self._config.broker_redis_dns.encoded_string(), protocol=3, decode_responses=True) as redis:
            stream_keys = []

            async for key in redis.scan_iter(match='*', count=100):
                key_type = await redis.type(key)

                if key_type == 'stream':
                    stream_keys.append(key)

            if not stream_keys:
                return

            min_id = int(time.time() - self._config.max_age_seconds) * 1000

            async with redis.pipeline() as pipe:
                for key in stream_keys:
                    await pipe.xtrim(key, minid=min_id)

                await pipe.execute(raise_on_error=False)

        logger.debug(f'Стримы были очищены. Всего обработано: {len(stream_keys)}')
