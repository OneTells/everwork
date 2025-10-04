import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any
from uuid import uuid4

from loguru import logger
from pydantic import BaseModel
from redis.asyncio import Redis

from everwork.utils import ShutdownSafeZone
from everwork.worker_base import BaseWorker


class Resources(BaseModel):
    stream_name: str
    message_id: str


class BaseResourceHandler(ABC):

    def __init__(self, redis: Redis, worker: BaseWorker, shutdown_safe_zone: ShutdownSafeZone) -> None:
        self._redis = redis
        self._worker = worker
        self._shutdown_safe_zone = shutdown_safe_zone

        self._uuid = str(uuid4())

        self.resources: Resources | None = None

        self._streams = (
            {processing_stream: '>' for processing_stream in self._worker.settings.source_streams}
            | {f'worker:{self._worker.settings.name}:stream': '>'}
        )

    def clear(self) -> None:
        self.resources = None

    @abstractmethod
    async def get_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError


class TriggerResourceHandler(BaseResourceHandler):

    async def get_kwargs(self) -> dict[str, Any]:
        logger.debug(f'[{self._worker.settings.name}] ')

        last_time = await self._redis.get(f'worker:{self._worker.settings.name}:last_time')

        start_time = time.time()
        timeout = self._worker.settings.mode.execution_interval - (start_time - float(last_time or 0))

        if int(timeout) > 0:
            with self._shutdown_safe_zone:
                data: dict[str, list[tuple[str, dict[str, Any]]]] | None = await self._redis.xreadgroup(
                    groupname=self._worker.settings.name,
                    consumername=self._uuid,
                    streams=self._streams,
                    count=1,
                    block=int(timeout)
                )

            logger.debug(f'[{self._worker.settings.name}] ')

            if data is not None:
                logger.debug(f'[{self._worker.settings.name}] ')

                stream_name, stream_value = list(data.items())[0]
                message_id, kwargs = stream_value[0]

                self.resources = Resources(stream_name=stream_name, message_id=message_id)

                return kwargs['data']

        logger.debug(f'[{self._worker.settings.name}] ')

        with self._shutdown_safe_zone:
            await asyncio.sleep(timeout - (time.time() - start_time))

        logger.debug(f'[{self._worker.settings.name}] ')

        await self._redis.set(f'worker:{self._worker.settings.name}:last_time', time.time())

        return {}


class ExecutorResourceHandler(BaseResourceHandler):

    async def get_kwargs(self) -> dict[str, Any]:
        logger.debug(f'[{self._worker.settings.name}] ')

        with self._shutdown_safe_zone:
            data: dict[str, list[tuple[str, dict[str, Any]]]] = await self._redis.xreadgroup(
                groupname=self._worker.settings.name,
                consumername=self._uuid,
                streams=self._streams,
                count=1,
                block=0
            )

        logger.debug(f'[{self._worker.settings.name}] ')

        stream_name, stream_value = list(data.items())[0]
        message_id, kwargs = stream_value[0]

        self.resources = Resources(stream_name=stream_name, message_id=message_id)

        return kwargs['data']
