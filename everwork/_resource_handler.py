import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any
from uuid import uuid4

from orjson import loads
from pydantic import BaseModel
from redis.asyncio import Redis

from ._utils import _wait_for_or_cancel
from .schemas import WorkerSettings


class _Resources(BaseModel):
    stream: str
    message_id: str


class _AbstractResourceHandler(ABC):

    def __init__(self, redis: Redis, worker_settings: WorkerSettings, shutdown_event: asyncio.Event) -> None:
        self._redis = redis
        self._worker_settings = worker_settings
        self._shutdown_event = shutdown_event

        self._uuid = str(uuid4())

        self.resources: _Resources | None = None

        self._streams = {processing_stream: '>' for processing_stream in self._worker_settings.source_streams}

    @abstractmethod
    async def get_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError


class _TriggerResourceHandler(_AbstractResourceHandler):

    async def get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'workers:{self._worker_settings.name}:last_time')

        start_time = time.time()
        timeout = self._worker_settings.mode.execution_interval - (start_time - float(last_time or 0))

        if int(timeout) > 0:
            data: dict[str, list[tuple[str, dict[str, Any]]]] = await _wait_for_or_cancel(
                self._redis.xreadgroup(
                    groupname=self._worker_settings.name,
                    consumername=self._uuid,
                    streams=self._streams,
                    count=1,
                    block=int(timeout)
                ),
                self._shutdown_event
            )

            if data:
                stream, stream_value = list(data.items())[0]
                message_id, kwargs = stream_value[0]

                self.resources = _Resources(stream=stream, message_id=message_id)

                return loads(kwargs['data'])

        timeout = max(timeout - (time.time() - start_time), 0)

        await _wait_for_or_cancel(asyncio.sleep(timeout), self._shutdown_event)

        await self._redis.set(f'workers:{self._worker_settings.name}:last_time', time.time())

        return {}


class _ExecutorResourceHandler(_AbstractResourceHandler):

    async def get_kwargs(self) -> dict[str, Any]:
        data: dict[str, list[tuple[str, dict[str, Any]]]] = await _wait_for_or_cancel(
            self._redis.xreadgroup(
                groupname=self._worker_settings.name,
                consumername=self._uuid,
                streams=self._streams,
                count=1,
                block=0
            ),
            self._shutdown_event
        )

        stream, stream_value = list(data.items())[0]
        message_id, kwargs = stream_value[0]

        self.resources = _Resources(stream=stream, message_id=message_id)

        return loads(kwargs['data'])
