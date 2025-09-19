import asyncio
import hashlib
import time
from abc import ABC, abstractmethod
from typing import Any, Literal, Dict

from orjson import dumps
from pydantic import BaseModel
from redis.asyncio import Redis

from everwork.utils import ShutdownSafeZone
from everwork.worker_base import BaseWorker


class Resources(BaseModel):
    kwargs: Dict[str, Any] | None = None

    stream_name: str
    message_id: str

    args_hash: str | None

    status: Literal['success', 'cancel', 'error']


class BaseWorkerWrapper(ABC):

    def __init__(self, redis: Redis, uuid: str, worker: BaseWorker, shutdown_safe_zone: ShutdownSafeZone) -> None:
        self._redis = redis
        self._uuid = uuid

        self.worker = worker
        self.shutdown_safe_zone = shutdown_safe_zone

        self._current_stream_name: str | None = None
        self._current_message_id: str | None = None
        self._current_args_hash: str | None = None

    async def get_resources(self) -> Resources:
        # noinspection PyBroadException
        try:
            kwargs = await self._get_kwargs()
            resources = Resources(
                kwargs=kwargs,
                stream_name=self._current_stream_name,
                message_id=self._current_message_id,
                args_hash=self._current_args_hash,
                status='success'
            )
        except asyncio.CancelledError:
            resources = Resources(
                stream_name=self._current_stream_name,
                message_id=self._current_message_id,
                args_hash=self._current_args_hash,
                status='cancel'
            )
        except Exception:
            resources = Resources(
                stream_name=self._current_stream_name,
                message_id=self._current_message_id,
                args_hash=self._current_args_hash,
                status='error'
            )

        self._current_stream_name = None
        self._current_message_id = None
        self._current_args_hash = None

        return resources

    @abstractmethod
    async def _get_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError


class TriggerWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'worker:{self.worker.settings.name}:last_time')

        with self.shutdown_safe_zone:
            await asyncio.sleep(self.worker.settings.mode.execution_interval - (time.time() - float(last_time or 0)))

        await self._redis.set(f'worker:{self.worker.settings.name}:last_time', time.time())

        return {}


class TriggerWithStreamsWorkerWrapper(BaseWorkerWrapper):

    def __init__(self, redis: Redis, uuid: str, worker: BaseWorker, shutdown_safe_zone: ShutdownSafeZone) -> None:
        super().__init__(redis, uuid, worker, shutdown_safe_zone)

        self.__streams = (
            {processing_stream: '>' for processing_stream in self.worker.settings.mode.source_streams}
            | {f'{self.worker.settings.name}:worker': '>'}
        )

    async def _get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'worker:{self.worker.settings.name}:last_time')

        start_time = time.time()
        timeout = self.worker.settings.mode.execution_interval - (start_time - float(last_time or 0))

        if timeout > 0:
            with self.shutdown_safe_zone:
                data: dict[str, list[tuple[str, dict[str, Any]]]] | None = await self._redis.xreadgroup(
                    groupname=self.worker.settings.name,
                    consumername=self._uuid,
                    streams=self.__streams,
                    count=1,
                    block=int(timeout)
                )

            if data is not None:
                stream_name, stream_value = list(data.items())[0]
                message_id, kwargs = stream_value[0]

                self._stream_name = stream_name
                self._message_id = message_id

                return kwargs

        with self.shutdown_safe_zone:
            await asyncio.sleep(timeout - (time.time() - start_time))

        await self._redis.set(f'worker:{self.worker.settings.name}:last_time', time.time())

        return {}


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    def __init__(self, redis: Redis, uuid: str, worker: BaseWorker, shutdown_safe_zone: ShutdownSafeZone) -> None:
        super().__init__(redis, uuid, worker, shutdown_safe_zone)

        self.__streams = (
            {processing_stream: '>' for processing_stream in self.worker.settings.mode.source_streams}
            | {f'{self.worker.settings.name}:worker': '>'}
        )

    async def _get_kwargs(self) -> dict[str, Any]:
        with self.shutdown_safe_zone:
            data: dict[str, list[tuple[str, dict[str, Any]]]] = await self._redis.xreadgroup(
                groupname=self.worker.settings.name,
                consumername=self._uuid,
                streams=self.__streams,
                count=1,
                block=0
            )

        stream_name, stream_value = list(data.items())[0]
        message_id, kwargs = stream_value[0]

        self._stream_name = stream_name
        self._message_id = message_id

        return kwargs


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    def __init__(self, redis: Redis, uuid: str, worker: BaseWorker, shutdown_safe_zone: ShutdownSafeZone) -> None:
        super().__init__(redis, uuid, worker, shutdown_safe_zone)

        self.__streams: dict[str, str] = (
            {processing_stream: '>' for processing_stream in self.worker.settings.mode.source_streams}
            | {f'{self.worker.settings.name}:worker': '>'}
        )

        self.__hashed_limited_args: dict[str, dict[str, Any]] = {
            hashlib.sha256(dumps(args)).hexdigest(): args for args in self.worker.settings.mode.limited_args
        }

    async def _get_kwargs(self) -> dict[str, Any]:
        with self.shutdown_safe_zone:
            data: dict[str, list[tuple[str, dict[str, Any]]]] = await self._redis.xreadgroup(
                groupname=self.worker.settings.name,
                consumername=self._uuid,
                streams=self.__streams,
                count=1,
                block=0
            )

        stream_name, stream_value = list(data.items())[0]
        message_id, kwargs = stream_value[0]

        self._stream_name = stream_name
        self._message_id = message_id

        with self.shutdown_safe_zone:
            self._current_args_hash = await self._redis.blmove(
                f'worker:{self.worker.settings.name}:limit_args',
                f'worker:{self.worker.settings.name}:taken_limit_args',
                timeout=0
            )

        return self.__hashed_limited_args[self._current_args_hash] | kwargs
