import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any

from loguru import logger
from orjson import loads, JSONDecodeError
from redis.asyncio import Redis

from everwork.process import Resources
from everwork.utils import SafeCancellationZone
from everwork.worker import BaseWorker


class BaseWorkerWrapper(ABC):

    def __init__(self, redis: Redis, worker: BaseWorker, safe_cancellation_zone: SafeCancellationZone):
        self._redis = redis
        self.safe_cancellation_zone = safe_cancellation_zone

        self.worker = worker
        self.settings = self.worker.settings()

        self._event_id: str | None = None
        self._event_raw: str | None = None

        self._limit_args_raw: str | None = None

    def clear(self):
        self._event_id = None
        self._event_raw = None

        self._limit_args_raw = None

    async def get_resources(self) -> Resources:
        try:
            kwargs = await self._get_kwargs()
        except asyncio.CancelledError:
            return Resources(
                event_id=self._event_id,
                event_raw=self._event_raw,
                limit_args_raw=self._limit_args_raw,
                status='cancel'
            )
        except Exception as error:
            _ = error
            return Resources(
                event_id=self._event_id,
                event_raw=self._event_raw,
                limit_args_raw=self._limit_args_raw,
                status='error'
            )

        return Resources(
            kwargs=kwargs,
            event_id=self._event_id,
            event_raw=self._event_raw,
            limit_args_raw=self._limit_args_raw,
            status='success'
        )

    @abstractmethod
    async def _get_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError


class TriggerWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'worker:{self.settings.name}:last_time')

        with self.safe_cancellation_zone:
            await asyncio.sleep(max(self.settings.mode.timeout - (time.time() - float(last_time or 0)), 0))

        await self._redis.set(f'worker:{self.settings.name}:last_time', time.time())

        return {}


class TriggerWithQueueWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        last_time = await self._redis.get(f'worker:{self.settings.name}:last_time')

        now = time.time()
        timeout = max(self.settings.mode.timeout - (now - float(last_time or 0)), 0)

        if int(timeout) > 0:
            with self.safe_cancellation_zone:
                self._event_raw = await self._redis.blmove(
                    f'worker:{self.settings.name}:events',
                    f'worker:{self.settings.name}:taken_events',
                    timeout=int(timeout)
                )

            if self._event_raw is not None:
                try:
                    event = loads(self._event_raw)
                except JSONDecodeError as error:
                    logger.exception(f'Ошибка при преобразовании события: {error}')
                    raise error

                self._event_id = event['id']

                return event['kwargs']

        with self.safe_cancellation_zone:
            await asyncio.sleep(timeout - (time.time() - now))

        await self._redis.set(f'worker:{self.settings.name}:last_time', time.time())

        return {}


class ExecutorWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        with self.safe_cancellation_zone:
            self._event_raw = await self._redis.blmove(
                f'worker:{self.settings.name}:events',
                f'worker:{self.settings.name}:taken_events',
                timeout=0
            )

        try:
            event = loads(self._event_raw)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании события: {error}')
            raise error

        self._event_id = event['id']

        return event['kwargs']


class ExecutorWithLimitArgsWorkerWrapper(BaseWorkerWrapper):

    async def _get_kwargs(self) -> dict[str, Any]:
        with self.safe_cancellation_zone:
            self._event_raw = await self._redis.blmove(
                f'worker:{self.settings.name}:events',
                f'worker:{self.settings.name}:taken_events',
                timeout=0
            )

        try:
            event = loads(self._event_raw)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании события: {error}')
            raise error

        self._event_id = event['id']

        with self.safe_cancellation_zone:
            self._limit_args_raw = await self._redis.blmove(
                f'worker:{self.settings.name}:limit_args',
                f'worker:{self.settings.name}:taken_limit_args',
                timeout=0
            )

        try:
            limit_args_kwargs = loads(self._limit_args_raw)
        except JSONDecodeError as error:
            logger.exception(f'Ошибка при преобразовании limit_args: {error}')
            raise error

        return event['kwargs'] | limit_args_kwargs
