import asyncio
from typing import Any

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from everwork._internal.resource.resource_handler import (
    AbstractResourceHandler,
    ExecutorResourceHandler,
    Resources,
    TriggerResourceHandler
)
from everwork._internal.utils.redis_retry import RetryShutdownException
from everwork._internal.utils.single_value_channel import SingleValueChannel
from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork.schemas import TriggerMode, WorkerSettings
from everwork.workers.base import AbstractWorker


class WorkerStateManager:

    def __init__(self, redis: Redis, worker: type[AbstractWorker], shutdown_event: asyncio.Event) -> None:
        self._redis = redis
        self._worker_name = worker.settings.name
        self._check_interval = worker.settings.worker_status_check_interval
        self._shutdown_event = shutdown_event

    async def is_enabled(self) -> bool:
        value = await self._redis.get(f'workers:{self._worker_name}:is_worker_on')
        return value == '1'

    async def should_stop(self) -> bool:
        return self._shutdown_event.is_set() or not (await self.is_enabled())

    async def wait_until_enabled(self) -> bool:
        if await self.is_enabled():
            return True

        try:
            await wait_for_or_cancel(
                asyncio.sleep(self._check_interval),
                self._shutdown_event
            )
        except OperationCancelled:
            return False

        return False


class RedisResourceProcessor:

    def __init__(self, redis: Redis, worker: type[AbstractWorker]) -> None:
        self._redis = redis
        self._worker = worker

        self._scripts: dict[str, str] = {}

    async def _load_handle_cancel_script(self) -> None:
        self._scripts['handle_cancel'] = await self._redis.script_load(
            """
            local messages = redis.call('XRANGE', KEYS[1], KEYS[3], KEYS[3], 'COUNT', 1)
            if #messages > 0 then
                redis.call('XACK', KEYS[1], KEYS[2], KEYS[3])
                redis.call('XADD', 'workers:' .. KEYS[2] .. ':stream', '*', unpack(messages[1][2]))
            end
            """
        )

    async def initialize(self) -> None:
        await self._load_handle_cancel_script()

    async def handle_success(self, resources: Resources | None) -> None:
        if resources is None:
            return

        await self._redis.xack(resources.stream, self._worker.settings.name, resources.message_id)

    async def handle_cancel(self, resources: Resources | None) -> None:
        if resources is None:
            return

        keys = [resources.stream, self._worker.settings.name, resources.message_id]

        try:
            await self._redis.evalsha(self._scripts['handle_cancel'], 3, *keys)
        except NoScriptError:
            await self._load_handle_cancel_script()
            await self._redis.evalsha(self._scripts['handle_cancel'], 3, *keys)

    async def handle_error(self, resources: Resources | None) -> None:
        if resources is None:
            return

        logger.warning(
            f'({self._worker.settings.name}) Не удалось обработать сообщение из потока. '
            f'Поток: {resources.stream}. '
            f'ID сообщения: {resources.message_id}'
        )

        await self._redis.xack(resources.stream, self._worker.settings.name, resources.message_id)


def get_resource_handler(
    worker_settings: WorkerSettings,
    redis: Redis,
    shutdown_event: asyncio.Event
) -> AbstractResourceHandler:
    if isinstance(worker_settings.mode, TriggerMode):
        handler_cls = TriggerResourceHandler
    else:
        handler_cls = ExecutorResourceHandler

    return handler_cls(redis, worker_settings, shutdown_event)


class ResourceSupervisor:

    def __init__(
        self,
        redis: Redis,
        worker: type[AbstractWorker],
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool],
        lock: asyncio.Lock,
        shutdown_event: asyncio.Event
    ) -> None:
        self._redis = redis
        self._worker = worker
        self._response_channel = response_channel
        self._answer_channel = answer_channel
        self._lock = lock
        self._shutdown_event = shutdown_event

        self._resource_handler = get_resource_handler(
            self._worker.settings,
            self._redis,
            self._shutdown_event
        )

        self._state_manager = WorkerStateManager(self._redis, self._worker, self._shutdown_event)
        self._resource_processor = RedisResourceProcessor(self._redis, self._worker)

    async def _process_worker_messages(self) -> None:
        while not self._shutdown_event.is_set():
            if not await self._state_manager.wait_until_enabled():
                continue

            try:
                kwargs = await self._resource_handler.get_kwargs()
            except OperationCancelled:
                await self._resource_processor.handle_cancel(self._resource_handler.resources)
                continue

            if await self._state_manager.should_stop():
                await self._resource_processor.handle_cancel(self._resource_handler.resources)
                del kwargs
                continue

            async with self._lock:
                if await self._state_manager.should_stop():
                    await self._resource_processor.handle_cancel(self._resource_handler.resources)
                    del kwargs
                    continue

                self._response_channel.send((self._worker.settings.name, kwargs))
                del kwargs

                if await self._answer_channel.receive():
                    await self._resource_processor.handle_success(self._resource_handler.resources)
                    continue

                await self._resource_processor.handle_error(self._resource_handler.resources)

    async def run(self) -> None:
        logger.debug(f'({self._worker.settings.name}) Запущен наблюдатель ресурсов')

        try:
            await self._resource_processor.initialize()
        except Exception as error:
            logger.critical(f'({self._worker.settings.name}) Ошибка при инициализации обработчика ресурсов: {error}')
            return

        logger.debug(f'({self._worker.settings.name}) Скрипты зарегистрированы')

        try:
            await self._process_worker_messages()
        except RetryShutdownException:
            logger.exception(f'({self._worker.settings.name}) Redis недоступен при мониторинге ресурсов')

            if self._resource_handler.resources is not None:
                logger.warning(
                    f'({self._worker.settings.name}) Не удалось обработать сообщение. '
                    f'Поток: {self._resource_handler.resources.stream}. '
                    f'ID сообщения: {self._resource_handler.resources.message_id}'
                )
        except Exception as error:
            logger.exception(f'({self._worker.settings.name}) Мониторинг ресурсов завершился с ошибкой: {error}')

        logger.debug(f'({self._worker.settings.name}) Наблюдатель ресурсов завершил работ')
