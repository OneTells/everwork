import asyncio
from typing import Any

from loguru import logger
from redis.asyncio import Redis

from everwork._internal.redis_utils.redis_resource_processor import RedisResourceProcessor
from everwork._internal.redis_utils.worker_state_manager import WorkerStateManager
from everwork._internal.resource.resource_handler import AbstractResourceHandler, ExecutorResourceHandler, TriggerResourceHandler
from everwork._internal.utils.redis_retry import RetryShutdownException
from everwork._internal.utils.single_value_channel import SingleValueChannel
from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork.schemas import TriggerMode, WorkerSettings
from everwork.workers.base import AbstractWorker


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

        self._state_manager = WorkerStateManager(self._redis, self._worker.settings.name)
        self._resource_processor = RedisResourceProcessor(self._redis, self._worker)

    async def _process_worker_messages(self) -> None:
        while not self._shutdown_event.is_set():
            if not (await self._state_manager.is_enabled()):
                try:
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker.settings.worker_status_check_interval),
                        self._shutdown_event
                    )
                except OperationCancelled:
                    break

                continue

            try:
                kwargs = await self._resource_handler.get_kwargs()
            except OperationCancelled:
                await self._resource_processor.handle_cancel(self._resource_handler.resources)
                continue

            if self._shutdown_event.is_set() or not (await self._state_manager.is_enabled()):
                await self._resource_processor.handle_cancel(self._resource_handler.resources)
                del kwargs
                continue

            async with self._lock:
                if self._shutdown_event.is_set() or not (await self._state_manager.is_enabled()):
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
