import asyncio
from typing import Callable

from loguru import logger

from everwork._internal.resource.resource_handler import ResourceHandler
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.worker.utils.executor_channel import ExecutorTransmitter
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process


class ResourceManager:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        transmitter: ExecutorTransmitter,
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._transmitter = transmitter
        self._shutdown_event = shutdown_event

    async def _mark_worker_executor_as_available(self, backend: AbstractBackend) -> None:
        try:
            await wait_for_or_cancel(
                backend.mark_worker_executor_as_available(self._manager_uuid, self._process.uuid),
                self._shutdown_event,
                max_timeout=5
            )
        except (OperationCancelled, asyncio.TimeoutError):
            logger.debug(f'[{self._process.uuid}] Менеджер ресурсов прервал mark_worker_executor_as_available')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] Не удалось установить метку доступности исполнителя: {error}'
            )

    async def _run_handlers(self) -> None:
        lock = asyncio.Lock()

        try:
            async with self._backend_factory() as backend, self._broker_factory() as broker:
                logger.debug(f'[{self._process.uuid}] Менеджеру ресурсов инициализировал backend / broker')

                await self._mark_worker_executor_as_available(backend)
                logger.debug(f'[{self._process.uuid}] Исполнитель воркеров стал доступным')

                async with asyncio.TaskGroup() as task_group:
                    for worker in self._process.workers:
                        handler = ResourceHandler(
                            self._manager_uuid,
                            self._process,
                            worker,
                            backend,
                            broker,
                            self._transmitter,
                            lock,
                            self._shutdown_event
                        )

                        task_group.create_task(handler.run())
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] Менеджеру ресурсов не удалось открыть или закрыть backend / broker: {error}'
            )

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Менеджер ресурсов запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        logger.debug(f'[{self._process.uuid}] Менеджер ресурсов запустил обработчики ресурсов')
        await self._run_handlers()
        logger.debug(f'[{self._process.uuid}] Менеджер ресурсов завершил обработчики ресурсов')

        self._transmitter.close()

        logger.debug(f'[{self._process.uuid}] Менеджер ресурсов завершил работу')
