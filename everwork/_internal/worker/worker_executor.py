import asyncio
from typing import Callable

from loguru import logger

from everwork._internal.utils.external_executor import ExecutorReceiver
from everwork._internal.utils.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork._internal.worker.worker_task_processor import WorkerTaskProcessor
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.events import HybridEventStorage
from everwork.schemas import Process


class WorkerExecutor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        receiver: ExecutorReceiver,
        notifier: HeartbeatNotifier,
        worker_registry: WorkerRegistry,
        is_executing_event: asyncio.Event,
        shutdown_event: asyncio.Event,
        terminate_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._receiver = receiver
        self._notifier = notifier
        self._worker_registry = worker_registry
        self._is_executing_event = is_executing_event
        self._shutdown_event = shutdown_event
        self._terminate_event = terminate_event

    async def _run_worker_loop(self) -> None:
        async with self._backend_factory() as backend, self._broker_factory() as broker:
            logger.debug(f'[{self._process.uuid}] Исполнитель воркеров инициализировал backend и broker')

            await backend.mark_worker_executor_as_available(self._manager_uuid, self._process.uuid)
            logger.debug(f'[{self._process.uuid}] Исполнитель воркеров стал доступным')

            async with HybridEventStorage() as storage:
                task_processor = WorkerTaskProcessor(
                    self._manager_uuid,
                    self._process,
                    self._worker_registry,
                    self._receiver,
                    backend,
                    broker,
                    storage,
                    self._notifier,
                    self._is_executing_event
                )

                logger.debug(f'[{self._process.uuid}] Исполнитель воркеров запустил цикл обработки ивентов')

                while await task_processor.process_task():
                    await storage.clear()

                logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил цикл обработки ивентов')

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Исполнитель воркеров запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров зашел в цикл обработки')
        await self._run_worker_loop()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров вышел из цикла обработки')

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил работу')
