import asyncio
from multiprocessing import connection
from typing import Callable

from loguru import logger

from everwork._internal.utils.external_executor import ExecutorReceiver
from everwork._internal.worker.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.worker.worker_invoker import WorkerInvoker
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork._internal.worker.worker_task_processor import WorkerTaskProcessor
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.events import EventCollector, EventPublisher, HybridEventStorage
from everwork.schemas import Process


class WorkerLoop:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        pipe_connection: connection.Connection,
        receiver: ExecutorReceiver,
        worker_registry: WorkerRegistry,
        is_executing_event: asyncio.Event,
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._pipe_connection = pipe_connection

        self._receiver = receiver
        self._worker_registry = worker_registry

        self._is_executing_event = is_executing_event

    async def run(self) -> None:
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
                    storage,
                    EventCollector(storage),
                    WorkerInvoker(
                        self._manager_uuid,
                        self._process,
                        HeartbeatNotifier(self._pipe_connection),
                        EventPublisher(storage, broker),
                        self._is_executing_event,
                    )
                )

                logger.debug(f'[{self._process.uuid}] Исполнитель воркеров запустил цикл обработки ивентов')

                while await task_processor.process_task():
                    await storage.clear()

                logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил цикл обработки ивентов')
