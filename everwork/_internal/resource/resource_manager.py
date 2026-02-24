import asyncio
from typing import Callable

from loguru import logger

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.resource.resource_handler import ResourceHandler
from everwork._internal.utils.caller import call
from everwork._internal.worker.utils.executor_channel import ExecutorTransmitter
from everwork.schemas.process import Process


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
        await (
            call(backend.mark_worker_executor_as_available, self._manager_uuid, self._process.uuid)
            .retry(retries=2)
            .wait_for_or_cancel(self._shutdown_event, max_timeout=5)
            .execute(on_error_return=None, on_timeout_return=None, on_cancel_return=None)
        )

    async def _run_handlers(self, backend: AbstractBackend, broker: AbstractBroker) -> None:
        lock = asyncio.Lock()

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

    async def run(self) -> None:
        try:
            async with self._backend_factory() as backend, self._broker_factory() as broker:
                await self._mark_worker_executor_as_available(backend)
                await self._run_handlers(backend, broker)
        except Exception as error:
            logger.opt(exception=True).critical(f'Не удалось открыть или закрыть backend / broker: {error}')

        self._transmitter.close()
