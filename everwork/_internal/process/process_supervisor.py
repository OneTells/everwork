import asyncio
import time
from contextlib import suppress
from typing import Callable

from loguru import logger
from orjson import loads
from pydantic import BaseModel

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.process.utils.connection_utils import poll_connection
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.utils.caller import call
from everwork._internal.worker.worker_process import WorkerProcess
from everwork.schemas import Process


class StartEvent(BaseModel):
    worker_id: str
    end_time: float


class ProcessSupervisor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._shutdown_event = shutdown_event

        self._worker_process = WorkerProcess(manager_uuid, process, backend_factory, broker_factory)

    async def _mark_worker_executor_for_reboot(self, worker_id: str) -> None:
        with suppress(Exception):
            async with self._backend_factory() as backend:
                await (
                    call(backend.mark_worker_executor_for_reboot, self._manager_uuid, self._process.uuid)
                    .retry(retries=2)
                    .wait_for_or_cancel(self._shutdown_event, max_timeout=5)
                    .execute(
                        on_error_return=None,
                        on_timeout_return=None,
                        on_cancel_return=None,
                        log_context=f'[{self._process.uuid}] ({worker_id}) Супервайзер процесса'
                    )
                )

    async def _restart_worker_process(self, worker_id: str) -> None:
        logger.warning(f'[{self._process.uuid}] ({worker_id}) Процесс завис и будет перезапущен')

        await self._mark_worker_executor_for_reboot(worker_id)

        await self._worker_process.close()
        await self._worker_process.start()

        logger.warning(f'[{self._process.uuid}] Процесс перезапущен')

    async def _run_monitoring_cycle(self) -> None:
        while not self._shutdown_event.is_set():
            await wait_for_or_cancel(
                poll_connection(self._worker_process.pipe_reader),
                self._shutdown_event
            )

            payload = self._worker_process.pipe_reader.recv_bytes()
            state = StartEvent.model_validate(loads(payload))

            timeout = state.end_time - time.time()
            is_exist_message = await wait_for_or_cancel(
                poll_connection(self._worker_process.pipe_reader, timeout),
                self._shutdown_event
            )

            if is_exist_message:
                self._worker_process.pipe_reader.recv_bytes()
                continue

            await self._restart_worker_process(state.worker_id)

    async def run(self) -> None:
        logger.debug(f'[{self._process.uuid}] Супервайзер процесса запущен')

        await self._worker_process.start()

        with suppress(OperationCancelled):
            await self._run_monitoring_cycle()

        await self._worker_process.close()

        logger.debug(f'[{self._process.uuid}] Супервайзер процесса завершил работу')
