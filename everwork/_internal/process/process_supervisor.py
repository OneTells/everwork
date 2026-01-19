import asyncio
import time
from contextlib import suppress
from typing import Callable

from loguru import logger
from orjson import loads
from pydantic import BaseModel

from everwork._internal.utils.task_utils import OperationCancelled, poll_connection, wait_for_or_cancel
from everwork._internal.worker.worker_process import WorkerProcess
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process


class StartEvent(BaseModel):
    worker_name: str
    end_time: float


class ProcessSupervisor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        backend: AbstractBackend,
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend = backend
        self._shutdown_event = shutdown_event

        self._worker_process = WorkerProcess(manager_uuid, process, backend_factory, broker_factory)

    async def _restart_worker_manager(self, worker_name: str) -> None:
        logger.warning(f'[{self._process.uuid}] Воркер {worker_name} завис. Начат перезапуск процесса')
        await self._worker_process.close()
        await self._worker_process.start()
        logger.warning(f'[{self._process.uuid}] Процесс перезапущен')

    async def _run_monitoring_cycle(self) -> None:
        with suppress(OperationCancelled):
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

                await self._backend.mark_worker_executor_for_reboot(self._manager_uuid, self._process.uuid)

                await self._restart_worker_manager(state.worker_name)

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Супервайзер процесса запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        await self._worker_process.start()

        logger.debug(f'[{self._process.uuid}] Супервайзер процесса начал мониторинг')
        await self._run_monitoring_cycle()
        logger.debug(f'[{self._process.uuid}] Супервайзер процесса закончил мониторинг')

        await self._worker_process.close()

        logger.debug(f'[{self._process.uuid}] Супервайзер процесса завершил работу')
