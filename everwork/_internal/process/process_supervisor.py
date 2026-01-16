import asyncio
import time
from typing import Callable

from loguru import logger
from orjson import loads
from pydantic import BaseModel

from everwork._internal.utils.task_utils import wait_for_pipe_data
from everwork._internal.worker.worker_executor import WorkerProcess
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process


class EventStartMessage(BaseModel):
    worker_name: str
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
        self._shutdown_event = shutdown_event

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)
        self._worker_process = WorkerProcess(manager_uuid, process, backend_factory, broker_factory)

    async def _restart_worker_manager(self, worker_name: str) -> None:
        logger.warning(f'[{self._worker_names}] Воркер {worker_name} завис. Начат перезапуск процесса')

        await self._worker_process.close()

        if self._shutdown_event.is_set():
            logger.warning(f'[{self._worker_names}] Процесс завершен')
            return

        await self._worker_process.start()
        logger.warning(f'[{self._worker_names}] Процесс перезапущен')

    async def _run_monitoring_cycle(self) -> None:
        while not self._shutdown_event.is_set():
            payload = self._worker_process.pipe_reader.recv_bytes()
            state = EventStartMessage.model_validate(loads(payload))

            is_exist_message = await wait_for_pipe_data(
                self._worker_process.pipe_reader,
                self._shutdown_event,
                state.end_time - time.time()
            )

            if self._shutdown_event.is_set():
                return

            if is_exist_message:
                self._worker_process.pipe_reader.recv_bytes()
                continue

            await self._restart_worker_manager(state.worker_name)

    async def run(self) -> None:
        logger.debug(f'[{self._worker_names}] Запущен наблюдатель процесса')

        await self._worker_process.start()

        try:
            await self._run_monitoring_cycle()
        except Exception as error:
            logger.critical(f'[{self._worker_names}] Наблюдатель процесса завершился с ошибкой: {error}')

        logger.debug(f'[{self._worker_names}] Начато завершение наблюдателя процесса')

        await self._worker_process.close()

        logger.debug(f'[{self._worker_names}] Наблюдатель процесса завершил работу')
