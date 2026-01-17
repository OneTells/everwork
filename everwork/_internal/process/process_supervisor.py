import asyncio
import time
from typing import Callable

from loguru import logger
from orjson import loads
from pydantic import BaseModel

from everwork._internal.utils.task_utils import OperationCancelled, poll_connection, wait_for_or_cancel
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

        if self._shutdown_event.is_set():
            logger.warning(f'[{self._process.uuid}] Процесс завершен')
            return

        await self._worker_process.start()
        logger.warning(f'[{self._process.uuid}] Процесс перезапущен')

    async def _run_monitoring_cycle(self) -> None:
        while not self._shutdown_event.is_set():
            await wait_for_or_cancel(
                poll_connection(self._worker_process.pipe_reader),
                self._shutdown_event
            )

            payload = self._worker_process.pipe_reader.recv_bytes()
            state = EventStartMessage.model_validate(loads(payload))

            timeout = state.end_time - time.time()
            is_exist_message = await wait_for_or_cancel(
                poll_connection(self._worker_process.pipe_reader, timeout),
                self._shutdown_event
            )

            if is_exist_message:
                self._worker_process.pipe_reader.recv_bytes()
                continue

            try:
                async with asyncio.timeout(5):
                    await wait_for_or_cancel(
                        self._backend.mark_worker_for_reboot(self._manager_uuid, self._process.uuid),
                        self._shutdown_event
                    )
            except asyncio.TimeoutError:
                logger.error(
                    f'[{self._process.uuid}] В наблюдателе процесса не удалось отметить '
                    f'воркер {state.worker_name} для перезапуска'
                )

            await self._restart_worker_manager(state.worker_name)

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Запущен наблюдатель процесса. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        await self._worker_process.start()

        try:
            await self._run_monitoring_cycle()
        except OperationCancelled:
            pass
        except Exception as error:
            logger.opt(exception=True).critical(f'[{self._process.uuid}] Наблюдатель процесса завершился с ошибкой: {error}')

        logger.debug(f'[{self._process.uuid}] Начато завершение наблюдателя процесса')

        await self._worker_process.close()

        logger.debug(f'[{self._process.uuid}] Наблюдатель процесса завершил работу')
