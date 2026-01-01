import asyncio
import time
from multiprocessing import connection, Pipe

from loguru import logger
from orjson import loads
from pydantic import BaseModel

from everwork._internal.process_supervisor.redis_task_checker import RedisTaskChecker
from everwork._internal.process_supervisor.utils import wait_for_pipe_data
from everwork._internal.worker_manager import WorkerManagerRunner
from everwork.schemas import Process


class EventStartMessage(BaseModel):
    worker_name: str
    end_time: float


class ProcessSupervisor:

    def __init__(self, redis_dsn: str, process: Process, shutdown_event: asyncio.Event) -> None:
        self._redis_dsn = redis_dsn
        self._process = process
        self._shutdown_event = shutdown_event

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)

        self._pipe_reader: connection.Connection | None = None
        self._pipe_writer: connection.Connection | None = None

        self._worker_manager_runner = WorkerManagerRunner(self._redis_dsn, self._process)
        self._redis_task_checker = RedisTaskChecker(self._redis_dsn, self._process)

    def _start_worker_manager(self) -> None:
        reader, writer = Pipe(duplex=False)
        self._pipe_reader = reader
        self._pipe_writer = writer

        self._worker_manager_runner.start(self._pipe_writer)

    def _close_worker_manager(self) -> None:
        if self._worker_manager_runner.is_close():
            return

        self._worker_manager_runner.close()

        self._pipe_reader.close()
        self._pipe_writer.close()

        self._pipe_reader = None
        self._pipe_writer = None

    async def _handle_hung_worker(self, state: EventStartMessage) -> None:
        logger.warning(f'[{self._worker_names}] Воркер {state.worker_name} завис. Начат перезапуск процесса')

        await asyncio.to_thread(self._close_worker_manager)
        await self._redis_task_checker.check_for_hung_tasks()

        if self._shutdown_event.is_set():
            logger.warning(f'[{self._worker_names}] Процесс завершен')
            return

        await asyncio.to_thread(self._start_worker_manager)
        logger.warning(f'[{self._worker_names}] Процесс перезапущен')

    async def _run_monitoring_cycle(self) -> None:
        while not self._shutdown_event.is_set():
            raw_data = self._pipe_reader.recv_bytes()
            state = EventStartMessage.model_validate(loads(raw_data))

            is_exist_message = await wait_for_pipe_data(
                self._pipe_reader,
                self._shutdown_event,
                state.end_time - time.time()
            )

            if self._shutdown_event.is_set():
                return

            if is_exist_message:
                self._pipe_reader.recv_bytes()
                continue

            await self._handle_hung_worker(state)

    async def run(self) -> None:
        logger.debug(f'[{self._worker_names}] Запущен наблюдатель процесса')

        await self._redis_task_checker.check_for_hung_tasks()
        await asyncio.to_thread(self._start_worker_manager)

        try:
            await self._run_monitoring_cycle()
        except Exception as error:
            logger.critical(f'[{self._worker_names}] Наблюдатель процесса завершился с ошибкой: {error}')

        logger.debug(f'[{self._worker_names}] Начато завершение наблюдателя процесса')

        await asyncio.to_thread(self._close_worker_manager)
        await self._redis_task_checker.check_for_hung_tasks()

        logger.debug(f'[{self._worker_names}] Наблюдатель процесса завершил работу')
