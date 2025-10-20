from __future__ import annotations

import asyncio
import signal
import typing
from multiprocessing import connection
from threading import Lock

from loguru import logger

from .base_worker import BaseWorker
from .resource_handler import TriggerResourceHandler, ExecutorResourceHandler
from .schemas import TriggerMode
from .utils import ShutdownEvent
from .worker_supervisor import WorkerSupervisor

if typing.TYPE_CHECKING:
    from loguru import Logger

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class WorkerManager:

    def __init__(self, redis_dsn: str, workers: list[type[BaseWorker]], pipe_connection: connection.Connection) -> None:
        self.__workers = workers
        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__shutdown_event = ShutdownEvent()
        self.__worker_supervisors: list[WorkerSupervisor] = []

        self.__pipe_connection = pipe_connection

        lock = Lock()

        for worker in self.__workers:
            if isinstance(worker.settings.mode, TriggerMode):
                resource_handler = TriggerResourceHandler
            else:
                resource_handler = ExecutorResourceHandler

            self.__worker_supervisors.append(
                WorkerSupervisor(redis_dsn, worker, self.__shutdown_event, lock, self.__pipe_connection, resource_handler)
            )

    def __handle_shutdown_signal(self, *_) -> None:
        logger.info(f'[{self.__worker_names}] Получен сигнал о закрытии менеджера воркеров')

        self.__shutdown_event.set()

        for worker_supervisor in self.__worker_supervisors:
            worker_supervisor.close()

    async def __run(self) -> None:
        logger.info(f'[{self.__worker_names}] Менеджер воркеров запущен')

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)

        for worker_supervisor in self.__worker_supervisors:
            worker_supervisor.run()

        logger.info(f'[{self.__worker_names}] Наблюдатели воркеров запущены')

        for worker_supervisor in self.__worker_supervisors:
            worker_supervisor.wait()

        self.__pipe_connection.close()

        logger.info(f'[{self.__worker_names}] Менеджер воркеров завершил работу')

    @classmethod
    def run(
        cls,
        redis_dsn: str,
        workers: list[type[BaseWorker]],
        pipe_connection: connection.Connection,
        logger_: Logger
    ) -> None:
        logger_.reinstall()

        with asyncio.Runner(loop_factory=new_event_loop, debug=True) as runner:
            runner.run(cls(redis_dsn, workers, pipe_connection).__run())
