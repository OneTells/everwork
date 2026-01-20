from __future__ import annotations

import asyncio
import os
import signal
import time
from multiprocessing import connection, Pipe, Process as BaseProcess
from typing import Any, Callable, TYPE_CHECKING

from loguru import logger

from everwork._internal.worker.worker_manager import WorkerManager
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process

if TYPE_CHECKING:
    from loguru import Logger

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class WorkerProcess:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker]
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory

        self._base_process: BaseProcess | None = None
        self._pipe_reader: connection.Connection | None = None
        self._pipe_writer: connection.Connection | None = None

    @property
    def pipe_reader(self) -> connection.Connection:
        if self._pipe_reader is None:
            raise RuntimeError("Процесс не запущен")

        return self._pipe_reader

    async def start(self) -> None:
        if self._base_process is not None:
            raise ValueError('Нельзя запустить менеджер воркеров повторно, пока он запущен')

        self._pipe_reader, self._pipe_writer = Pipe(duplex=False)

        self._base_process = BaseProcess(
            target=self._run,
            kwargs={
                'manager_uuid': self._manager_uuid,
                'process': self._process,
                'backend_factory': self._backend_factory,
                'broker_factory': self._broker_factory,
                'pipe_connection': self._pipe_writer,
                'logger_': logger
            }
        )
        self._base_process.start()

    async def close(self) -> None:
        if self._base_process is None:
            return

        if self._base_process.is_alive():
            logger.debug(f'[{self._process.uuid}] Подан сигнал о закрытии процесса')
            os.kill(self._base_process.pid, signal.SIGUSR1)

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self._process.workers)

        while True:
            if time.time() > end_time:
                break

            if not self._base_process.is_alive():
                break

            await asyncio.sleep(0.001)

        if self._base_process.is_alive():
            logger.debug(f'[{self._process.uuid}] Подан сигнал о немедленном закрытии процесса')
            os.kill(self._base_process.pid, signal.SIGTERM)

        end_time = time.time() + self._process.shutdown_timeout

        while True:
            if time.time() > end_time:
                break

            if not self._base_process.is_alive():
                break

            await asyncio.sleep(0.001)

        if self._base_process.is_alive():
            logger.critical(
                f'[{self._process.uuid}] Не удалось мягко завершить процесс, отправлен сигнал SIGKILL. '
                f'Возможно зависание воркеров, необходимо перезапустить менеджер'
            )
            os.kill(self._base_process.pid, signal.SIGKILL)

        await asyncio.to_thread(self._base_process.join)

        self._base_process.close()
        self._base_process = None

        self._pipe_reader.close()
        self._pipe_writer.close()

        self._pipe_reader = None
        self._pipe_writer = None

    @staticmethod
    def _run(logger_: Logger, **kwargs: Any) -> None:
        logger.remove()
        logger_.reinstall()

        with asyncio.Runner(loop_factory=new_event_loop) as runner:
            runner.run(WorkerManager(**kwargs).run())

        logger.remove()
