from __future__ import annotations

import asyncio
import os
import signal
import time
import typing
from multiprocessing import connection, Process

from loguru import logger
from orjson import dumps
from pydantic import validate_call
from redis.asyncio import Redis

from .base_worker import BaseWorker
from .resource_manager import ResourceManagerRunner
from .utils import SingleValueChannel

if typing.TYPE_CHECKING:
    from loguru import Logger

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class WorkerManager:

    def __init__(self, redis_dsn: str, workers: list[type[BaseWorker]], pipe_connection: connection.Connection) -> None:
        self.__redis_dsn = redis_dsn
        self.__workers = workers
        self.__pipe_connection = pipe_connection

        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__is_shutdown = False

        self.__response_channel = SingleValueChannel[tuple[str, dict[str, typing.Any]]]()
        self.__answer_channel = SingleValueChannel[bool]()

        self.__resource_manager_runner = ResourceManagerRunner(
            self.__redis_dsn,
            self.__workers,
            self.__response_channel,
            self.__answer_channel
        )

        self.__worker_instances: dict[str, BaseWorker] = {}
        self.__is_execute = False

    def __handle_shutdown_signal(self, *_) -> None:
        self.__is_shutdown = True
        self.__resource_manager_runner.cancel()

    def __handle_terminate_signal(self, *_) -> None:
        if not self.__is_execute:
            return

        raise KeyboardInterrupt

    def __notify_worker_start(self, worker: BaseWorker) -> None:
        if self.__is_shutdown:
            return

        self.__pipe_connection.send_bytes(dumps({
            'worker_name': worker.settings.name,
            'end_time': time.time() + worker.settings.execution_timeout
        }))

    def __notify_worker_end(self) -> None:
        if self.__is_shutdown:
            return

        self.__pipe_connection.send_bytes(b'')

    async def __init_workers(self, redis: Redis) -> None:
        for worker in self.__workers:
            try:
                worker_obj = worker()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Не удалось выполнить __init__: {error}')
                continue

            worker_obj.initialize(redis)
            worker_obj.__call__ = validate_call(worker_obj.__call__)

            self.__worker_instances[worker_obj.settings.name] = worker_obj

    async def __startup_workers(self) -> None:
        for worker in self.__worker_instances.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Не удалось выполнить startup: {error}')

    async def __shutdown_workers(self) -> None:
        for worker in self.__worker_instances.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Не удалось выполнить shutdown: {error}')

    async def __run_worker_loop(self, redis: Redis) -> None:
        await self.__init_workers(redis)
        await self.__startup_workers()

        while True:
            try:
                worker_name, kwargs = await self.__response_channel.receive()
            except asyncio.CancelledError:
                logger.debug(f'[{self.__worker_names}] Чтение канала было отменено')
                break

            worker = self.__worker_instances[worker_name]

            self.__notify_worker_start(worker)

            try:
                self.__is_execute = True

                try:
                    async with worker.event_publisher:
                        await worker.__call__(**kwargs)
                finally:
                    self.__is_execute = False
            except (KeyboardInterrupt, asyncio.CancelledError):
                logger.exception(f'({worker_name}) Задача обрабатывалась дольше максимально разрешенного времени')
                self.__answer_channel.send(False)
            except Exception as error:
                logger.exception(f'({worker_name}) Не удалось обработать ивент. Ошибка: {error}')
                self.__answer_channel.send(False)
            else:
                self.__answer_channel.send(True)
            finally:
                del worker_name, kwargs, worker

            self.__notify_worker_end()

        await self.__shutdown_workers()

    async def run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Менеджер воркеров запущен')

        signal.signal(signal.SIGUSR1, self.__handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self.__handle_terminate_signal)

        self.__response_channel.bind_to_event_loop(asyncio.get_running_loop())

        self.__resource_manager_runner.start()

        async with Redis.from_url(self.__redis_dsn, protocol=3, decode_responses=True) as redis:
            await self.__run_worker_loop(redis)

        logger.debug(f'[{self.__worker_names}] Менеджер воркеров начал завершение')

        self.__resource_manager_runner.join()

        self.__pipe_connection.close()

        logger.debug(f'[{self.__worker_names}] Менеджер воркеров завершил работу')


def _run_worker_manager(
    redis_dsn: str,
    workers: list[type[BaseWorker]],
    pipe_connection: connection.Connection,
    logger_: Logger
) -> None:
    logger_.reinstall()

    with asyncio.Runner(loop_factory=new_event_loop) as runner:
        runner.run(WorkerManager(redis_dsn, workers, pipe_connection).run())


class WorkerManagerRunner:
    __SHUTDOWN_TIMEOUT_SECONDS = 20

    def __init__(self, redis_dsn: str, workers: list[type[BaseWorker]]) -> None:
        self.__redis_dsn = redis_dsn
        self.__workers = workers

        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__process: Process | None = None

    def is_close(self) -> bool:
        return self.__process is None

    def start(self, pipe_writer_connection: connection.Connection) -> None:
        self.__process = Process(
            target=_run_worker_manager,
            kwargs={
                'redis_dsn': self.__redis_dsn,
                'workers': self.__workers,
                'pipe_connection': pipe_writer_connection,
                'logger_': logger
            }
        )
        self.__process.start()

    def close(self) -> None:
        if self.__process is None:
            return

        if self.__process.is_alive():
            logger.debug(f'[{self.__worker_names}] Подан сигнал о закрытии процесса')
            os.kill(self.__process.pid, signal.SIGUSR1)

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self.__workers)

        while True:
            if time.time() > end_time:
                break

            if not self.__process.is_alive():
                break

            time.sleep(0.01)

        if self.__process.is_alive():
            logger.debug(f'[{self.__worker_names}] Подан сигнал о немедленном закрытии процесса')
            os.kill(self.__process.pid, signal.SIGTERM)

        end_time = time.time() + self.__SHUTDOWN_TIMEOUT_SECONDS

        while True:
            if time.time() > end_time:
                break

            if not self.__process.is_alive():
                break

            time.sleep(0.01)

        if self.__process.is_alive():
            logger.critical(
                f'[{self.__worker_names}] Не удалось мягко завершить процесс, отправлен сигнал SIGKILL. '
                f'Возможно зависание воркеров, необходимо перезапустить менеджер'
            )
            os.kill(self.__process.pid, signal.SIGKILL)

        self.__process.join()
        self.__process.close()
        self.__process = None
