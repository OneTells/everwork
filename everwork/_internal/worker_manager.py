from __future__ import annotations

import asyncio
import os
import signal
import time
import typing
from contextlib import suppress
from inspect import signature
from multiprocessing import connection, Process as BaseProcess
from types import FrameType
from typing import Any

from loguru import logger
from orjson import dumps
from redis.asyncio import Redis

from everwork.events import EventCollector, EventPublisher, HybridEventStorage
from everwork.schemas import Process
from everwork.workers.base import AbstractWorker
from .utils.redis_retry import GracefulShutdownRetry, RetryShutdownException
from .utils.single_value_channel import ChannelClosed, SingleValueChannel
from .resource_manager import ResourceManagerRunner

if typing.TYPE_CHECKING:
    from loguru import Logger

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class _WorkerManager:

    def __init__(self, redis_dsn: str, process: Process, pipe_connection: connection.Connection) -> None:
        self.__redis_dsn = redis_dsn
        self.__process = process
        self.__pipe_connection = pipe_connection

        self.__worker_names = ', '.join(worker.settings.name for worker in process.workers)

        self.__shutdown_event = asyncio.Event()
        self.__is_execute = False

        self.__response_channel = SingleValueChannel[tuple[str, dict[str, Any]]]()
        self.__answer_channel = SingleValueChannel[bool]()

        self.__resource_manager_runner = ResourceManagerRunner(
            self.__redis_dsn,
            self.__process,
            self.__response_channel,
            self.__answer_channel
        )

        self.__worker_instances: dict[str, AbstractWorker] = {}
        self.__worker_parameters: dict[str, list[str]] = {}

    def __handle_shutdown_signal(self, *_: Any) -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(self.__shutdown_event.set)  # type: ignore

        self.__resource_manager_runner.cancel()

    def __handle_terminate_signal(self, signal_num: int, frame: FrameType | None) -> None:
        if not self.__is_execute:
            return

        signal.default_int_handler(signal_num, frame)

    def __register_signals(self) -> None:
        signal.signal(signal.SIGUSR1, self.__handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self.__handle_terminate_signal)

    def __notify_worker_start(self, worker: AbstractWorker) -> None:
        if self.__shutdown_event.is_set():
            return

        data = {
            'worker_name': worker.settings.name,
            'end_time': time.time() + worker.settings.execution_timeout
        }

        self.__pipe_connection.send_bytes(dumps(data))

    def __notify_worker_end(self) -> None:
        if self.__shutdown_event.is_set():
            return

        self.__pipe_connection.send_bytes(b'')

    async def __init_workers(self) -> None:
        for worker_cls in self.__process.workers:
            try:
                worker = worker_cls()
            except Exception as error:
                logger.exception(f'({worker_cls.settings.name}) Ошибка при инициализации: {error}')
                continue

            self.__worker_instances[worker.settings.name] = worker
            self.__worker_parameters[worker.settings.name] = list(signature(worker.__call__).parameters.keys())

    async def __startup_workers(self) -> None:
        for worker in self.__worker_instances.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Ошибка при startup: {error}')

    async def __shutdown_workers(self) -> None:
        for worker in self.__worker_instances.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Ошибка при shutdown: {error}')

    async def __process_next_event(
        self,
        storage: HybridEventStorage,
        collector: EventCollector,
        publisher: EventPublisher
    ) -> bool:
        try:
            worker_name, kwargs_raw = await self.__response_channel.receive()
        except ChannelClosed:
            logger.debug(f'[{self.__worker_names}] Канал закрыт')
            return False

        worker = self.__worker_instances[worker_name]
        worker_params = self.__worker_parameters[worker_name]

        kwargs = {
            key: value
            for key, value in kwargs_raw.items()
            if key in worker_params
        }

        if 'collector' in worker_params:
            storage.max_events_in_memory = worker.settings.event_storage.max_events_in_memory
            kwargs['collector'] = collector

        self.__notify_worker_start(worker)

        try:
            self.__is_execute = True

            try:
                await worker.__call__(**kwargs)
            finally:
                self.__is_execute = False
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.exception(f'({worker_name}) Выполнение прервано по таймауту')
            answer = False
        except Exception as error:
            logger.exception(f'({worker_name}) Ошибка при обработке события: {error}')
            answer = False
        else:
            answer = True
        finally:
            self.__notify_worker_end()

        if answer:
            try:
                await publisher.push_events_from_async_iterator(
                    storage.read_all(),
                    worker.settings.event_publisher.max_batch_size
                )
            except RetryShutdownException:
                logger.exception(f'({worker_name}) Ошибка Redis при сохранении ивентов')
                answer = False

        self.__answer_channel.send(answer)
        return True

    async def __run_worker_loop(self) -> None:
        await self.__init_workers()
        await self.__startup_workers()

        retry = GracefulShutdownRetry(self.__process.redis_backoff_strategy, self.__shutdown_event)

        try:
            async with Redis.from_url(self.__redis_dsn, retry=retry, protocol=3, decode_responses=True) as redis:
                async with HybridEventStorage() as storage:
                    collector = EventCollector(storage)
                    publisher = EventPublisher(redis)

                    while await self.__process_next_event(storage, collector, publisher):
                        await storage.clear()
        finally:
            await self.__shutdown_workers()

    async def run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Менеджер воркеров запущен')

        self.__register_signals()
        self.__response_channel.bind_to_event_loop(asyncio.get_running_loop())
        self.__resource_manager_runner.start()

        try:
            await self.__run_worker_loop()
        except Exception as error:
            logger.critical(f'[{self.__worker_names}] Менеджер воркеров завершился с ошибкой: {error}')

        logger.debug(f'[{self.__worker_names}] Менеджер воркеров начал завершение')

        self.__resource_manager_runner.join()
        self.__pipe_connection.close()

        logger.debug(f'[{self.__worker_names}] Менеджер воркеров завершил работу')


def _run_worker_manager(
    redis_dsn: str,
    process: Process,
    pipe_connection: connection.Connection,
    logger_: Logger
) -> None:
    logger.remove()
    logger_.reinstall()

    with suppress(KeyboardInterrupt):
        with asyncio.Runner(loop_factory=new_event_loop) as runner:
            runner.run(_WorkerManager(redis_dsn, process, pipe_connection).run())

    logger.remove()


class WorkerManagerRunner:

    def __init__(self, redis_dsn: str, process: Process) -> None:
        self.__redis_dsn = redis_dsn
        self.__process = process

        self.__worker_names = ', '.join(worker.settings.name for worker in process.workers)

        self.__base_process: BaseProcess | None = None

    def is_close(self) -> bool:
        return self.__base_process is None

    def start(self, pipe_writer_connection: connection.Connection) -> None:
        if self.__base_process is not None:
            raise ValueError('Нельзя запустить менеджер воркеров повторно, пока он запущен')

        self.__base_process = BaseProcess(
            target=_run_worker_manager,
            kwargs={
                'redis_dsn': self.__redis_dsn,
                'process': self.__process,
                'pipe_connection': pipe_writer_connection,
                'logger_': logger
            }
        )
        self.__base_process.start()

    def close(self) -> None:
        if self.__base_process is None:
            return

        if self.__base_process.is_alive():
            logger.debug(f'[{self.__worker_names}] Подан сигнал о закрытии процесса')
            os.kill(self.__base_process.pid, signal.SIGUSR1)

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self.__process.workers)

        while True:
            if time.time() > end_time:
                break

            if not self.__base_process.is_alive():
                break

            time.sleep(0.001)

        if self.__base_process.is_alive():
            logger.debug(f'[{self.__worker_names}] Подан сигнал о немедленном закрытии процесса')
            os.kill(self.__base_process.pid, signal.SIGTERM)

        end_time = time.time() + self.__process.shutdown_timeout

        while True:
            if time.time() > end_time:
                break

            if not self.__base_process.is_alive():
                break

            time.sleep(0.001)

        if self.__base_process.is_alive():
            logger.critical(
                f'[{self.__worker_names}] Не удалось мягко завершить процесс, отправлен сигнал SIGKILL. '
                f'Возможно зависание воркеров, необходимо перезапустить менеджер'
            )
            os.kill(self.__base_process.pid, signal.SIGKILL)

        self.__base_process.join()
        self.__base_process.close()
        self.__base_process = None
