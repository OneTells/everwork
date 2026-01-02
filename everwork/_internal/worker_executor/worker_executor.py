from __future__ import annotations

import asyncio
import os
import signal
import time
import typing
from contextlib import suppress
from inspect import signature
from multiprocessing import connection, Pipe, Process as BaseProcess
from types import FrameType
from typing import Any

from loguru import logger
from orjson import dumps
from redis.asyncio import Redis

from everwork._internal.resource.resource_manager import ResourceManager
from everwork._internal.utils.redis_retry import GracefulShutdownRetry, RetryShutdownException
from everwork._internal.utils.single_value_channel import ChannelClosed, SingleValueChannel
from everwork.events import EventCollector, EventPublisher, HybridEventStorage
from everwork.schemas import Process, WorkerSettings
from everwork.workers.base import AbstractWorker

if typing.TYPE_CHECKING:
    from loguru import Logger

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class SignalHandler:

    def __init__(
        self,
        shutdown_event: asyncio.Event,
        is_executing_callback: typing.Callable[[], bool],
        on_resource_runner_cancel: typing.Callable[[], None],
    ) -> None:
        self._shutdown_event = shutdown_event
        self._is_executing_callback = is_executing_callback
        self._on_resource_runner_cancel = on_resource_runner_cancel

    def _handle_shutdown_signal(self, *_: Any) -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

        self._on_resource_runner_cancel()

    def _handle_terminate_signal(self, signal_num: int, frame: FrameType | None) -> None:
        if not self._is_executing_callback():
            return

        signal.default_int_handler(signal_num, frame)

    def register(self) -> None:
        signal.signal(signal.SIGUSR1, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_terminate_signal)


class ProcessNotifier:

    def __init__(self, pipe_connection: connection.Connection) -> None:
        self._pipe_connection = pipe_connection

    def notify_worker_start(self, worker_settings: WorkerSettings) -> None:
        data = {
            'worker_name': worker_settings.name,
            'end_time': time.time() + worker_settings.execution_timeout
        }

        self._pipe_connection.send_bytes(dumps(data))

    def notify_worker_end(self) -> None:
        self._pipe_connection.send_bytes(b'')


class WorkerRegistry:

    def __init__(self, process: Process) -> None:
        self._process = process

        self._workers: dict[str, AbstractWorker] = {}
        self._worker_params: dict[str, list[str]] = {}

    async def initialize(self) -> None:
        for worker_cls in self._process.workers:
            try:
                worker = worker_cls()
            except Exception as error:
                logger.exception(f'({worker_cls.settings.name}) Ошибка при инициализации: {error}')
                continue

            self._workers[worker.settings.name] = worker
            self._worker_params[worker.settings.name] = list(signature(worker.__call__).parameters.keys())

    async def startup_all(self) -> None:
        for worker in self._workers.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Ошибка при startup: {error}')

    async def shutdown_all(self) -> None:
        for worker in self._workers.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'({worker.settings.name}) Ошибка при shutdown: {error}')

    def get_worker(self, name: str) -> AbstractWorker:
        return self._workers[name]

    def get_worker_params(self, name: str) -> list[str]:
        return self._worker_params[name]


class EventProcessor:

    def __init__(
        self,
        worker_registry: WorkerRegistry,
        pipe_connection: connection.Connection,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool]
    ) -> None:
        self._worker_registry = worker_registry
        self._response_channel = response_channel
        self._answer_channel = answer_channel

        self._notifier = ProcessNotifier(pipe_connection)
        self._is_executing = False

    def is_executing(self) -> bool:
        return self._is_executing

    async def process_next(self, storage: HybridEventStorage, collector: EventCollector, publisher: EventPublisher) -> bool:
        try:
            worker_name, kwargs_raw = await self._response_channel.receive()
        except ChannelClosed:
            return False

        worker = self._worker_registry.get_worker(worker_name)
        worker_params = self._worker_registry.get_worker_params(worker_name)

        kwargs = {k: v for k, v in kwargs_raw.items() if k in worker_params}

        if 'collector' in worker_params:
            storage.max_events_in_memory = worker.settings.event_storage.max_events_in_memory
            kwargs['collector'] = collector

        self._notifier.notify_worker_start(worker.settings)

        try:
            self._is_executing = True
            await worker.__call__(**kwargs)
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.exception(f'({worker_name}) Выполнение прервано по таймауту')
            success = False
        except Exception as error:
            logger.exception(f'({worker_name}) Ошибка при обработке события: {error}')
            success = False
        else:
            success = True
        finally:
            self._is_executing = False
            self._notifier.notify_worker_end()

        if success:
            try:
                await publisher.push_events_from_async_iterator(
                    storage.read_all(),
                    worker.settings.event_publisher.max_batch_size
                )
            except RetryShutdownException:
                logger.exception(f'({worker_name}) Ошибка Redis при сохранении ивентов')
                success = False

        self._answer_channel.send(success)
        return True


class WorkerExecutor:

    def __init__(self, redis_dsn: str, process: Process, pipe_connection: connection.Connection) -> None:
        self._redis_dsn = redis_dsn
        self._process = process
        self._pipe_connection = pipe_connection

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)

        self._shutdown_event = asyncio.Event()

        self._response_channel = SingleValueChannel[tuple[str, dict[str, Any]]]()
        self._answer_channel = SingleValueChannel[bool]()

        self._resource_manager = ResourceManager(
            self._redis_dsn,
            self._process,
            self._response_channel,
            self._answer_channel
        )

        self._worker_registry = WorkerRegistry(process)

        self._event_processor = EventProcessor(
            self._worker_registry,
            self._pipe_connection,
            self._response_channel,
            self._answer_channel
        )

        self._signal_handler = SignalHandler(
            self._shutdown_event,
            self._event_processor.is_executing,
            self._resource_manager.cancel
        )

    async def _run_worker_loop(self) -> None:
        retry = GracefulShutdownRetry(self._process.redis_backoff_strategy, self._shutdown_event)

        async with Redis.from_url(self._redis_dsn, retry=retry, protocol=3, decode_responses=True) as redis:
            async with HybridEventStorage() as storage:
                collector = EventCollector(storage)
                publisher = EventPublisher(redis)

                while await self._event_processor.process_next(storage, collector, publisher):
                    await storage.clear()

    async def run(self) -> None:
        logger.debug(f'[{self._worker_names}] Исполнитель воркеров запущен')

        self._signal_handler.register()

        self._response_channel.bind_to_event_loop(asyncio.get_running_loop())
        self._resource_manager.start()

        await self._worker_registry.initialize()
        await self._worker_registry.startup_all()

        try:
            await self._run_worker_loop()
        except Exception as error:
            logger.critical(f'[{self._worker_names}] Исполнитель воркеров завершился с ошибкой: {error}')

        await self._worker_registry.shutdown_all()

        logger.debug(f'[{self._worker_names}] Исполнитель воркеров начал завершение')

        self._resource_manager.join()
        self._pipe_connection.close()

        logger.debug(f'[{self._worker_names}] Исполнитель воркеров завершил работу')


class WorkerProcess:

    def __init__(self, redis_dsn: str, process: Process) -> None:
        self._redis_dsn = redis_dsn
        self._process = process

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)

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
                'redis_dsn': self._redis_dsn,
                'process': self._process,
                'pipe_connection': self._pipe_writer,
                'logger_': logger
            }
        )
        self._base_process.start()

    async def close(self) -> None:
        if self._base_process is None:
            return

        if self._base_process.is_alive():
            logger.debug(f'[{self._worker_names}] Подан сигнал о закрытии процесса')
            os.kill(self._base_process.pid, signal.SIGUSR1)

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self._process.workers)

        while True:
            if time.time() > end_time:
                break

            if not self._base_process.is_alive():
                break

            await asyncio.sleep(0.001)

        if self._base_process.is_alive():
            logger.debug(f'[{self._worker_names}] Подан сигнал о немедленном закрытии процесса')
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
                f'[{self._worker_names}] Не удалось мягко завершить процесс, отправлен сигнал SIGKILL. '
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
    def _run(
        redis_dsn: str,
        process: Process,
        pipe_connection: connection.Connection,
        logger_: Logger
    ) -> None:
        logger.remove()
        logger_.reinstall()

        with suppress(KeyboardInterrupt):
            with asyncio.Runner(loop_factory=new_event_loop) as runner:
                runner.run(WorkerExecutor(redis_dsn, process, pipe_connection).run())

        logger.remove()
