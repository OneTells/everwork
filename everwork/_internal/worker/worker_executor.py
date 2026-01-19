from __future__ import annotations

import asyncio
import os
import signal
import time
from contextlib import suppress
from inspect import signature
from multiprocessing import connection, Pipe, Process as BaseProcess
from types import FrameType
from typing import Any, Callable, TYPE_CHECKING

from loguru import logger
from orjson import dumps

from everwork._internal.resource.resource_manager import ResourceManager
from everwork._internal.utils.async_thread import AsyncThread
from everwork._internal.utils.single_value_channel import ChannelClosed, SingleValueChannel
from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.events import EventCollector, EventPublisher, HybridEventStorage
from everwork.schemas import Process, WorkerSettings
from everwork.workers import AbstractWorker

if TYPE_CHECKING:
    from loguru import Logger

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class SignalHandler:

    def __init__(
        self,
        shutdown_event: asyncio.Event,
        terminate_event: asyncio.Event,
        is_executing_callback: Callable[[], bool],
        on_resource_runner_cancel: Callable[[], None],
    ) -> None:
        self._shutdown_event = shutdown_event
        self._terminate_event = terminate_event
        self._is_executing_callback = is_executing_callback
        self._on_resource_runner_cancel = on_resource_runner_cancel

    def _handle_shutdown_signal(self, *_: Any) -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

        self._on_resource_runner_cancel()

    def _handle_terminate_signal(self, signal_num: int, frame: FrameType | None) -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(self._terminate_event.set)  # type: ignore

        if not self._is_executing_callback():
            return

        signal.default_int_handler(signal_num, frame)

    def register(self) -> None:
        signal.signal(signal.SIGUSR1, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_terminate_signal)


class ProcessNotifier:

    def __init__(self, conn: connection.Connection) -> None:
        self._conn = conn

    def send_start_event(self, worker_settings: WorkerSettings) -> None:
        data = {
            'worker_name': worker_settings.name,
            'end_time': time.time() + worker_settings.execution_timeout
        }

        self._conn.send_bytes(dumps(data))

    def send_completion_event(self) -> None:
        self._conn.send_bytes(b'')


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
                logger.exception(f'[{self._process.uuid}] ({worker_cls.settings.name}) Ошибка при инициализации: {error}')
                continue

            self._workers[worker.settings.name] = worker
            self._worker_params[worker.settings.name] = list(signature(worker.__call__).parameters.keys())

    async def startup_all(self) -> None:
        for worker in self._workers.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Ошибка при startup: {error}')

    async def shutdown_all(self) -> None:
        for worker in self._workers.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Ошибка при shutdown: {error}')

    def get_worker(self, name: str) -> AbstractWorker:
        return self._workers[name]

    def get_worker_params(self, name: str) -> list[str]:
        return self._worker_params[name]


class EventProcessor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        worker_registry: WorkerRegistry,
        pipe_connection: connection.Connection,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[BaseException | None],
        shutdown_event: asyncio.Event,
        terminate_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._worker_registry = worker_registry
        self._response_channel = response_channel
        self._answer_channel = answer_channel
        self._shutdown_event = shutdown_event
        self._terminate_event = terminate_event

        self._notifier = ProcessNotifier(pipe_connection)
        self._is_executing = False

    def is_executing(self) -> bool:
        return self._is_executing

    async def process_next(
        self,
        storage: HybridEventStorage,
        collector: EventCollector,
        publisher: EventPublisher,
        backend: AbstractBackend
    ) -> bool:
        await wait_for_or_cancel(
            backend.mark_worker_executor_as_available(self._manager_uuid, self._process.uuid),
            self._terminate_event
        )

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

        try:
            await wait_for_or_cancel(
                backend.mark_worker_executor_as_busy(self._manager_uuid, self._process.uuid, worker_name),
                self._terminate_event
            )
        except OperationCancelled:
            return False

        self._notifier.send_start_event(worker.settings)

        error_answer: BaseException | None = None

        try:
            self._is_executing = True
            await worker.__call__(**kwargs)
        except (KeyboardInterrupt, asyncio.CancelledError) as error:
            logger.exception(f'[{self._process.uuid}] ({worker_name}) Выполнение прервано по таймауту')
            error_answer = error
        except Exception as error:
            logger.exception(f'[{self._process.uuid}] ({worker_name}) Ошибка при обработке события: {error}')
            error_answer = error
        finally:
            self._is_executing = False
            self._notifier.send_completion_event()

        if error_answer is None:
            try:
                await publisher.push_events_from_async_iterator(
                    storage.read_all(),
                    worker.settings.event_publisher.max_batch_size
                )
            except RetryShutdownException as error:
                logger.exception(f'[{self._process.uuid}] ({worker_name}) Ошибка Redis при сохранении ивентов')
                error_answer = error

        self._answer_channel.send(error_answer)

        await wait_for_or_cancel(
            backend.mark_worker_executor_as_available(self._manager_uuid, self._process.uuid),
            self._terminate_event
        )

        return True


class WorkerExecutor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        pipe_connection: connection.Connection
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._pipe_connection = pipe_connection

        self._shutdown_event = asyncio.Event()
        self._terminate_event = asyncio.Event()

        self._response_channel = SingleValueChannel[tuple[str, dict[str, Any]]]()
        self._answer_channel = SingleValueChannel[BaseException | None]()

        self._resource_manager = AsyncThread(
            target=lambda **kwargs: ResourceManager(**kwargs).run(),
            kwargs={
                'manager_uuid': manager_uuid,
                'process': process,
                'backend_factory': backend_factory,
                'broker_factory': broker_factory,
                'response_channel': self._response_channel,
                'answer_channel': self._answer_channel
            }
        )

        self._worker_registry = WorkerRegistry(process)

        self._event_processor = EventProcessor(
            manager_uuid,
            process,
            self._worker_registry,
            self._pipe_connection,
            self._response_channel,
            self._answer_channel,
            self._shutdown_event,
            self._terminate_event
        )

        self._signal_handler = SignalHandler(
            self._shutdown_event,
            self._terminate_event,
            self._event_processor.is_executing,
            self._resource_manager.cancel
        )

    async def _run_worker_loop(self) -> None:
        async with self._backend_factory() as backend:
            async with self._broker_factory() as broker:
                async with HybridEventStorage() as storage:
                    collector = EventCollector(storage)
                    publisher = EventPublisher(broker)

                    while await self._event_processor.process_next(storage, collector, publisher, backend):
                        await storage.clear()

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Исполнитель воркеров запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        self._signal_handler.register()

        self._resource_manager.start()

        await self._worker_registry.initialize()
        await self._worker_registry.startup_all()

        try:
            await self._run_worker_loop()
        except Exception as error:
            logger.opt(exception=True).critical(f'[{self._process.uuid}] Исполнитель воркеров завершился с ошибкой: {error}')

        await self._worker_registry.shutdown_all()

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров начал завершение')

        self._resource_manager.join()
        self._pipe_connection.close()

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил работу')


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

        with suppress(KeyboardInterrupt):
            with asyncio.Runner(loop_factory=new_event_loop) as runner:
                runner.run(WorkerExecutor(**kwargs).run())

        logger.remove()
