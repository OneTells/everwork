import asyncio
import signal
from types import FrameType
from typing import Any, Callable

from loguru import logger

from everwork._internal.resource.resource_manager import ResourceManager
from everwork._internal.utils.async_thread import AsyncThread
from everwork._internal.worker.utils.executor_channel import create_executor_channel
from everwork._internal.worker.utils.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.worker.worker_executor import WorkerExecutor
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.events import HybridEventStorage
from everwork.schemas import Process


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


class WorkerManager:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        notifier: HeartbeatNotifier
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._notifier = notifier

        self._transmitter, self._receiver = create_executor_channel()

        self._shutdown_event = asyncio.Event()
        self._terminate_event = asyncio.Event()

        self._is_executing_event = asyncio.Event()

        self._resource_manager = AsyncThread(
            target=lambda **kwargs: ResourceManager(**kwargs).run(),
            kwargs={
                'manager_uuid': self._manager_uuid,
                'process': self._process,
                'backend_factory': self._backend_factory,
                'broker_factory': self._broker_factory,
                'transmitter': self._transmitter
            }
        )

        self._worker_registry = WorkerRegistry(process)

    async def _startup(self) -> None:
        self._resource_manager.start()

        await self._worker_registry.initialize()
        await self._worker_registry.startup_all()

    async def _shutdown(self) -> None:
        await self._worker_registry.shutdown_all()

        self._resource_manager.join()

    async def _run_worker_executor(self) -> None:
        try:
            async with self._backend_factory() as backend, self._broker_factory() as broker:
                logger.debug(f'[{self._process.uuid}] Менеджер воркеров инициализировал backend и broker')

                async with HybridEventStorage() as storage:
                    executor = WorkerExecutor(
                        self._manager_uuid,
                        self._process,
                        self._worker_registry,
                        self._receiver,
                        backend,
                        broker,
                        storage,
                        self._notifier,
                        self._is_executing_event,
                        self._shutdown_event,
                        self._terminate_event
                    )

                    await executor.run()
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] Ошибка при открытии или закрытии backend и broker: {error}'
            )

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Менеджер воркеров запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        SignalHandler(
            self._shutdown_event,
            self._terminate_event,
            self._is_executing_event.is_set,
            self._resource_manager.cancel
        ).register()

        await self._startup()
        logger.debug(f'[{self._process.uuid}] Менеджер воркеров выполнил startup')

        logger.debug(f'[{self._process.uuid}] Менеджер воркеров запустил исполнитель воркеров')
        await self._run_worker_executor()
        logger.debug(f'[{self._process.uuid}] Менеджер воркеров завершил исполнитель воркеров')

        await self._shutdown()
        logger.debug(f'[{self._process.uuid}] Менеджер воркеров выполнил shutdown')

        self._notifier.close()

        logger.debug(f'[{self._process.uuid}] Менеджер воркеров завершил работу')
