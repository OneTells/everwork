import asyncio
import signal
from types import FrameType
from typing import Any, Callable

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.resource.resource_manager import ResourceManager
from everwork._internal.utils.async_thread import AsyncThread
from everwork._internal.utils.event_storage import HybridStorage
from everwork._internal.worker.utils.executor_channel import create_executor_channel
from everwork._internal.worker.utils.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.worker.worker_executor import WorkerExecutor
from everwork.schemas.process import Process


class SignalHandler:

    def __init__(
        self,
        shutdown_event: asyncio.Event,
        terminate_event: asyncio.Event,
        is_executing_callback: Callable[[], bool],
        on_resource_manager_cancel: Callable[[], None]
    ) -> None:
        self._shutdown_event = shutdown_event
        self._terminate_event = terminate_event
        self._is_executing_callback = is_executing_callback
        self._on_resource_manager_cancel = on_resource_manager_cancel

    def _handle_shutdown_signal(self, *_: Any) -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

        self._on_resource_manager_cancel()

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

        transmitter, self._receiver = create_executor_channel()

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
                'transmitter': transmitter
            }
        )

    async def _start_resource_manager(self) -> None:
        self._resource_manager.start()

    async def _join_resource_manager(self) -> None:
        self._resource_manager.join()

    async def _run_worker_executor(self) -> None:
        with HybridStorage() as storage:
            executor = WorkerExecutor(
                self._manager_uuid,
                self._process,
                self._receiver,
                self._notifier,
                self._is_executing_event,
                storage
            )

            await executor.run()

    async def run(self) -> None:
        SignalHandler(
            self._shutdown_event,
            self._terminate_event,
            self._is_executing_event.is_set,
            self._resource_manager.cancel
        ).register()

        await self._start_resource_manager()
        await self._run_worker_executor()
        await self._join_resource_manager()

        self._notifier.close()
