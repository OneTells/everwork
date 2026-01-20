import asyncio
import signal
from multiprocessing import connection
from types import FrameType
from typing import Any, Callable

from loguru import logger

from everwork._internal.resource.resource_manager import ResourceManager
from everwork._internal.utils.async_thread import AsyncThread
from everwork._internal.utils.external_executor import ExecutorReceiver, ExecutorTransmitter
from everwork._internal.utils.single_value_channel import SingleValueChannel
from everwork._internal.worker.worker_loop import WorkerLoop
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
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

        response_channel = SingleValueChannel[tuple[str, dict[str, Any]]]()
        answer_channel = SingleValueChannel[BaseException | None]()

        self._receiver = ExecutorReceiver(response_channel, answer_channel)

        self._resource_manager = AsyncThread(
            target=lambda **kwargs: ResourceManager(**kwargs).run(),
            kwargs={
                'manager_uuid': manager_uuid,
                'process': process,
                'backend_factory': backend_factory,
                'broker_factory': broker_factory,
                'transmitter': ExecutorTransmitter(response_channel, answer_channel)
            }
        )

        self._worker_registry = WorkerRegistry(process)

        self._is_executing_event = asyncio.Event()

    async def _initialize_workers(self) -> None:
        await self._worker_registry.initialize()
        await self._worker_registry.startup_all()

    async def _shutdown_workers(self) -> None:
        await self._worker_registry.shutdown_all()

    async def _run_worker_loop(self) -> None:
        loop = WorkerLoop(
            self._manager_uuid,
            self._process,
            self._backend_factory,
            self._broker_factory,
            self._pipe_connection,
            self._receiver,
            self._worker_registry,
            self._is_executing_event,
        )

        await loop.run()

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Исполнитель воркеров запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        SignalHandler(
            self._shutdown_event,
            self._terminate_event,
            self._is_executing_event.is_set,
            self._resource_manager.cancel
        ).register()

        self._resource_manager.start()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров запустил менеджер ресурсов')

        await self._initialize_workers()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров выполнил startup_all')

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров зашел в цикл обработки')
        await self._run_worker_loop()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров вышел из цикла обработки')

        await self._shutdown_workers()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров выполнил shutdown_all')

        self._resource_manager.join()
        self._pipe_connection.close()

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил работу')
