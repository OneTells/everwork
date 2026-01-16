import asyncio
import signal
from asyncio import Event, get_running_loop
from multiprocessing import get_start_method
from platform import system
from typing import Annotated, Callable, final
from uuid import UUID

from loguru import logger
from pydantic import AfterValidator, ConfigDict, validate_call

from everwork._internal.process.process_supervisor import ProcessSupervisor
from everwork._internal.utils.task_utils import wait_for_or_cancel
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process, ProcessGroup


def check_environment() -> bool:
    current_system = system()

    if current_system != "Linux":
        logger.critical(
            f"Библиотека работает только на Linux. "
            f"Текущая система: {current_system}"
        )
        return False

    start_method = get_start_method()

    if start_method not in ("spawn", "forkserver"):
        logger.critical(
            f"Поддерживаемые методы запуска процессов: spawn, forkserver. "
            f"Текущий метод: {start_method}"
        )
        return False

    return True


def validate_worker_names(processes: list[ProcessGroup | Process]) -> list[ProcessGroup | Process]:
    names: set[str] = set()

    for item in processes:
        process = item.process if isinstance(item, ProcessGroup) else item

        for worker in process.workers:
            if worker.settings.name in names:
                raise ValueError(f"Имя воркера {worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return processes


def expand_groups(processes: list[ProcessGroup | Process]) -> list[Process]:
    result: list[Process] = []

    for item in processes:
        if isinstance(item, Process):
            result.append(item)
            continue

        for _ in range(item.replicas):
            result.append(item.process.model_copy(deep=True))

    return result


class SignalHandler:

    def __init__(self, shutdown_event: Event):
        self._shutdown_event = shutdown_event

    def _handle_signal(self, *_) -> None:
        loop = get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

    def register(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)


@final
class ProcessManager:

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(
        self,
        *,
        uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)],
        processes: Annotated[
            list[ProcessGroup | Process],
            AfterValidator(validate_worker_names),
            AfterValidator(expand_groups)
        ],
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker]
    ) -> None:
        self._uuid = uuid
        self._processes: list[Process] = processes
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory

        self._shutdown_event = asyncio.Event()

    async def _startup(self) -> None:
        worker_settings = [
            worker.settings
            for process in self._processes
            for worker in process.workers
        ]

        async with self._backend_factory() as backend:
            await wait_for_or_cancel(backend.initialize_manager(self._uuid, worker_settings), self._shutdown_event)
            await wait_for_or_cancel(backend.set_manager_status(self._uuid, 'on'), self._shutdown_event)

    async def _shutdown(self) -> None:
        async with self._backend_factory() as backend:
            await wait_for_or_cancel(backend.set_manager_status(self._uuid, 'off'), self._shutdown_event)

    async def _start_supervisors(self) -> None:
        async with asyncio.TaskGroup() as task_group:
            for process in self._processes:
                supervisor = ProcessSupervisor(
                    self._uuid,
                    process,
                    self._backend_factory,
                    self._broker_factory,
                    self._shutdown_event
                )

                task_group.create_task(supervisor.run())

            logger.info('Наблюдатели процессов запущены')

    async def run(self) -> None:
        if not check_environment():
            return

        logger.info('Менеджер процессов запущен')

        SignalHandler(self._shutdown_event).register()

        await self._startup()
        logger.info('Менеджер процессов инициализирован')

        await self._start_supervisors()

        await self._shutdown()
        logger.info('Менеджер процессов завершил работу')
