import asyncio
import signal
from asyncio import Event, get_running_loop
from multiprocessing import get_start_method
from platform import system
from typing import Annotated, Callable, final
from uuid import UUID, uuid4

from loguru import logger
from pydantic import AfterValidator, validate_call

from everwork._internal.process.process_supervisor import ProcessSupervisor
from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process, ProcessGroup
from everwork.workers import AbstractWorker


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
    workers: dict[str, type[AbstractWorker]] = dict()

    for item in processes:
        process = item.process if isinstance(item, ProcessGroup) else item

        for worker in process.workers:
            other_worker = workers.get(worker.settings.name, None)

            if other_worker is not None and other_worker is not worker:
                raise ValueError(f"Имя воркера {worker.settings.name} не уникально")

            workers[worker.settings.name] = worker

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


def validate_processes(processes: list[Process]) -> list[Process]:
    result: list[Process] = []
    uuids: set[str] = set()

    for process in processes:
        uuid = process.uuid

        while uuid in uuids:
            uuid = str(uuid4())

        uuids.add(uuid)

        if process.uuid != uuid:
            result.append(process.model_copy(update={'uuid': uuid}))
        else:
            result.append(process)

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

    @validate_call
    def __init__(
        self,
        *,
        uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)],
        processes: Annotated[
            list[ProcessGroup | Process],
            AfterValidator(validate_worker_names),
            AfterValidator(expand_groups),
            AfterValidator(validate_processes)
        ],
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker]
    ) -> None:
        self._manager_uuid = uuid
        self._processes: list[Process] = processes
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory

        self._shutdown_event = asyncio.Event()

    async def _startup(self, backend: AbstractBackend) -> None:
        try:
            await wait_for_or_cancel(
                backend.startup_manager(self._manager_uuid, self._processes),
                self._shutdown_event
            )
        except OperationCancelled:
            logger.warning('Менеджер процессов прервал startup_manager')
            raise

    async def _shutdown(self, backend: AbstractBackend) -> None:
        try:
            await asyncio.wait_for(
                backend.shutdown_manager(self._manager_uuid),
                timeout=5
            )
        except asyncio.TimeoutError:
            logger.warning('Менеджер процессов прервал shutdown_manager')
            raise

    async def _start_supervisors(self, backend: AbstractBackend) -> None:
        async with asyncio.TaskGroup() as task_group:
            for process in self._processes:
                supervisor = ProcessSupervisor(
                    self._manager_uuid,
                    process,
                    self._backend_factory,
                    self._broker_factory,
                    backend,
                    self._shutdown_event
                )

                task_group.create_task(supervisor.run())

    async def run(self) -> None:
        logger.info('Менеджер процессов запущен')

        if not check_environment():
            return

        SignalHandler(self._shutdown_event).register()
        logger.debug("Менеджер процессов зарегистрировал обработчик сигналов")

        try:
            async with self._backend_factory() as backend:
                logger.debug('Менеджер процессов инициализировал backend')

                await self._startup(backend)
                logger.debug('Менеджер процессов выполнил startup')

                await self._start_supervisors(backend)
                logger.debug('Супервайзеры процессов завершены')

                await self._shutdown(backend)
                logger.debug('Менеджер процессов выполнил shutdown')
        except (OperationCancelled, asyncio.TimeoutError):
            pass
        except Exception as error:
            logger.opt(exception=True).critical(f'Менеджер процессов завершился с ошибкой: {error}')
            return

        logger.info('Менеджер процессов завершил работу')
