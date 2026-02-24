import asyncio
import signal
from asyncio import Event, get_running_loop
from multiprocessing import get_start_method
from platform import system
from typing import Annotated, Any, Callable, final, Sequence
from uuid import UUID, uuid4

from loguru import logger
from pydantic import AfterValidator, validate_call

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.process.process_supervisor import ProcessSupervisor
from everwork._internal.trigger.trigger_manager import TriggerManager
from everwork._internal.utils.async_thread import AsyncThread
from everwork._internal.utils.caller import call
from everwork.schemas import Process, ProcessGroup
from everwork.utils import AbstractCronSchedule, CronSchedule
from everwork.workers.base import AbstractWorker


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


def validate_worker_titles(processes: Sequence[ProcessGroup | Process]) -> Sequence[ProcessGroup | Process]:
    workers: dict[str, type[AbstractWorker]] = dict()

    for item in processes:
        process = item.process if isinstance(item, ProcessGroup) else item

        for worker in process.workers:
            other_worker = workers.get(worker.settings.id, None)

            if other_worker is not None and other_worker is not worker:
                raise ValueError(f"Имя воркера '{worker.settings.title}' не уникально")

            workers[worker.settings.id] = worker

    return processes


def expand_groups(processes: Sequence[ProcessGroup | Process]) -> Sequence[Process]:
    result: list[Process] = []

    for item in processes:
        if isinstance(item, Process):
            result.append(item)
            continue

        for _ in range(item.replicas):
            result.append(item.process.model_copy(deep=True))

    return result


def validate_processes(processes: Sequence[Process]) -> Sequence[Process]:
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


def get_structure(processes: Sequence[Process]) -> Any:
    return [
        {
            'uuid': p.uuid,
            'workers': [
                {
                    'id': w.settings.id,
                    'title': w.settings.title,
                    'triggers': [
                        {'id': trigger.id, 'title': trigger.title}
                        for trigger in w.settings.triggers
                    ]
                } for w in p.workers
            ]
        } for p in processes
    ]


class SignalHandler:

    def __init__(self, shutdown_event: Event, on_services_cancel: Callable[[], None]) -> None:
        self._shutdown_event = shutdown_event
        self._on_services_cancel = on_services_cancel

    def _handle_signal(self, *_) -> None:
        loop = get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

        self._on_services_cancel()

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
            Sequence[ProcessGroup | Process],
            AfterValidator(validate_worker_titles),
            AfterValidator(expand_groups),
            AfterValidator(validate_processes)
        ],
        backend: Callable[[], AbstractBackend],
        broker: Callable[[], AbstractBroker],
        cron_schedule: type[AbstractCronSchedule] = CronSchedule
    ) -> None:
        self._manager_uuid = uuid
        self._processes: Sequence[Process] = tuple(processes)
        self._backend_factory = backend
        self._broker_factory = broker
        self._cron_schedule = cron_schedule

        self._shutdown_event = asyncio.Event()

        self._services: tuple[AsyncThread, ...] = (
            AsyncThread(
                target=lambda **kwargs: TriggerManager(**kwargs).run(),
                kwargs={
                    'manager_uuid': self._manager_uuid,
                    'processes': self._processes,
                    'backend_factory': self._backend_factory,
                    'broker_factory': self._broker_factory,
                    'cron_schedule': self._cron_schedule
                }
            ),
        )

    async def _startup(self) -> None:
        async with self._backend_factory() as backend:
            await (
                call(backend.build, self._manager_uuid, self._processes)
                .retry(retries=3)
                .wait_for_or_cancel(self._shutdown_event)
                .execute(
                    on_error_return=ValueError,
                    on_timeout_return=ValueError,
                    on_cancel_return=ValueError,
                    log_context='Менеджер процессов'
                )
            )

        async with self._broker_factory() as broker:
            await (
                call(broker.build, self._processes)
                .retry(retries=3)
                .wait_for_or_cancel(self._shutdown_event)
                .execute(
                    on_error_return=ValueError,
                    on_timeout_return=ValueError,
                    on_cancel_return=ValueError,
                    log_context='Менеджер процессов'
                )
            )

    async def _shutdown(self) -> None:
        error: Exception | None = None

        try:
            async with self._backend_factory() as backend:
                await (
                    call(backend.cleanup, self._manager_uuid, self._processes)
                    .retry(retries=3)
                    .wait_for_or_cancel(self._shutdown_event)
                    .execute(
                        on_error_return=ValueError,
                        on_timeout_return=ValueError,
                        on_cancel_return=ValueError,
                        log_context='Менеджер процессов'
                    )
                )
        except ValueError as e:
            error = e

        try:
            async with self._broker_factory() as broker:
                await (
                    call(broker.cleanup, self._processes)
                    .retry(retries=3)
                    .wait_for_or_cancel(self._shutdown_event)
                    .execute(
                        on_error_return=ValueError,
                        on_timeout_return=ValueError,
                        on_cancel_return=ValueError,
                        log_context='Менеджер процессов'
                    )
                )
        except ValueError as e:
            error = e

        if error is not None:
            raise error

    def _start_services(self) -> None:
        for service in self._services:
            service.start()

    def _cancel_services(self) -> None:
        for service in self._services:
            service.cancel()

    def _join_services(self) -> None:
        for service in self._services:
            service.join()

    async def _run_supervisors(self) -> None:
        async with asyncio.TaskGroup() as task_group:
            for process in self._processes:
                supervisor = ProcessSupervisor(
                    self._manager_uuid,
                    process,
                    self._backend_factory,
                    self._broker_factory,
                    self._shutdown_event
                )

                task_group.create_task(supervisor.run())

    async def run(self) -> None:
        logger.info('Менеджер процессов запущен')

        if not check_environment():
            return

        logger.debug(f'Структура менеджера: {get_structure(self._processes)}')

        SignalHandler(self._shutdown_event, self._cancel_services).register()
        logger.debug("Менеджер процессов зарегистрировал обработчик сигналов")

        try:
            await self._startup()

            self._start_services()
            logger.debug(f'Менеджер процессов запустил сервисы')

            await self._run_supervisors()
            logger.debug('Супервайзеры процессов завершены')

            self._join_services()
            logger.debug(f'Менеджер процессов дождался закрытия сервисов')

            await self._shutdown()
        except ValueError:
            ...
        except Exception as error:
            logger.opt(exception=True).critical(f'Менеджеру процессов не удалось открыть или закрыть backend / broker: {error}')

        logger.info('Менеджер процессов завершил работу')
