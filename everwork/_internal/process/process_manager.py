import asyncio
import signal
from asyncio import Event, get_running_loop
from multiprocessing import get_start_method
from platform import system
from typing import Annotated, Callable, final
from uuid import UUID, uuid4

from loguru import logger
from pydantic import AfterValidator, validate_call

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.process.process_supervisor import ProcessSupervisor
from everwork._internal.trigger.trigger_manager import TriggerManager
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.utils.async_thread import AsyncThread
from everwork.schemas import Process, ProcessGroup
from everwork.utils import AbstractCronSchedule, CronSchedule
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
            other_worker = workers.get(worker.settings.slug, None)

            if other_worker is not None and other_worker is not worker:
                raise ValueError(f"Имя воркера {worker.settings.slug} не уникально")

            workers[worker.settings.slug] = worker

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

    def __init__(self, shutdown_event: Event, on_trigger_manager_cancel: Callable[[], None]) -> None:
        self._shutdown_event = shutdown_event
        self._on_trigger_manager_cancel = on_trigger_manager_cancel

    def _handle_signal(self, *_) -> None:
        loop = get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

        self._on_trigger_manager_cancel()

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
        broker_factory: Callable[[], AbstractBroker],
        cron_schedule_factory: Callable[[str], AbstractCronSchedule] = CronSchedule
    ) -> None:
        self._manager_uuid = uuid
        self._processes: list[Process] = processes
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._cron_schedule_factory = cron_schedule_factory

        self._shutdown_event = asyncio.Event()

        self._trigger_manager = AsyncThread(
            target=lambda **kwargs: TriggerManager(**kwargs).run(),
            kwargs={
                'manager_uuid': self._manager_uuid,
                'worker_settings': list({w.settings for p in self._processes for w in p.workers}),
                'backend_factory': self._backend_factory,
                'broker_factory': self._broker_factory,
                'cron_schedule_factory': self._cron_schedule_factory
            }
        )

    async def _startup(self, backend: AbstractBackend, broker: AbstractBroker) -> None:
        try:
            await wait_for_or_cancel(
                backend.build(self._manager_uuid, self._processes),
                self._shutdown_event,
                min_timeout=5
            )
        except OperationCancelled:
            logger.exception('Менеджер процессов прервал build_backend')
            raise ValueError
        except Exception as error:
            logger.opt(exception=True).critical(f'Не удалось построить backend: {error}')
            raise ValueError

        worker_settings = list({w.settings for p in self._processes for w in p.workers})

        try:
            await wait_for_or_cancel(
                broker.build(self._manager_uuid, worker_settings),
                self._shutdown_event,
                min_timeout=5
            )
        except OperationCancelled:
            logger.exception('Менеджер процессов прервал build_broker')
            raise ValueError
        except Exception as error:
            logger.opt(exception=True).critical(f'Не удалось построить broker: {error}')
            raise ValueError

        return None

    async def _shutdown(self, backend: AbstractBackend, broker: AbstractBroker) -> None:
        is_error: bool = False

        try:
            await wait_for_or_cancel(
                backend.cleanup(self._manager_uuid),
                self._shutdown_event,
                min_timeout=5
            )
        except OperationCancelled:
            logger.exception('Менеджер процессов прервал cleanup_backend')
            is_error = True
        except Exception as error:
            logger.opt(exception=True).critical(f'Не удалось очистить backend: {error}')
            is_error = True

        try:
            await wait_for_or_cancel(
                broker.cleanup(self._manager_uuid),
                self._shutdown_event,
                min_timeout=5
            )
        except OperationCancelled:
            logger.exception('Менеджер процессов прервал cleanup_broker')
            is_error = True
        except Exception as error:
            logger.opt(exception=True).critical(f'Не удалось очистить broker: {error}')
            is_error = True

        if is_error:
            raise ValueError

        return None

    async def _start_trigger_manager(self) -> None:
        self._trigger_manager.start()

    async def _join_trigger_manager(self) -> None:
        self._trigger_manager.join()

    async def _run_supervisors(self, backend: AbstractBackend) -> None:
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

        SignalHandler(self._shutdown_event, self._trigger_manager.cancel).register()
        logger.debug("Менеджер процессов зарегистрировал обработчик сигналов")

        try:
            async with self._backend_factory() as backend, self._broker_factory() as broker:
                logger.debug('Менеджер процессов инициализировал backend / broker')

                await self._startup(backend, broker)
                logger.debug('Менеджер процессов выполнил startup')

                await self._start_trigger_manager()
                logger.debug(f'Менеджер процессов запустил менеджер триггеров')

                await self._run_supervisors(backend)
                logger.debug('Супервайзеры процессов завершены')

                await self._join_trigger_manager()
                logger.debug(f'Менеджер процессов дождался закрытия менеджера триггеров')

                await self._shutdown(backend, broker)
                logger.debug('Менеджер процессов выполнил shutdown')
        except ValueError:
            pass
        except Exception as error:
            logger.opt(exception=True).critical(f'Менеджеру процессов не удалось открыть или закрыть backend / broker: {error}')

        logger.info('Менеджер процессов завершил работу')
