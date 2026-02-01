import asyncio
from typing import Any

from loguru import logger

from everwork._internal.utils.event_storage import AbstractReader, HybridStorage
from everwork._internal.worker.utils.event_collector import EventCollector
from everwork._internal.worker.utils.executor_channel import ChannelClosed, ExecutorReceiver
from everwork._internal.worker.utils.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork.schemas import Event, Process
from everwork.workers import AbstractWorker


class WorkerExecutor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        receiver: ExecutorReceiver,
        notifier: HeartbeatNotifier,
        is_executing_event: asyncio.Event,
        storage: HybridStorage
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._receiver = receiver
        self._notifier = notifier
        self._is_executing_event = is_executing_event

        self._storage = storage
        self._collector = EventCollector(storage)

        self._worker_registry = WorkerRegistry(process)

    async def _startup(self) -> None:
        await self._worker_registry.initialize()
        await self._worker_registry.startup_all()

    async def _shutdown(self) -> None:
        await self._worker_registry.shutdown_all()

    def _prepare_kwargs(self, worker: AbstractWorker, event: Event) -> dict[str, Any]:
        filtered_kwargs, reserved_kwargs, default_kwargs = (
            self._worker_registry
            .get_resolver(worker.settings.slug)
            .get_kwargs(
                event.kwargs,
                {
                    'collector': self._collector,
                    'event': event
                }
            )
        )

        if 'collector' in reserved_kwargs.keys():
            self._storage.max_events_in_memory = worker.settings.event_settings.max_events_in_memory

        return filtered_kwargs | reserved_kwargs | default_kwargs

    async def _execute(self, worker: AbstractWorker, kwargs: dict[str, Any]) -> AbstractReader | BaseException:
        self._notifier.notify_started(worker.settings)

        try:
            self._is_executing_event.set()
            await worker.__call__(**kwargs)
        except (KeyboardInterrupt, asyncio.CancelledError) as error:
            logger.exception(f'[{self._process.uuid}] ({worker.settings.slug}) Выполнение прервано по таймауту')
            return error
        except Exception as error:
            logger.exception(f'[{self._process.uuid}] ({worker.settings.slug}) Ошибка при обработке события: {error}')
            return error
        finally:
            self._is_executing_event.clear()
            self._notifier.notify_completed()

        return self._storage.export()

    async def _run_execute_loop(self) -> None:
        while True:
            try:
                worker_name, event = await self._receiver.get_response()
            except ChannelClosed:
                break

            worker = self._worker_registry.get_worker(worker_name)

            try:
                kwargs = self._prepare_kwargs(worker, event)
            except TypeError as error:
                answer = error
            else:
                answer = await self._execute(worker, kwargs)

            self._receiver.send_answer(answer)

            self._storage.recreate()

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Исполнитель воркеров запущен. '
            f'Состав: {', '.join(worker.settings.slug for worker in self._process.workers)}'
        )

        await self._startup()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров выполнил startup')

        await self._run_execute_loop()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил цикл обработки')

        await self._shutdown()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров выполнил shutdown')

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил работу')
