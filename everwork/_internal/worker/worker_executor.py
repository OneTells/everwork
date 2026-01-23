import asyncio
from typing import Any

from loguru import logger

from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork._internal.worker.utils.executor_channel import ChannelClosed, ExecutorReceiver
from everwork._internal.worker.utils.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.events import EventCollector, EventPublisher, HybridEventStorage
from everwork.schemas import Process
from everwork.workers import AbstractWorker


class WorkerExecutor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        worker_registry: WorkerRegistry,
        receiver: ExecutorReceiver,
        backend: AbstractBackend,
        broker: AbstractBroker,
        storage: HybridEventStorage,
        notifier: HeartbeatNotifier,
        is_executing_event: asyncio.Event,
        shutdown_event: asyncio.Event,
        terminate_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._worker_registry = worker_registry
        self._receiver = receiver
        self._backend = backend
        self._storage = storage
        self._notifier = notifier
        self._is_executing_event = is_executing_event
        self._shutdown_event = shutdown_event
        self._terminate_event = terminate_event

        self._collector = EventCollector(storage)
        self._publisher = EventPublisher(storage, broker)

    def _prepare_worker_kwargs(self, worker: AbstractWorker, kwargs_raw: dict[str, Any]) -> dict[str, Any]:
        worker_params = self._worker_registry.get_worker_params(worker.settings.name)

        kwargs = {k: v for k, v in kwargs_raw.items() if k in worker_params}

        if 'collector' in worker_params:
            self._storage.max_events_in_memory = worker.settings.event_storage.max_events_in_memory
            kwargs['collector'] = self._collector

        return kwargs

    async def _mark_worker_executor_as_busy(self, worker_name: str) -> None:
        try:
            await wait_for_or_cancel(
                self._backend.mark_worker_executor_as_busy(self._manager_uuid, self._process.uuid, worker_name),
                self._terminate_event,
                timeout=5
            )
        except (OperationCancelled, asyncio.TimeoutError):
            logger.debug(f'[{self._process.uuid}] ({worker_name}) Исполнитель воркеров прервал mark_worker_executor_as_busy')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] ({worker_name}) Не удалось установить метку занятости исполнителя: {error}'
            )

    async def _mark_worker_executor_as_available(self) -> None:
        try:
            await wait_for_or_cancel(
                self._backend.mark_worker_executor_as_available(self._manager_uuid, self._process.uuid),
                self._terminate_event,
                timeout=5
            )
        except (OperationCancelled, asyncio.TimeoutError):
            logger.debug(f'[{self._process.uuid}] Исполнитель воркеров прервал mark_worker_executor_as_available')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] Не удалось установить метку доступности исполнителя: {error}'
            )

    async def _execute(self, worker: AbstractWorker, kwargs: dict[str, Any]) -> BaseException | None:
        self._notifier.notify_started(worker.settings)

        error_answer: BaseException | None = None

        try:
            self._is_executing_event.set()
            await worker.__call__(**kwargs)
        except (KeyboardInterrupt, asyncio.CancelledError) as error:
            logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Выполнение прервано по таймауту')
            error_answer = error
        except Exception as error:
            logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Ошибка при обработке события: {error}')
            error_answer = error
        finally:
            self._is_executing_event.clear()
            self._notifier.notify_completed()

        if error_answer is None:
            try:
                await wait_for_or_cancel(
                    self._publisher.push_events(worker.settings.event_publisher.max_batch_size),
                    self._terminate_event
                )
            except OperationCancelled as error:
                error_answer = error
                logger.debug(f'[{self._process.uuid}] ({worker.settings.name}) Исполнитель воркеров прервал push_events')
            except Exception as error:
                error_answer = error
                logger.opt(exception=True).critical(
                    f'[{self._process.uuid}] ({worker.settings.name}) Не удалось сохранить ивенты: {error}'
                )

        return error_answer

    async def _process_task(self) -> bool:
        try:
            worker_name, kwargs_raw = await self._receiver.get_response()
        except ChannelClosed:
            return False

        worker = self._worker_registry.get_worker(worker_name)
        kwargs = self._prepare_worker_kwargs(worker, kwargs_raw)

        await self._mark_worker_executor_as_busy(worker_name)

        error_answer = await self._execute(worker, kwargs)
        self._receiver.send_answer(error_answer)

        await self._mark_worker_executor_as_available()

        return True

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Исполнитель воркеров запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        await self._mark_worker_executor_as_available()
        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров стал доступным')

        while await self._process_task():
            await self._storage.clear()

        logger.debug(f'[{self._process.uuid}] Исполнитель воркеров завершил работу')
