import asyncio
from typing import Any

from loguru import logger

from everwork._internal.worker.heartbeat_notifier import HeartbeatNotifier
from everwork.events import EventPublisher
from everwork.schemas import Process
from everwork.workers import AbstractWorker


class WorkerInvoker:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        notifier: HeartbeatNotifier,
        publisher: EventPublisher,
        is_executing_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process

        self._notifier = notifier
        self._publisher = publisher

        self._is_executing = is_executing_event

    async def execute(self, worker: AbstractWorker, kwargs: dict[str, Any]) -> BaseException | None:
        self._notifier.notify_started(worker.settings)

        error_answer: BaseException | None = None

        try:
            self._is_executing.set()
            await worker.__call__(**kwargs)
        except (KeyboardInterrupt, asyncio.CancelledError) as error:
            logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Выполнение прервано по таймауту')
            error_answer = error
        except Exception as error:
            logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Ошибка при обработке события: {error}')
            error_answer = error
        finally:
            self._is_executing.clear()
            self._notifier.notify_completed()

        if error_answer is None:
            await self._publisher.push_events(worker.settings.event_publisher.max_batch_size)

        return error_answer
