import asyncio
from contextlib import suppress
from typing import Any, Literal

from loguru import logger

from everwork._internal.utils.single_value_channel import SingleValueChannel
from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process
from everwork.workers import AbstractWorker


class ResourceSupervisor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        worker: type[AbstractWorker],
        backend: AbstractBackend,
        broker: AbstractBroker,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[BaseException | None],
        lock: asyncio.Lock,
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._worker = worker
        self._backend = backend
        self._broker = broker
        self._response_channel = response_channel
        self._answer_channel = answer_channel
        self._lock = lock
        self._shutdown_event = shutdown_event

    async def _get_worker_status(self) -> Literal['on', 'off']:
        return await self._backend.get_worker_status(self._manager_uuid, self._worker.settings.name)

    async def _ack_event(self, event_identifier: Any) -> None:
        await self._broker.ack_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, event_identifier)

    async def _reject_event(self, event_identifier: Any, error: BaseException) -> None:
        await self._broker.reject_event(
            self._manager_uuid, self._process.uuid, self._worker.settings.name, event_identifier, error
        )

    async def _requeue_event(self, event_identifier: Any) -> None:
        await self._broker.requeue_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, event_identifier)

    async def _process_worker_messages(self) -> None:
        with suppress(OperationCancelled):
            while not self._shutdown_event.is_set():
                if await self._get_worker_status() == 'off':
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker.settings.worker_status_check_interval),
                        self._shutdown_event
                    )
                    continue

                kwargs, event_identifier = await self._broker.fetch_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.name,
                    self._worker.settings.source_streams
                )

                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    await self._requeue_event(event_identifier)
                    continue

                async with self._lock:
                    if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                        await self._requeue_event(event_identifier)
                        continue

                    self._response_channel.send((self._worker.settings.name, kwargs))
                    error_answer = await self._answer_channel.receive()

                    if error_answer is not None:
                        await self._reject_event(event_identifier, error_answer)
                        continue

                    await self._ack_event(event_identifier)

    async def run(self) -> None:
        logger.debug(f'({self._worker.settings.name}) Супервайзер ресурса запущен')

        await self._process_worker_messages()

        logger.debug(f'({self._worker.settings.name}) Супервайзер ресурса завершил работ')
