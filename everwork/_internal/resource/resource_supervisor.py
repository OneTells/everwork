import asyncio
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

    async def _process_worker_messages(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._broker.get_worker_status(self._manager_uuid, self._worker.settings.name) == 'off':
                try:
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker.settings.worker_status_check_interval),
                        self._shutdown_event
                    )
                except OperationCancelled:
                    break

                continue

            try:
                kwargs, resource = await wait_for_or_cancel(
                    self._broker.fetch_event(
                        self._manager_uuid,
                        self._process.uuid,
                        self._worker.settings.name,
                        self._worker.settings.source_streams
                    ),
                    self._shutdown_event
                )
            except OperationCancelled:
                continue

            if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                await self._broker.requeue_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, resource)
                continue

            async with self._lock:
                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    await self._broker.requeue_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, resource)
                    continue

                self._response_channel.send((self._worker.settings.name, kwargs))
                error_answer = await self._answer_channel.receive()

                if error_answer is None:
                    await self._broker.ack_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, resource)
                    continue

                await self._broker.reject_event(
                    self._manager_uuid, self._process.uuid, self._worker.settings.name, resource, kwargs, error_answer
                )

    async def run(self) -> None:
        logger.debug(f'({self._worker.settings.name}) Запущен наблюдатель ресурсов')

        try:
            await self._process_worker_messages()
        except Exception as error:
            (
                logger
                .opt(exception=True)
                .critical(
                    f'[{self._process.uuid}] ({self._worker.settings.name}) '
                    f'Наблюдатель ресурсов завершился с ошибкой: {error}'
                )
            )

        logger.debug(f'({self._worker.settings.name}) Наблюдатель ресурсов завершил работ')
