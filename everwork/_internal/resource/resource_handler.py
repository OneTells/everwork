import asyncio
from contextlib import suppress
from datetime import datetime, UTC
from typing import Literal

from loguru import logger

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.schemas import AckResponse, FailResponse, RejectResponse, Request, RetryResponse
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.utils.caller import call
from everwork._internal.worker.utils.executor_channel import ExecutorTransmitter
from everwork.schemas.event import Event
from everwork.schemas.process import Process
from everwork.workers.base import AbstractWorker


class ResourceHandler:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        worker: type[AbstractWorker],
        backend: AbstractBackend,
        broker: AbstractBroker,
        transmitter: ExecutorTransmitter,
        lock: asyncio.Lock,
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._worker = worker
        self._backend = backend
        self._broker = broker
        self._transmitter = transmitter
        self._lock = lock
        self._shutdown_event = shutdown_event

    async def _mark_worker_executor_as_available(self) -> None:
        await (
            call(self._backend.mark_worker_executor_as_available, self._manager_uuid, self._process.uuid)
            .retry(retries=2)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None)
        )

    async def _mark_worker_executor_as_busy(self, request: Request) -> None:
        await (
            call(
                self._backend.mark_worker_executor_as_busy,
                self._manager_uuid,
                self._process.uuid,
                self._worker.settings.id,
                request.event_id
            )
            .retry(retries=2)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None)
        )

    async def _get_worker_status(self) -> Literal['on', 'off']:
        return await (
            call(self._backend.get_worker_status, self._worker.settings.id)
            .retry(retries=3)
            .cache(ttl=self._worker.settings.status_cache_ttl)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return='off', on_timeout_return='off')
        )

    async def _fetch(self) -> Request | None:
        return await (
            call(
                self._broker.fetch,
                self._process.uuid,
                self._worker.settings.id,
                self._worker.settings.sources
            )
            .retry(retries=None)
            .wait_for_or_cancel(self._shutdown_event, max_timeout=None)
            .execute(on_error_return=None, on_timeout_return=None, log_cancellation=False)
        )

    async def _push_events(self, events: list[Event]) -> None:
        await (
            call(self._broker.push, events)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=ValueError, on_timeout_return=ValueError)
        )

    async def _push(self, request: Request, response: AckResponse) -> AckResponse | FailResponse:
        try:
            batch = []

            for event in response.reader:
                batch.append(event)

                if len(batch) >= self._worker.settings.event_settings.max_batch_size:
                    await self._push_events(batch)
                    batch.clear()

            if batch:
                await self._push_events(batch)
        except ValueError as error:
            logger.exception(f'({self._worker.settings.id}) Не удалось сохранить ивенты. Ивент: {request.event_id}')
            return FailResponse(detail='Не удалось сохранить ивенты', error=error)
        finally:
            response.reader.close()

        return response

    async def _ack(self, request: Request, response: AckResponse) -> None:
        await (
            call(self._broker.ack, self._worker.settings.id, request, response)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None)
        )

    async def _fail(self, request: Request, response: FailResponse) -> None:
        await (
            call(self._broker.fail, self._worker.settings.id, request, response)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None)
        )

    async def _reject(self, request: Request, response: RejectResponse) -> None:
        await (
            call(self._broker.reject, self._worker.settings.id, request, response)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None)
        )

    async def _retry(self, request: Request, response: RetryResponse) -> None:
        await (
            call(self._broker.retry, self._worker.settings.id, request, response)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None)
        )

    async def _run_event_processing_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._get_worker_status() == 'off':
                await wait_for_or_cancel(
                    asyncio.sleep(self._worker.settings.status_check_interval),
                    self._shutdown_event
                )

                continue

            request = await self._fetch()

            if request is None:
                continue

            if request.event.expires is not None and request.event.expires < datetime.now(UTC):
                response = RejectResponse()
                await self._reject(request, response)

                del request, response
                continue

            if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                response = RetryResponse()
                await self._retry(request, response)

                del request, response
                continue

            async with self._lock:
                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    response = RetryResponse()
                    await self._retry(request, response)

                    del request, response
                    continue

                await self._mark_worker_executor_as_busy(request)

                response = await self._transmitter.execute(self._worker.settings.id, request)

                await self._mark_worker_executor_as_available()

            if isinstance(response, AckResponse):
                # noinspection PyUnboundLocalVariable
                response = await self._push(request, response)

            if isinstance(response, AckResponse):
                await self._ack(request, response)
            elif isinstance(response, FailResponse):
                await self._fail(request, response)
            elif isinstance(response, RejectResponse):
                await self._reject(request, response)
            elif isinstance(response, RetryResponse):
                await self._retry(request, response)

            del request, response

    async def run(self) -> None:
        with suppress(OperationCancelled):
            await self._run_event_processing_loop()
