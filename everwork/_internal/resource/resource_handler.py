import asyncio
from contextlib import suppress
from datetime import datetime, UTC
from typing import Any, Coroutine, Literal

from loguru import logger

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.schemas import AckResponse, FailResponse, RejectResponse, Request, RetryResponse
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.worker.utils.executor_channel import ExecutorTransmitter
from everwork.schemas import Process
from everwork.workers import AbstractWorker


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

    async def _execute_with_graceful_cancel[T](self, coroutine: Coroutine[Any, Any, T], min_timeout: int = 0) -> T:
        try:
            return await wait_for_or_cancel(coroutine, self._shutdown_event, min_timeout)
        except OperationCancelled:
            logger.error(
                f"[{self._process.uuid}] ({self._worker.settings.slug}) "
                f"Обработчик ресурсов прервал '{coroutine.__name__}'"
            )
            raise
        except Exception as error:
            logger.opt(exception=True).critical(
                f"[{self._process.uuid}] ({self._worker.settings.slug}) "
                f"Не удалось выполнить '{coroutine.__name__}': {error}"
            )
            raise

    async def _mark_worker_executor_as_available(self) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._backend.mark_worker_executor_as_available(
                    self._manager_uuid,
                    self._process.uuid
                ),
                min_timeout=5
            )

    async def _mark_worker_executor_as_busy(self, request: Request) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._backend.mark_worker_executor_as_busy(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    request.event_id
                ),
                min_timeout=5
            )

    async def _get_worker_status(self) -> Literal['on', 'off']:
        with suppress(Exception):
            return await self._execute_with_graceful_cancel(
                self._backend.get_worker_status(
                    self._manager_uuid,
                    self._worker.settings.slug
                ),
                min_timeout=5
            )

        return 'off'

    async def _fetch(self) -> Request:
        with suppress(Exception):
            return await self._execute_with_graceful_cancel(
                self._broker.fetch(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    self._worker.settings.sources
                ),
                min_timeout=5
            )

        raise ValueError

    async def _push_events(self, request: Request, response: AckResponse) -> AckResponse | FailResponse:
        try:
            batch = []

            for event in response.reader:
                batch.append(event)

                if len(batch) >= self._worker.settings.event_settings.max_batch_size:
                    await self._execute_with_graceful_cancel(self._broker.push(batch), min_timeout=5)
                    batch.clear()

            if batch:
                await self._execute_with_graceful_cancel(self._broker.push(batch), min_timeout=5)
                batch.clear()
        except Exception as error:
            logger.exception(
                f'[{self._process.uuid}] ({self._worker.settings.slug}) '
                f'Не удалось сохранить ивенты, event_id={request.event_id}: {error}'
            )
            return FailResponse(detail='Не удалось сохранить ивенты', error=error)
        finally:
            response.reader.close()

        return response

    async def _ack(self, request: Request, response: AckResponse) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.ack(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    request,
                    response
                ),
                min_timeout=5
            )

    async def _fail(self, request: Request, response: FailResponse) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.fail(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    request,
                    response
                ),
                min_timeout=5
            )

    async def _reject(self, request: Request, response: RejectResponse) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.reject(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    request,
                    response
                ),
                min_timeout=5
            )

    async def _retry(self, request: Request, response: RetryResponse) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.retry(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    request,
                    response
                ),
                min_timeout=5
            )

    async def _run_event_processing_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._get_worker_status() == 'off':
                with suppress(OperationCancelled):
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker.settings.worker_status_check_interval), self._shutdown_event
                    )

                continue

            try:
                request = await self._fetch()
            except ValueError:
                continue

            if request.event.expires is not None and request.event.expires < datetime.now(UTC):
                response = RejectResponse(detail='Ивент отклонён из-за истёкшего срока действия')
                await self._reject(request, response)
                continue

            if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                response = RetryResponse()
                await self._retry(request, response)
                continue

            async with self._lock:
                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    response = RetryResponse()
                    await self._retry(request, response)
                    continue

                await self._mark_worker_executor_as_busy(request)

                response = await self._transmitter.execute(self._worker.settings.slug, request)

                await self._mark_worker_executor_as_available()

            if isinstance(response, AckResponse):
                response = await self._push_events(request, response)

            if isinstance(response, AckResponse):
                await self._ack(request, response)
            elif isinstance(response, FailResponse):
                await self._fail(request, response)
            elif isinstance(response, RejectResponse):
                await self._reject(request, response)
            elif isinstance(response, RetryResponse):
                await self._retry(request, response)

    async def run(self) -> None:
        logger.debug(f'[{self._process.uuid}] ({self._worker.settings.slug}) Обработчик ресурсов запущен')

        await self._run_event_processing_loop()

        logger.debug(f'[{self._process.uuid}] ({self._worker.settings.slug}) Обработчик ресурсов завершил работу')
