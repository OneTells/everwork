import asyncio
from contextlib import suppress
from datetime import datetime, UTC
from typing import Any, Coroutine, Literal

from loguru import logger

from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.utils.event_storage import AbstractReader
from everwork._internal.worker.utils.executor_channel import ExecutorTransmitter
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import EventPayload, Process
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
            logger.debug(
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

    async def _mark_worker_executor_as_busy(self, event_id: str) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._backend.mark_worker_executor_as_busy(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    event_id
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

    async def _fetch_event(self) -> tuple[str, EventPayload]:
        with suppress(Exception):
            return await self._execute_with_graceful_cancel(
                self._broker.fetch_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    self._worker.settings.sources
                ),
                min_timeout=5
            )

        raise ValueError

    async def _ack_event(self, event_id: str) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.ack_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    event_id
                ),
                min_timeout=5
            )

    async def _fail_event(self, event_id: str, error_answer: BaseException) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.fail_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    event_id,
                    error_answer
                ),
                min_timeout=5
            )

    async def _return_event(self, event_id: str) -> None:
        logger.debug(f'[{self._process.uuid}] ({self._worker.settings.slug}) Ивент будет возвращен, event_id={event_id}')

        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.return_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    event_id
                ),
                min_timeout=5
            )

    async def _reject_event(self, event_id: str) -> None:
        logger.debug(f'[{self._process.uuid}] ({self._worker.settings.slug}) Ивент будет отменён, event_id={event_id}')

        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.reject_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.slug,
                    event_id
                ),
                min_timeout=5
            )

    async def _push_events(self, event_id: str, reader: AbstractReader) -> BaseException | None:
        try:
            batch = []

            for event in reader:
                batch.append(event)

                if len(batch) >= self._worker.settings.event_settings.max_batch_size:
                    await self._execute_with_graceful_cancel(self._broker.push_event(batch), min_timeout=5)
                    batch.clear()

            if batch:
                await self._execute_with_graceful_cancel(self._broker.push_event(batch), min_timeout=5)
                batch.clear()
        except Exception as error:
            logger.exception(
                f'[{self._process.uuid}] ({self._worker.settings.slug}) '
                f'Не удалось сохранить ивенты, event_id={event_id}: {error}'
            )
            return error

        return None

    async def _run_event_processing_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._get_worker_status() == 'off':
                with suppress(OperationCancelled):
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker.settings.worker_status_check_interval), self._shutdown_event
                    )

                continue

            try:
                event_id, event_payload = await self._fetch_event()
            except ValueError:
                continue

            if event_payload.expires is not None and event_payload.expires < datetime.now(UTC):
                await self._reject_event(event_id)
                continue

            if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                await self._return_event(event_id)
                continue

            async with self._lock:
                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    await self._return_event(event_id)
                    continue

                await self._mark_worker_executor_as_busy(event_id)

                reader_or_error = await self._transmitter.execute(self._worker.settings.slug, event_payload)

                if isinstance(reader_or_error, AbstractReader):
                    error = await self._push_events(event_id, reader_or_error)
                    reader_or_error.close()
                else:
                    error = reader_or_error

                await self._mark_worker_executor_as_available()

            if error is not None:
                await self._fail_event(event_id, error)
                continue

            await self._ack_event(event_id)

    async def run(self) -> None:
        logger.debug(f'[{self._process.uuid}] ({self._worker.settings.slug}) Обработчик ресурсов запущен')

        await self._run_event_processing_loop()

        logger.debug(f'[{self._process.uuid}] ({self._worker.settings.slug}) Обработчик ресурсов завершил работу')
