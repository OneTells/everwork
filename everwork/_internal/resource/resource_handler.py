import asyncio
from contextlib import suppress
from typing import Any, Literal

from loguru import logger

from everwork._internal.utils.task_utils import OperationCancelled, wait_for_or_cancel
from everwork._internal.worker.utils.executor_channel import ExecutorTransmitter
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
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

    async def _get_worker_status(self) -> Literal['on', 'off']:
        try:
            return await wait_for_or_cancel(
                self._backend.get_worker_status(self._manager_uuid, self._worker.settings.name),
                self._shutdown_event,
                timeout=5
            )
        except (OperationCancelled, asyncio.TimeoutError):
            logger.debug(f'[{self._process.uuid}] ({self._worker.settings.name}) Обработчик ресурсов прервал get_worker_status')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] ({self._worker.settings.name}) Не удалось получить статус воркера: {error}'
            )

        return 'off'

    async def _fetch_event(self) -> tuple[dict[str, Any], Any]:
        try:
            return await wait_for_or_cancel(
                self._broker.fetch_event(
                    self._manager_uuid,
                    self._process.uuid,
                    self._worker.settings.name,
                    self._worker.settings.source_streams
                ),
                self._shutdown_event
            )
        except OperationCancelled:
            logger.debug(f'[{self._process.uuid}] ({self._worker.settings.name}) Обработчик ресурсов прервал fetch_event')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] ({self._worker.settings.name}) Не удалось получить ивент: {error}'
            )

        raise ValueError

    async def _ack_event(self, event_identifier: Any) -> None:
        try:
            return await asyncio.wait_for(
                self._broker.ack_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, event_identifier),
                timeout=5
            )
        except asyncio.TimeoutError:
            logger.error(f'[{self._process.uuid}] ({self._worker.settings.name}) Обработчик ресурсов прервал ack_event')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] ({self._worker.settings.name}) Не удалось подтвердить ивент: {error}'
            )

    async def _reject_event(self, event_identifier: Any, error: BaseException) -> None:
        try:
            return await asyncio.wait_for(
                self._broker.reject_event(
                    self._manager_uuid, self._process.uuid, self._worker.settings.name, event_identifier, error
                ),
                timeout=5
            )
        except asyncio.TimeoutError:
            logger.error(f'[{self._process.uuid}] ({self._worker.settings.name}) Обработчик ресурсов прервал reject_event')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] ({self._worker.settings.name}) Не удалось отклонить ивент: {error}'
            )

    async def _requeue_event(self, event_identifier: Any) -> None:
        try:
            return await asyncio.wait_for(
                self._broker.requeue_event(self._manager_uuid, self._process.uuid, self._worker.settings.name, event_identifier),
                timeout=5
            )
        except asyncio.TimeoutError:
            logger.error(f'[{self._process.uuid}] ({self._worker.settings.name}) Обработчик ресурсов прервал requeue_event')
        except Exception as error:
            logger.opt(exception=True).critical(
                f'[{self._process.uuid}] ({self._worker.settings.name}) Не удалось вернуть ивент: {error}'
            )

    async def _run_event_processing_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._get_worker_status() == 'off':
                with suppress(OperationCancelled):
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker.settings.worker_status_check_interval),
                        self._shutdown_event
                    )

                continue

            try:
                kwargs, event_identifier = await self._fetch_event()
            except ValueError:
                continue

            if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                await self._requeue_event(event_identifier)
                continue

            async with self._lock:
                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    await self._requeue_event(event_identifier)
                    continue

                error_answer = await self._transmitter.execute(self._worker.settings.name, kwargs)

                if error_answer is not None:
                    await self._reject_event(event_identifier, error_answer)
                    continue

                await self._ack_event(event_identifier)

    async def run(self) -> None:
        logger.debug(f'({self._worker.settings.name}) Обработчик ресурсов запущен')

        await self._run_event_processing_loop()

        logger.debug(f'({self._worker.settings.name}) Обработчик ресурсов завершил работ')
