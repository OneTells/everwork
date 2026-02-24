import asyncio
from typing import Callable, Sequence

from loguru import logger

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.trigger.trigger_handler import TriggerHandler
from everwork.schemas.process import Process
from everwork.utils import AbstractCronSchedule


class TriggerManager:

    def __init__(
        self,
        manager_uuid: str,
        processes: Sequence[Process],
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        cron_schedule: type[AbstractCronSchedule],
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._processes = processes
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._cron_schedule = cron_schedule
        self._shutdown_event = shutdown_event

    async def _run_handlers(self, backend: AbstractBackend, broker: AbstractBroker) -> None:
        async with asyncio.TaskGroup() as task_group:
            for worker_settings in {w.settings.id: w.settings for p in self._processes for w in p.workers}.values():
                for trigger in worker_settings.triggers:
                    handler = TriggerHandler(
                        self._manager_uuid,
                        worker_settings,
                        trigger,
                        backend,
                        broker,
                        self._cron_schedule,
                        self._shutdown_event
                    )

                    task_group.create_task(handler.run())

    async def run(self) -> None:
        try:
            async with self._backend_factory() as backend, self._broker_factory() as broker:
                await self._run_handlers(backend, broker)
        except Exception as error:
            logger.opt(exception=True).critical(f'Не удалось открыть или закрыть backend / broker: {error}')
