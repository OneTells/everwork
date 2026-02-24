import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, UTC
from typing import Callable, Literal

from pydantic import AwareDatetime

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
from everwork._internal.utils.caller import call
from everwork.schemas import Cron, Event, Interval, Trigger, WorkerSettings
from everwork.utils import AbstractCronSchedule


def _get_time_point_generator(
    trigger: Trigger,
    cron_schedule: type[AbstractCronSchedule]
) -> Callable[[AwareDatetime], AwareDatetime]:
    if isinstance(trigger.schedule, Interval):
        interval = timedelta(**trigger.schedule.model_dump())
        return lambda x: x + interval
    elif isinstance(trigger.schedule, Cron):
        cron = cron_schedule(trigger.schedule.expression)
        return lambda x: cron.get_next(x)
    else:
        raise ValueError(f"Неизвестный тип графика триггеров: {type(trigger.schedule)}")


class TriggerHandler:

    def __init__(
        self,
        manager_uuid: str,
        worker_settings: WorkerSettings,
        trigger: Trigger,
        backend: AbstractBackend,
        broker: AbstractBroker,
        cron_schedule: type[AbstractCronSchedule],
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._worker_settings = worker_settings
        self._trigger = trigger
        self._backend = backend
        self._broker = broker
        self._cron_schedule = cron_schedule
        self._shutdown_event = shutdown_event

        self._time_point_generator = _get_time_point_generator(self._trigger, self._cron_schedule)

        self._log_context = f'({self._worker_settings.id}) |{self._trigger.id}| Обработчик триггера'

    async def _get_worker_status(self) -> Literal['on', 'off']:
        return await (
            call(self._backend.get_worker_status, self._worker_settings.id)
            .retry(retries=3)
            .cache(ttl=self._worker_settings.status_cache_ttl)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return='off', on_timeout_return='off', log_context=self._log_context)
        )

    async def _get_trigger_status(self) -> Literal['on', 'off']:
        return await (
            call(self._backend.get_trigger_status, self._worker_settings.id, self._trigger.id)
            .retry(retries=3)
            .cache(ttl=self._trigger.status_cache_ttl)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return='off', on_timeout_return='off', log_context=self._log_context)
        )

    async def _get_last_time_point(self) -> AwareDatetime | None:
        return await (
            call(self._backend.get_time_point, self._worker_settings.id, self._trigger.id)
            .retry(retries=None)
            .wait_for_or_cancel(self._shutdown_event, max_timeout=None)
            .execute(on_error_return=None, on_timeout_return=None, log_context=self._log_context)
        )

    async def _set_last_time_point(self, time_point: AwareDatetime) -> None:
        return await (
            call(self._backend.set_time_point, self._worker_settings.id, self._trigger.id, time_point)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=ValueError, on_timeout_return=ValueError, log_context=self._log_context)
        )

    async def _push_event(self, event: Event) -> None:
        return await (
            call(self._broker.push, event)
            .retry(retries=3)
            .wait_for_or_cancel(self._shutdown_event)
            .execute(on_error_return=None, on_timeout_return=None, log_context=self._log_context)
        )

    async def _run_loop(self, time_point: datetime) -> None:
        while not self._shutdown_event.is_set():
            if await self._get_worker_status() == 'off':
                await wait_for_or_cancel(
                    asyncio.sleep(self._worker_settings.status_check_interval),
                    self._shutdown_event
                )

                continue

            if await self._get_trigger_status() == 'off':
                await wait_for_or_cancel(
                    asyncio.sleep(self._trigger.status_check_interval),
                    self._shutdown_event
                )

                continue

            time_point = self._time_point_generator(time_point)

            if time_point >= datetime.now(UTC):
                await wait_for_or_cancel(
                    asyncio.sleep((time_point - datetime.now(UTC)).total_seconds()),
                    self._shutdown_event
                )

                if (await self._get_worker_status() == 'off') or (await self._get_trigger_status() == 'off'):
                    continue
            elif self._trigger.is_catchup:
                continue

            if self._shutdown_event.is_set():
                break

            try:
                await self._set_last_time_point(time_point)
            except ValueError:
                continue

            await self._push_event(
                Event(
                    source=self._worker_settings.default_source,
                    kwargs={'time_point': time_point, 'trigger_id': self._trigger.id} | self._trigger.kwargs,
                    expires=datetime.now(UTC) + timedelta(seconds=self._trigger.lifetime) if self._trigger.lifetime else None
                )
            )

    async def run(self) -> None:
        with suppress(OperationCancelled):
            time_point = await self._get_last_time_point()

            if time_point is None:
                time_point = datetime.now(UTC)

            await self._run_loop(time_point)
