import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, UTC
from typing import Any, Callable, Coroutine, Literal

from loguru import logger
from pydantic import AwareDatetime

from everwork._internal.backend import AbstractBackend
from everwork._internal.broker import AbstractBroker
from everwork._internal.utils.async_task import OperationCancelled, wait_for_or_cancel
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

    async def _execute_with_graceful_cancel[T](self, coroutine: Coroutine[Any, Any, T], min_timeout: int = 0) -> T:
        try:
            return await wait_for_or_cancel(coroutine, self._shutdown_event, min_timeout)
        except OperationCancelled:
            logger.error(
                f"({self._worker_settings.id}) |{self._trigger.id}| "
                f"Обработчик триггера прервал '{coroutine.__name__}'"
            )
            raise
        except Exception as error:
            logger.opt(exception=True).critical(
                f"({self._worker_settings.id}) |{self._trigger.id}| "
                f"Обработчику триггера не удалось выполнить '{coroutine.__name__}': {error}"
            )
            raise

    async def _get_worker_status(self) -> Literal['on', 'off']:
        with suppress(Exception):
            return await self._execute_with_graceful_cancel(
                self._backend.get_worker_status(
                    self._worker_settings.id,
                    ttl=self._worker_settings.status_cache_ttl
                ),
                min_timeout=5
            )

        return 'off'

    async def _get_trigger_status(self) -> Literal['on', 'off']:
        with suppress(Exception):
            return await self._execute_with_graceful_cancel(
                self._backend.get_trigger_status(
                    self._worker_settings.id,
                    self._trigger.id,
                    ttl=self._trigger.status_cache_ttl
                ),
                min_timeout=5
            )

        return 'off'

    async def _get_last_time_point(self) -> AwareDatetime | None:
        with suppress(Exception):
            return await self._execute_with_graceful_cancel(
                self._backend.get_time_point(self._worker_settings.id, self._trigger.id),
                min_timeout=5
            )

        return None

    async def _set_last_time_point(self, time_point: AwareDatetime) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._backend.set_time_point(self._worker_settings.id, self._trigger.id, time_point),
                min_timeout=5
            )

    async def _push_event(self, event: Event) -> None:
        with suppress(Exception):
            await self._execute_with_graceful_cancel(
                self._broker.push(event),
                min_timeout=5
            )

    async def run(self) -> None:
        logger.debug(f"({self._worker_settings.id}) |{self._trigger.id}| Обработчик триггера запущен")

        with suppress(OperationCancelled):
            time_point = await self._get_last_time_point()

            if time_point is None:
                time_point = datetime.now(UTC)

            while not self._shutdown_event.is_set():
                logger.debug(f'1')
                if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                    logger.debug(f'1.1')
                    await wait_for_or_cancel(
                        asyncio.sleep(self._worker_settings.status_check_interval),
                        self._shutdown_event
                    )

                    continue
                logger.debug(f'2')
                if self._shutdown_event.is_set() or (await self._get_trigger_status() == 'off'):
                    logger.debug(f'2.1')
                    await wait_for_or_cancel(
                        asyncio.sleep(self._trigger.status_check_interval),
                        self._shutdown_event
                    )

                    continue
                logger.debug(f'3')
                time_point = self._time_point_generator(time_point)

                if time_point >= datetime.now(UTC):
                    await wait_for_or_cancel(
                        asyncio.sleep((time_point - datetime.now(UTC)).total_seconds()),
                        self._shutdown_event
                    )

                    if self._shutdown_event.is_set() or (await self._get_worker_status() == 'off'):
                        continue

                    if self._shutdown_event.is_set() or (await self._get_trigger_status() == 'off'):
                        continue
                elif self._trigger.is_catchup:
                    continue

                await self._set_last_time_point(time_point)
                await self._push_event(
                    Event(
                        source=self._worker_settings.default_source,
                        kwargs={'time_point': time_point, 'trigger_id': self._trigger.id} | self._trigger.kwargs,
                        expires=datetime.now(UTC) + timedelta(seconds=self._trigger.lifetime) if self._trigger.lifetime else None
                    )
                )

        logger.debug(f"({self._worker_settings.id}) |{self._trigger.id}| Обработчик триггера завершил работу")
