import asyncio
import signal
import time
import uuid
from typing import Any, Callable, Awaitable

from loguru import logger
from orjson import dumps
from pydantic import validate_call
from redis.asyncio import Redis

from everwork.process import Process, RedisSettings, ProcessState, Resources
from everwork.utils import SafeCancellationZone, CloseEvent
from everwork.worker import TriggerMode, ExecutorMode, Event
from everwork.worker_wrapper import (
    TriggerWithQueueWorkerWrapper,
    ExecutorWorkerWrapper,
    ExecutorWithLimitArgsWorkerWrapper,
    TriggerWorkerWrapper,
    BaseWorkerWrapper
)

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class ProcessWrapper:

    def __init__(self, index: int, process_data: Process, redis_settings: RedisSettings, scripts: dict[str, str]):
        self.__index = index
        self.__process_data = process_data

        self.__redis = Redis(**redis_settings.model_dump())

        self.__lock = asyncio.Lock()

        self.__move_script = scripts['move_by_value']
        self.__set_script = scripts['set_state']

        self.__close_event = CloseEvent()
        self.__tasks: list[tuple[asyncio.Task, SafeCancellationZone]] = []

    def __set_closed_flag(self, *_):
        self.__close_event.set()

        for task, safe_cancellation_zone in self.__tasks:
            if safe_cancellation_zone.is_use():
                task.cancel()

    async def __move_resource(self, pipeline: Redis, worker_name: str, source: str, target: str, resource: Any) -> None:
        if resource is None:
            return

        await pipeline.evalsha(
            self.__move_script,
            2,
            f'worker:{worker_name}:{source}',
            f'worker:{worker_name}:{target}',
            resource
        )

    async def __handle_error(self, resources: Resources, worker_name: str):
        pipeline = self.__redis.pipeline()

        await self.__move_resource(pipeline, worker_name, 'taken_events', 'error_events', resources.event_raw)
        await self.__move_resource(pipeline, worker_name, 'taken_limit_args', 'limit_args', resources.limit_args_raw)

        if resources.event_id is not None:
            await pipeline.rpush(f'worker:{worker_name}:events:{resources.event_id}:status', 'error')

        await pipeline.execute()

    async def __handle_cancel(self, resources, worker_name: str):
        pipeline = self.__redis.pipeline()

        await self.__move_resource(pipeline, worker_name, 'taken_events', 'events', resources.event_raw)
        await self.__move_resource(pipeline, worker_name, 'taken_limit_args', 'limit_args', resources.limit_args_raw)

        if resources.event_id is not None:
            await pipeline.rpush(f'worker:{worker_name}:events:{resources.event_id}:status', 'cancel')

        await pipeline.execute()

    async def __set_running_state(self, timeout_reset: float):
        await self.__redis.evalsha(
            self.__set_script,
            1,
            f'process:{self.__index}:state',
            dumps(
                ProcessState(
                    status='running',
                    end_time=time.time() + timeout_reset
                ).model_dump(mode='json')
            ).decode()
        )

    async def __set_waiting_state(self):
        await self.__redis.evalsha(
            self.__set_script,
            1,
            f'process:{self.__index}:state',
            dumps(
                ProcessState(
                    status='waiting', end_time=None
                ).model_dump(mode='json')
            ).decode()
        )

    async def __processing_successful_execution(self, events: list[Event] | None, resources: Resources, worker_name: str):
        pipeline = self.__redis.pipeline()

        for event in (events or []):
            await pipeline.rpush(f'worker:{event.target}:events', dumps({'kwargs': event.kwargs, 'id': str(uuid.uuid4())}))

        if resources.event_raw is not None:
            await pipeline.lrem(f'worker:{worker_name}:taken_events', 1, resources.event_raw)

        if resources.event_id is not None:
            await pipeline.rpush(f'worker:{worker_name}:events:{resources.event_id}:status', 'success')

        await self.__move_resource(pipeline, worker_name, 'taken_limit_args', 'limit_args', resources.limit_args_raw)

        await pipeline.execute()

    async def __get_is_worker_on(self, worker_name: str) -> bool:
        value = await self.__redis.get(f'worker:{worker_name}:is_worker_on')
        return value == '1'

    async def __worker_task(self, function: Callable[..., Awaitable[list[Event] | None]], wrapper: BaseWorkerWrapper):
        try:
            while not self.__close_event.get():
                is_worker_on = await self.__get_is_worker_on(wrapper.settings.name)

                if not is_worker_on:
                    with wrapper.safe_cancellation_zone:
                        await asyncio.sleep(60)

                    continue

                resources = await wrapper.get_resources()

                async with self.__lock:
                    wrapper.clear()

                    if resources.status == 'error':
                        logger.error(f'{wrapper.settings.name} принял ивент с ошибкой')
                        await self.__handle_error(resources, wrapper.settings.name)
                        continue

                    if (
                        resources.status == 'cancel'
                        or self.__close_event.get()
                        or not (await self.__get_is_worker_on(wrapper.settings.name))
                    ):
                        logger.debug(f'{wrapper.settings.name} принял ивент на отмену')
                        await self.__handle_cancel(resources, wrapper.settings.name)
                        continue

                    logger.debug(f'{wrapper.settings.name} принял ивент на обработку')
                    await self.__set_running_state(wrapper.settings.timeout_reset)

                    try:
                        events = await function(**resources.kwargs)
                    except Exception as error:
                        logger.exception(f'Ошибка при выполнении {wrapper.settings.name}: {error}')
                        await self.__handle_error(resources, wrapper.settings.name)
                    else:
                        logger.debug(f'{wrapper.settings.name} успешно обработал ивент')
                        await self.__processing_successful_execution(events, resources, wrapper.settings.name)

                    await self.__set_waiting_state()
                    logger.debug(f'{wrapper.settings.name} успешно обработал ресурсы')
        except asyncio.CancelledError:
            pass
        except Exception as error:
            logger.exception(f'Неизвестная ошибка в {wrapper.settings.name}: {error}')

    def __initialize_workers(self) -> tuple[list[Callable[..., Awaitable[list[Event] | None]]], list[BaseWorkerWrapper]]:
        wrappers: list[BaseWorkerWrapper] = []
        function_wrappers: list[Callable[..., Awaitable[list[Event] | None]]] = []

        for worker in self.__process_data.workers:
            settings = worker.settings()

            if isinstance(settings.mode, TriggerMode):
                if settings.mode.with_queue_events:
                    worker_wrapper = TriggerWithQueueWorkerWrapper
                else:
                    worker_wrapper = TriggerWorkerWrapper
            elif isinstance(settings.mode, ExecutorMode):
                if settings.mode.limited_args is None:
                    worker_wrapper = ExecutorWorkerWrapper
                else:
                    worker_wrapper = ExecutorWithLimitArgsWorkerWrapper
            else:
                raise ValueError(f'Неизвестный worker mode: {settings.mode}')

            worker_object = worker()

            wrappers.append(worker_wrapper(self.__redis, worker_object, SafeCancellationZone(self.__close_event)))
            function_wrappers.append(validate_call(validate_return=True)(worker_object.__call__))

        return function_wrappers, wrappers

    @staticmethod
    async def __shutdown_workers(wrappers):
        for wrapper in wrappers:
            try:
                await wrapper.worker.shutdown()
            except Exception as error:
                logger.exception(f'Ошибка при выполнении shutdown {wrapper.settings.name}: {error}')

    @staticmethod
    async def __startup_workers(wrappers):
        for wrapper in wrappers:
            try:
                await wrapper.worker.startup()
            except Exception as error:
                logger.exception(f'Ошибка при выполнении startup {wrapper.settings.name}: {error}')
                raise

    async def __async_run(self) -> None:
        names = ', '.join(e.settings().name for e in self.__process_data.workers)
        logger.info(f'Процесс запущен. Состав: {names}')

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        function_wrappers, wrappers = self.__initialize_workers()
        logger.debug('Worker wrappers, worker functions успешно созданы')

        await self.__startup_workers(wrappers)
        logger.debug('Worker startup успешно запущены')

        for function, wrapper in zip(function_wrappers, wrappers):
            task = asyncio.create_task(self.__worker_task(function, wrapper))
            self.__tasks.append((task, wrapper.safe_cancellation_zone))

        await asyncio.gather(*map(lambda x: x[0], self.__tasks), return_exceptions=True)

        await self.__shutdown_workers(wrappers)
        logger.debug('Worker shutdown успешно запущены')

        await self.__redis.close()
        logger.info(f'Процесс завершен. Состав: {names}')

    @classmethod
    def run(cls, index: int, data: dict[str, Any], redis_settings: dict[str, Any], scripts: dict[str, str]) -> None:
        process_data = Process.model_validate(data)

        try:
            with asyncio.Runner(loop_factory=new_event_loop) as runner:
                runner.run(cls(index, process_data, RedisSettings.model_validate(redis_settings), scripts).__async_run())
        except Exception as error:
            names = ', '.join(e.settings().name for e in process_data.workers)
            logger.exception(f'Процесс неожиданно завершился. Состав: {names}. {error}')
