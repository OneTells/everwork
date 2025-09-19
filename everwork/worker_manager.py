import asyncio
import signal
import time
from typing import Any, Callable, Awaitable
from uuid import uuid4, UUID

from loguru import logger
from orjson import dumps
from pydantic import validate_call, RedisDsn
from redis.asyncio import Redis

from everwork.worker_base import BaseWorker, WorkerEvent, ExecutorMode, TriggerMode
from everwork.utils import ShutdownSafeZone, ShutdownEvent
from everwork.worker_wrapper import (
    Resources,
    TriggerWithStreamsWorkerWrapper,
    ExecutorWorkerWrapper,
    ExecutorWithLimitArgsWorkerWrapper,
    TriggerWorkerWrapper,
    BaseWorkerWrapper
)

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class WorkerManager:

    def __init__(self, uuid: str, workers: list[type[BaseWorker]], redis_dsn: RedisDsn, scripts: dict[str, str]) -> None:
        self.__uuid = uuid
        self.__workers = workers

        self.__redis = Redis.from_url(
            url=redis_dsn.encoded_string(),
            protocol=3,
            decode_responses=True
        )

        self.__lock = asyncio.Lock()

        self.__set_script = scripts['set_state']

        self.__shutdown_event = ShutdownEvent()
        self.__tasks: list[tuple[asyncio.Task, ShutdownSafeZone]] = []

    def __set_closed_flag(self, *_) -> None:
        self.__shutdown_event.set()

        for task, shutdown_safe_zone in self.__tasks:
            if shutdown_safe_zone.is_use():
                task.cancel()

    async def __move_resource(self, pipeline: Redis, worker_name: str, source: str, target: str, resource: Any) -> None:
        pass

    async def __handle_error(self, resources: Resources, worker_name: str) -> None:
        pipeline = self.__redis.pipeline()

        await self.__move_resource(pipeline, worker_name, 'taken_events', 'error_events', resources.event_raw)
        await self.__move_resource(pipeline, worker_name, 'taken_limit_args', 'limit_args', resources.limit_args_raw)

        if resources.event_id is not None:
            await pipeline.rpush(f'worker:{worker_name}:events:{resources.event_id}:status', 'error')

        await pipeline.execute()

    async def __handle_cancel(self, resources, worker_name: str) -> None:
        pipeline = self.__redis.pipeline()

        await self.__move_resource(pipeline, worker_name, 'taken_events', 'events', resources.event_raw)
        await self.__move_resource(pipeline, worker_name, 'taken_limit_args', 'limit_args', resources.limit_args_raw)

        if resources.event_id is not None:
            await pipeline.rpush(f'worker:{worker_name}:events:{resources.event_id}:status', 'cancel')

        await pipeline.execute()

    async def __set_running_state(self, execution_timeout: float) -> None:
        await self.__redis.evalsha(
            self.__set_script,
            1,
            f'process:{self.__uuid}:state',
            dumps({'status': 'running', 'end_time': time.time() + execution_timeout}).decode()
        )

    async def __set_waiting_state(self) -> None:
        await self.__redis.evalsha(
            self.__set_script,
            1,
            f'process:{self.__uuid}:state',
            dumps({'status': 'waiting', 'end_time': None}).decode()
        )

    async def __processing_successful_execution(self, events: list[WorkerEvent] | None, resources: Resources, worker_name: str):
        pipeline = self.__redis.pipeline()

        for event in (events or []):
            await pipeline.rpush(f'worker:{event.target_stream}:events', dumps({'kwargs': event.data, 'id': str(uuid4())}))

        if resources.event_raw is not None:
            await pipeline.lrem(f'worker:{worker_name}:taken_events', 1, resources.event_raw)

        if resources.event_id is not None:
            await pipeline.rpush(f'worker:{worker_name}:events:{resources.event_id}:status', 'success')

        await self.__move_resource(pipeline, worker_name, 'taken_limit_args', 'limit_args', resources.limit_args_raw)

        await pipeline.execute()

    async def __get_is_worker_on(self, worker_name: str) -> bool:
        value = await self.__redis.get(f'worker:{worker_name}:is_worker_on')
        return value == '1'

    async def __worker_task(self, function: Callable[..., Awaitable[list[WorkerEvent] | None]], wrapper: BaseWorkerWrapper):
        try:
            while not self.__shutdown_event.is_set():
                is_worker_on = await self.__get_is_worker_on(wrapper.settings.name)

                if not is_worker_on:
                    with wrapper.shutdown_safe_zone:
                        await asyncio.sleep(60)

                    continue

                resources = await wrapper.get_resources()

                async with self.__lock:
                    if resources.status == 'error':
                        logger.error(f'{wrapper.settings.name} принял ивент с ошибкой')
                        await self.__handle_error(resources, wrapper.settings.name)
                        continue

                    if (
                        resources.status == 'cancel'
                        or self.__shutdown_event.is_set()
                        or not (await self.__get_is_worker_on(wrapper.settings.name))
                    ):
                        logger.debug(f'{wrapper.settings.name} принял ивент на отмену')
                        await self.__handle_cancel(resources, wrapper.settings.name)
                        continue

                    logger.debug(f'{wrapper.settings.name} принял ивент на обработку')
                    await self.__set_running_state(wrapper.settings.execution_timeout)

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

    def __initialize_workers(self) -> tuple[list[Callable[..., Awaitable[list[WorkerEvent] | None]]], list[BaseWorkerWrapper]]:
        wrappers: list[BaseWorkerWrapper] = []
        function_wrappers: list[Callable[..., Awaitable[list[WorkerEvent] | None]]] = []

        for worker in self.__workers:
            if isinstance(worker.settings.mode, TriggerMode):
                if worker.settings.mode.source_streams:
                    worker_wrapper = TriggerWithStreamsWorkerWrapper
                else:
                    worker_wrapper = TriggerWorkerWrapper
            elif isinstance(worker.settings.mode, ExecutorMode):
                if worker.settings.mode.limited_args is None:
                    worker_wrapper = ExecutorWorkerWrapper
                else:
                    worker_wrapper = ExecutorWithLimitArgsWorkerWrapper
            else:
                raise ValueError(f'Неизвестный worker mode: {worker.settings.mode}')

            worker_object = worker()

            wrappers.append(worker_wrapper(self.__redis, self.__uuid, worker_object, ShutdownSafeZone(self.__shutdown_event)))
            function_wrappers.append(validate_call(validate_return=True)(worker_object.__call__))

        return function_wrappers, wrappers

    @staticmethod
    async def __shutdown_workers(wrappers) -> None:
        for wrapper in wrappers:
            try:
                await wrapper.worker.shutdown()
            except Exception as error:
                logger.exception(f'Ошибка при выполнении shutdown {wrapper.settings.name}: {error}')

    @staticmethod
    async def __startup_workers(wrappers) -> None:
        for wrapper in wrappers:
            try:
                await wrapper.worker.startup()
            except Exception as error:
                logger.exception(f'Ошибка при выполнении startup {wrapper.settings.name}: {error}')
                raise

    async def __async_run(self) -> None:
        worker_names = ', '.join(worker.settings.name for worker in self.__workers)
        logger.info(f'Процесс запущен. Состав: {worker_names}')

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        function_wrappers, wrappers = self.__initialize_workers()
        logger.debug('Worker wrappers, worker functions успешно созданы')

        await self.__startup_workers(wrappers)
        logger.debug('Worker startup успешно запущены')

        for function, wrapper in zip(function_wrappers, wrappers):
            task = asyncio.create_task(self.__worker_task(function, wrapper))
            self.__tasks.append((task, wrapper.shutdown_safe_zone))

        await asyncio.gather(*map(lambda x: x[0], self.__tasks), return_exceptions=True)

        await self.__shutdown_workers(wrappers)
        logger.debug('Worker shutdown успешно запущены')

        await self.__redis.close()
        logger.info(f'Процесс завершен. Состав: {worker_names}')

    @classmethod
    @validate_call
    def run(cls, uuid: UUID, workers: list[type[BaseWorker]], redis_dsn: RedisDsn, scripts: dict[str, str]) -> None:
        try:
            with asyncio.Runner(loop_factory=new_event_loop) as runner:
                runner.run(cls(str(uuid), workers, redis_dsn, scripts).__async_run())
        except Exception as error:
            worker_names = ', '.join(worker.settings.name for worker in workers)
            logger.exception(f'Процесс неожиданно завершился. Состав: {worker_names}. {error}')
