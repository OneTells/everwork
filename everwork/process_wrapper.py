import asyncio
import signal
import time
from contextlib import suppress
from typing import Any, Callable, Awaitable

from loguru import logger
from orjson import dumps
from pydantic import validate_call
from redis.asyncio import Redis
from uvloop import new_event_loop

from everwork.process import Process, RedisSettings
from everwork.utils import register_move_by_value_script, return_limit_args, cancel_event, remove_event, set_error_event, \
    push_event, get_is_worker_on, AwaitLock, CloseEvent
from everwork.worker import TriggerMode, ExecutorMode, Event
from everwork.worker_wrapper import TriggerWithQueueWorkerWrapper, ExecutorWorkerWrapper, ExecutorWithLimitArgsWorkerWrapper, \
    TriggerWorkerWrapper, BaseWorkerWrapper


class ProcessWrapper:

    def __init__(self, index: int, process_data: Process, redis_settings: RedisSettings):
        self.__index = index
        self.__process_data = process_data

        self.__redis = Redis(**redis_settings.model_dump())

        self.__lock = asyncio.Lock()
        self.__script_sha = ''

        self.__close_event = CloseEvent()
        self.__tasks: list[tuple[asyncio.Task, AwaitLock]] = []

    def __set_closed_flag(self, *_):
        self.__close_event.set()

        for task, await_lock in self.__tasks:
            if await_lock():
                task.cancel()

    async def __worker_task(self, function: Callable[..., Awaitable[list[Event] | None]], wrapper: BaseWorkerWrapper):
        last_work_time = time.time()

        with suppress(asyncio.CancelledError):
            while not self.__close_event.get():
                if time.time() - last_work_time > wrapper.settings.timeout.active_lifetime:
                    delay = wrapper.settings.timeout.inactive
                else:
                    delay = wrapper.settings.timeout.active

                with wrapper.await_lock:
                    await asyncio.sleep(delay)

                is_worker_on = await get_is_worker_on(self.__redis, wrapper.settings.name)

                if not is_worker_on:
                    with wrapper.await_lock:
                        await asyncio.sleep(60)

                    continue

                resources = await wrapper.get_kwargs()

                if resources.status == 'error':
                    await set_error_event(self.__redis, self.__script_sha, wrapper.settings.name, resources.event)
                    await return_limit_args(self.__redis, self.__script_sha, wrapper.settings.name, resources.limit_args)
                    continue

                if (
                    resources.status == 'cancel'
                    or self.__close_event.get()
                    or not (await get_is_worker_on(self.__redis, wrapper.settings.name))
                ):
                    await cancel_event(self.__redis, self.__script_sha, wrapper.settings.name, resources.event)
                    await return_limit_args(self.__redis, self.__script_sha, wrapper.settings.name, resources.limit_args)
                    continue

                async with self.__lock:
                    logger.debug(f'{wrapper.settings.name} принял ивент на обработку')

                    state = dumps({'end_time': time.time() + wrapper.settings.timeout_reset}).decode()

                    pipeline = self.__redis.pipeline()
                    await pipeline.lset(f'process:{self.__index}:state:running', 0, state)
                    await pipeline.delete(f'process:{self.__index}:state:wait')
                    await pipeline.execute()

                    try:
                        events = await function(**resources.kwargs)
                    except Exception as error:
                        logger.exception(f'Ошибка при выполнении {wrapper.settings.name}: {error}')
                        await set_error_event(self.__redis, self.__script_sha, wrapper.settings.name, resources.event)
                    else:
                        logger.debug(f'{wrapper.settings.name} успешно обработал ивент')

                        pipeline = self.__redis.pipeline()
                        for event in events:
                            await push_event(pipeline, event)
                        await pipeline.execute()

                        await remove_event(self.__redis, wrapper.settings.name, resources.event)

                    pipeline = self.__redis.pipeline()
                    await pipeline.lset(f'process:{self.__index}:state:wait', 0, '')
                    await pipeline.delete(f'process:{self.__index}:state:running')
                    await pipeline.execute()

                    await return_limit_args(self.__redis, self.__script_sha, wrapper.settings.name, resources.limit_args)
                    logger.debug(f'{wrapper.settings.name} успешно обработал ресурсы')

                last_work_time = time.time()

    async def __async_run(self) -> None:
        names = ', '.join(e.settings().name for e in self.__process_data.workers)
        logger.info(f'Процесс запущен. Состав: {names}')

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        self.__script_sha = await register_move_by_value_script(self.__redis)
        logger.debug('Скрипт перемещения успешно зарегистрирован')

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

            wrappers.append(worker_wrapper(self.__redis, worker_object, AwaitLock(self.__close_event)))
            function_wrappers.append(validate_call(validate_return=True)(worker_object.__call__))

        logger.debug('Worker wrappers, worker functions успешно созданы')

        for wrapper in wrappers:
            await wrapper.worker.startup()

        logger.debug('Worker startup успешно запущены')

        for function, wrapper in zip(function_wrappers, wrappers):
            self.__tasks.append((asyncio.create_task(self.__worker_task(function, wrapper)), wrapper.await_lock))

        await asyncio.gather(*map(lambda x: x[0], self.__tasks))

        for wrapper in wrappers:
            await wrapper.worker.shutdown()

        logger.debug('Worker shutdown успешно запущены')

        await self.__redis.close()

        logger.info(f'Процесс завершен. Состав: {names}')

    @classmethod
    def run(cls, index: int, data: dict[str, Any], redis_settings: dict[str, Any]) -> None:
        process_data = Process.model_validate(data)

        try:
            with asyncio.Runner(loop_factory=new_event_loop) as runner:
                runner.run(cls(index, process_data, RedisSettings.model_validate(redis_settings)).__async_run())
        except Exception as error:
            names = ', '.join(e.settings().name for e in process_data.workers)
            logger.exception(f'Процесс неожиданно завершился. Состав: {names}. {error}')
