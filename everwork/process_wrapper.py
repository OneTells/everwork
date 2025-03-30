import asyncio
import signal
import time

from loguru import logger
from orjson import dumps
from pydantic import validate_call
from redis.asyncio import Redis
from uvloop import new_event_loop

from everwork.process import Process, RedisSettings, Resources
from everwork.utils import register_move_by_value_script, return_limit_args, cancel_event, remove_event, return_event, \
    set_process_state
from everwork.worker import TriggerMode, ExecutorMode, Event
from everwork.worker_wrapper import TriggerWithQueueWorkerWrapper, ExecutorWorkerWrapper, ExecutorWithLimitArgsWorkerWrapper, \
    TriggerWorkerWrapper


class ProcessWrapper:

    def __init__(self, index: int, process_data: Process, redis_settings: RedisSettings):
        self.__index = index
        self.__process_data = process_data

        self.__redis = Redis(**redis_settings.model_dump())

        self.__is_closed = False

    def __set_closed_flag(self, *_):
        self.__is_closed = True

    async def __async_run(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        move_by_value_script_sha = await register_move_by_value_script(self.__redis)

        wrappers = []
        function_wrappers = []

        for worker in self.__process_data.workers:
            if isinstance(worker.settings().mode, TriggerMode):
                if worker.settings().mode.with_queue_events:
                    worker_wrapper = TriggerWithQueueWorkerWrapper
                else:
                    worker_wrapper = TriggerWorkerWrapper
            elif isinstance(worker.settings().mode, ExecutorMode):
                if worker.settings().mode.limited_args is None:
                    worker_wrapper = ExecutorWorkerWrapper
                else:
                    worker_wrapper = ExecutorWithLimitArgsWorkerWrapper
            else:
                raise ValueError(f'Неизвестный worker mode: {worker.settings().mode}')

            worker_object = worker()

            wrappers.append(worker_wrapper(self.__redis, worker_object))
            function_wrappers.append(validate_call(validate_return=True)(worker_object.__call__))

        for wrapper in wrappers:
            await wrapper.worker.startup()

        last_work_time = 0

        while True:
            if time.time() > last_work_time + self.__process_data.timeout.active_lifetime:
                delay = self.__process_data.timeout.inactive_timeout
            else:
                delay = self.__process_data.timeout.active_timeout

            await asyncio.sleep(delay)

            if self.__is_closed:
                break

            for function_wrapper, wrapper in zip(function_wrappers, wrappers):
                worker_name: str = wrapper.worker.settings().name

                worker_is_on: bool = await wrapper.check_worker_is_on()

                if not worker_is_on:
                    continue

                resources: Resources = await wrapper.get_kwargs()

                if resources.kwargs is None:
                    await cancel_event(self.__redis, move_by_value_script_sha, worker_name, resources.event)
                    await return_limit_args(self.__redis, move_by_value_script_sha, worker_name, resources.limit_args)
                    continue

                await set_process_state(self.__redis, self.__index, time.time() + wrapper.worker.settings().timeout_reset)

                pipeline = self.__redis.pipeline()

                try:
                    events: list[Event] | None = await function_wrapper(**resources.kwargs)
                except Exception as error:
                    logger.exception(f'Ошибка при выполнении {worker_name}: {error}')
                    await return_event(pipeline, move_by_value_script_sha, worker_name, resources.event)
                else:
                    for event in (events or []):
                        await pipeline.rpush(f'worker:{event.target}:events', dumps(event))

                    await remove_event(pipeline, worker_name, resources.event)

                await return_limit_args(pipeline, move_by_value_script_sha, worker_name, resources.limit_args)
                await set_process_state(pipeline, self.__index, None)

                await pipeline.execute()

                last_work_time = time.time()

        for wrapper in wrappers:
            await wrapper.worker.shutdown()

        await self.__redis.close()

    @classmethod
    def run(cls, index: int, process_data: Process, redis_settings: RedisSettings) -> None:
        with asyncio.Runner(loop_factory=new_event_loop) as runner:
            runner.run(cls(index, process_data, redis_settings).__async_run())
