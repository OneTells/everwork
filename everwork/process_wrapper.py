import asyncio
import signal
import time

from loguru import logger
from orjson import dumps
from redis.asyncio import Redis
from uvloop import new_event_loop

from everwork.process import Process, RedisSettings
from everwork.worker import TriggerMode, ExecutorMode
from everwork.worker_wrapper import TriggerWithQueueWorkerWrapper, ExecutorWorkerWrapper, ExecutorWithLimitArgsWorkerWrapper, \
    TriggerWorkerWrapper


class ProcessWrapper:

    def __init__(self, index: int, process_data: Process, redis_settings: RedisSettings):
        self.__index = index

        self.__workers = process_data.workers
        self.__settings = process_data.settings

        self.__redis = Redis(**redis_settings.model_dump())

        self.__is_closed = False

    def __set_closed_flag(self, _, __):
        self.__is_closed = True

    async def __async_run(self) -> None:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        workers = []

        for worker in self.__workers:
            if isinstance(worker.settings().mode, TriggerMode):
                if worker.settings().mode.with_queue_events:
                    wrapper = TriggerWithQueueWorkerWrapper
                else:
                    wrapper = TriggerWorkerWrapper
            elif isinstance(worker.settings().mode, ExecutorMode):
                if worker.settings().mode.limited_args is None:
                    wrapper = ExecutorWorkerWrapper
                else:
                    wrapper = ExecutorWithLimitArgsWorkerWrapper
            else:
                raise ValueError(f'Неизвестный worker mode: {worker.settings().mode}')

            workers.append(wrapper(self.__redis, worker))

        for worker in workers:
            await worker.worker.startup()

        last_work_time = 0

        while True:
            if last_work_time + self.__settings.timeout.active_lifetime < time.time():
                delay = self.__settings.timeout.inactive_timeout
            else:
                delay = self.__settings.timeout.active_timeout

            await asyncio.sleep(delay)

            if self.__is_closed:
                break

            for worker in workers:
                worker_is_on = await worker.check_worker_is_on()

                if not worker_is_on:
                    continue

                kwargs, resources = await worker.get_kwargs()

                if kwargs is None:
                    continue

                await self.__redis.set(
                    f'process:{self.__index}:state',
                    dumps({'status': 'running', 'end_time': time.time() + worker.worker.settings().timeout_reset})
                )

                try:
                    events = await worker.worker(**kwargs)
                except Exception as error:
                    logger.exception(f'Ошибка при выполнении {worker.worker.settings().name}: {error}')

                    for resource in resources:
                        await resource.error()
                else:
                    await worker.push_events(events)

                    for resource in resources:
                        await resource.success()

                last_work_time = time.time()

                await self.__redis.set(
                    f'process:{self.__index}:state',
                    dumps({'status': 'waiting', 'end_time': None})
                )

        for worker in workers:
            await worker.worker.shutdown()

        await self.__redis.close()

    @classmethod
    def run(cls, index: int, process_data: Process, redis_settings: RedisSettings) -> None:
        with asyncio.Runner(loop_factory=new_event_loop) as runner:
            runner.run(cls(index, process_data, redis_settings).__async_run())
