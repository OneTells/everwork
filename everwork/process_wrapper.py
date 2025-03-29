import asyncio
import signal
import time

from loguru import logger
from pydantic import validate_call, ValidationError
from redis.asyncio import Redis
from uvloop import new_event_loop

from everwork.process import Process, RedisSettings, ProcessState
from everwork.resource import register_move_by_value_script
from everwork.utils import push_events
from everwork.worker import TriggerMode, ExecutorMode
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
        functions = []

        for worker in self.__process_data.workers:
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

            wrappers.append(wrapper(self.__redis, worker, move_by_value_script_sha))
            functions.append(validate_call(validate_return=True)(worker.__call__))

        for wrapper in wrappers:
            await wrapper.worker.startup()

        last_work_time = 0

        while True:
            if time.time() > last_work_time + self.__process_data.settings.timeout.active_lifetime:
                delay = self.__process_data.settings.timeout.inactive_timeout
            else:
                delay = self.__process_data.settings.timeout.active_timeout

            await asyncio.sleep(delay)

            if self.__is_closed:
                break

            for function, wrapper in zip(functions, wrappers):
                worker_is_on = await wrapper.check_worker_is_on()

                if not worker_is_on:
                    continue

                kwargs, resources = await wrapper.get_kwargs()

                if kwargs is None:
                    for resource in resources:
                        await resource.cancel(self.__redis)

                    continue

                await self.__redis.set(
                    f'process:{self.__index}:state',
                    ProcessState(
                        status='running',
                        end_time=time.time() + wrapper.worker.settings().timeout_reset
                    ).model_dump_json()
                )

                try:
                    events = await function(**kwargs)
                except ValidationError as error:
                    logger.exception(f'Ошибка при валидации {wrapper.worker.settings().name}: {error}')

                    for resource in resources:
                        await resource.error(self.__redis)
                except Exception as error:
                    logger.exception(f'Ошибка при выполнении {wrapper.worker.settings().name}: {error}')

                    for resource in resources:
                        await resource.error(self.__redis)
                else:
                    await push_events(self.__redis, events)

                    for resource in resources:
                        await resource.success(self.__redis)

                last_work_time = time.time()

                await self.__redis.set(
                    f'process:{self.__index}:state',
                    ProcessState(status='waiting', end_time=None).model_dump_json()
                )

        for wrapper in wrappers:
            await wrapper.worker.shutdown()

        await self.__redis.close()

    @classmethod
    def run(cls, index: int, process_data: Process, redis_settings: RedisSettings) -> None:
        with asyncio.Runner(loop_factory=new_event_loop) as runner:
            runner.run(cls(index, process_data, redis_settings).__async_run())
