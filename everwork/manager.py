import asyncio
import signal
import time
from contextlib import suppress
from multiprocessing.context import SpawnProcess
from typing import Annotated

from loguru import logger
from orjson import dumps
from pydantic import validate_call, AfterValidator
from redis.asyncio import Redis

from everwork.process import Process, RedisSettings, ProcessState, check_worker_names
from everwork.process_wrapper import ProcessWrapper
from everwork.utils import get_worker_parameters, CloseEvent, AwaitLock
from everwork.worker import ExecutorMode


class Manager:

    @validate_call
    def __init__(self, redis_settings: RedisSettings, processes: Annotated[list[Process], AfterValidator(check_worker_names)]):
        self.__redis_settings = redis_settings
        self.__redis = Redis(**redis_settings.model_dump())

        self.__processes_data = []

        for process in processes:
            self.__processes_data += [process] * process.replicas

        self.__processes: list[SpawnProcess] = []

        self.__close_event = CloseEvent()
        self.__tasks: list[tuple[asyncio.Task, AwaitLock]] = []

    async def __init_process(self):
        default_state = ProcessState(status='waiting', end_time=None).model_dump_json()
        await self.__redis.mset({f'process:{index}:state': default_state for index in range(len(self.__processes_data))})

    async def __init_workers(self):
        keys = []

        for process in self.__processes_data:
            for worker in process.workers:
                keys.append(f'worker:{worker.settings().name}:is_worker_on')

        response = await self.__redis.mget(keys)

        value = {key: 0 for key, is_worker_on in zip(keys, response) if is_worker_on is None}

        if not value:
            return

        await self.__redis.mset(value)

    async def __register_limit_args(self):
        pipeline = self.__redis.pipeline()

        for process_data in self.__processes_data:
            for worker in process_data.workers:
                mode = worker.settings().mode

                if not isinstance(mode, ExecutorMode):
                    continue

                if mode.limited_args is None:
                    continue

                await pipeline.delete(f'worker:{worker.settings().name}:taken_limit_args')
                await pipeline.delete(f'worker:{worker.settings().name}:limit_args')
                await pipeline.rpush(
                    f'worker:{worker.settings().name}:limit_args',
                    *(dumps(args) for args in mode.limited_args)
                )

        await pipeline.execute()

    def __set_closed_flag(self, *_):
        self.__close_event.set()

        for task, await_lock in self.__tasks:
            if await_lock():
                task.cancel()

    def __create_process(self, index: int, process_data: Process) -> SpawnProcess:
        process = SpawnProcess(
            target=ProcessWrapper.run,
            kwargs={
                'index': index,
                'data': process_data.model_dump(),
                'redis_settings': self.__redis_settings.model_dump()
            },
            daemon=True
        )
        process.start()
        return process

    async def __process_task(self, index: int, await_lock: AwaitLock):
        process = self.__processes[index]
        process_data = self.__processes_data[index]

        names = ', '.join(e.settings().name for e in process_data.workers)
        max_timeout_reset = max(worker.settings().timeout_reset for worker in process_data.workers)

        key = f'process:{index}:state'

        with suppress(asyncio.CancelledError):
            while not self.__close_event.get():
                state = ProcessState.model_validate_json(
                    await self.__redis.get(key)
                )

                if state.end_time is None:
                    with await_lock:
                        await asyncio.sleep(5)

                    continue

                if (timeout := state.end_time - time.time()) >= 0:
                    with await_lock:
                        await asyncio.sleep(timeout)

                    continue

                logger.warning(f'Начат перезапуск процесса. Состав: {names}')

                process.terminate()
                logger.debug(f'Отправлен сигнал SIGTERM процессу. Состав: {names}')

                end_time = time.time() + max_timeout_reset

                with await_lock:
                    while True:
                        if time.time() > end_time:
                            break

                        if not process.is_alive():
                            break

                        await asyncio.sleep(0.1)

                if process.is_alive():
                    process.kill()
                    logger.warning(f'Отправлен сигнал SIGKILL процессу. Состав: {names}')

                process.join()
                process.close()

                self.__processes[index] = self.__create_process(index, process_data)
                await asyncio.sleep(0.1)

                logger.warning(f'Завершен перезапуск процесса. Состав: {names}')

    async def run(self):
        logger.info(await get_worker_parameters(self.__redis))

        logger.info('Manager запушен')

        signal.signal(signal.SIGINT, self.__set_closed_flag)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        await self.__init_process()
        await self.__init_workers()
        await self.__register_limit_args()

        logger.info('Процессы, workers, limit args инициализированы')

        logger.info('Начато создание процессов')

        for index, process_data in enumerate(self.__processes_data):
            self.__processes.append(self.__create_process(index, process_data))

        await asyncio.sleep(0.1)

        logger.info('Закончено создание процессов')

        for index in range(len(self.__processes)):
            await_lock = AwaitLock(self.__close_event)
            self.__tasks.append((asyncio.create_task(self.__process_task(index, await_lock)), await_lock))

        await asyncio.gather(*map(lambda x: x[0], self.__tasks))

        logger.info('Manager начал процесс завершения')

        for process in self.__processes:
            process.terminate()

        logger.debug('Процессам послан сигнал SIGTERM')

        end_time = time.time() + max(
            max(worker.settings().timeout_reset for worker in process.workers) for process in self.__processes_data
        )

        while True:
            if time.time() > end_time:
                logger.warning('Процессы не успели завершиться')
                break

            if all(not process.is_alive() for process in self.__processes):
                logger.debug('Процессы успешно завершились')
                break

            await asyncio.sleep(0.1)

        for process in self.__processes:
            process.kill()

        logger.debug('Процессам послан сигнал SIGKILL')

        for process in self.__processes:
            process.join()

        for process in self.__processes:
            process.close()

        logger.debug('Процессы закрыты')

        await self.__redis.close()

        logger.info('Manager завершен')
