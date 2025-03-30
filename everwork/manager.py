import asyncio
import multiprocessing
import signal
import time
from multiprocessing.context import SpawnProcess

from orjson import dumps
from redis.asyncio import Redis

from everwork.process import Process, RedisSettings, ProcessState
from everwork.process_wrapper import ProcessWrapper
from everwork.worker import ExecutorMode


class Manager:

    def __init__(self, redis_settings: RedisSettings, processes: list[Process]):
        self.__redis_settings = redis_settings
        self.__processes_data = processes

        self.__redis = Redis(**redis_settings.model_dump())

        self.__is_closed = False

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

                await pipeline.delete(f'worker:{worker.settings().name}:limit_args')
                await pipeline.rpush(
                    f'worker:{worker.settings().name}:limit_args',
                    *(dumps(args) for args in mode.limited_args)
                )

        await pipeline.execute()

    def __set_closed_flag(self, *_):
        self.__is_closed = True

    def __create_process(self, index: int, process_data: Process) -> SpawnProcess:
        process = SpawnProcess(
            target=ProcessWrapper.run,
            kwargs={
                'index': index,
                'process_data': process_data,
                'redis_settings': self.__redis_settings
            },
            daemon=True
        )
        print(process)
        process.start()
        print(9999)
        return process

    async def run(self):
        print(1)
        signal.signal(signal.SIGINT, self.__set_closed_flag)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)
        print(2)
        await self.__init_process()
        await self.__init_workers()
        print(3)
        await self.__register_limit_args()
        print(4)
        processes: list[SpawnProcess] = []

        for index, process_data in enumerate(self.__processes_data):
            print(7777)
            processes.append(self.__create_process(index, process_data))
        print(6)
        while True:
            if self.__is_closed:
                break

            for index, (process_data, process) in enumerate(zip(self.__processes_data, processes)):
                state = ProcessState.model_validate_json(
                    await self.__redis.get(f'process:{index}:state')
                )

                if state.status != 'running':
                    continue

                if state.end_time is not None and time.time() < state.end_time:
                    continue

                process.terminate()

                end_time = time.time() + max(
                    worker.settings().timeout_reset for worker in process_data.workers
                )

                while True:
                    if time.time() > end_time:
                        break

                    if not process.is_alive():
                        break

                    await asyncio.sleep(0.1)

                process.kill()
                process.join()
                process.close()

                processes[index] = self.__create_process(index, process_data)

        for process in processes:
            process.terminate()

        end_time = time.time() + max(
            max(worker.settings().timeout_reset for worker in process.workers) for process in self.__processes_data
        )

        while True:
            if time.time() > end_time:
                break

            if all(not process.is_alive() for process in processes):
                break

            await asyncio.sleep(0.1)

        for process in processes:
            process.kill()

        for process in processes:
            process.join()

        for process in processes:
            process.close()

        await self.__redis.close()
