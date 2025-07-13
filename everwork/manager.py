import asyncio
import signal
import time
from multiprocessing.context import SpawnProcess
from typing import Annotated

from loguru import logger
from orjson import dumps
from pydantic import validate_call, AfterValidator
from redis.asyncio import Redis

from everwork.process import Process, RedisSettings, ProcessState, check_worker_names
from everwork.process_wrapper import ProcessWrapper
from everwork.utils import CloseEvent, SafeCancellationZone
from everwork.worker import ExecutorMode


class Manager:

    @validate_call
    def __init__(self, redis_settings: RedisSettings, processes: Annotated[list[Process], AfterValidator(check_worker_names)]):
        self.__redis_settings = redis_settings
        self.__redis = Redis(**redis_settings.model_dump())

        self.__processes_data: list[Process] = []

        for process in processes:
            self.__processes_data += [process] * process.replicas

        self.__processes: list[SpawnProcess] = []

        self.__close_event = CloseEvent()
        self.__tasks: list[tuple[asyncio.Task, SafeCancellationZone]] = []

        self.__scripts: dict[str, str] = {}

    async def __init_process(self):
        await self.__redis.delete(*(f'process:{index}:state' for index in range(len(self.__processes_data))))

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

        for task, safe_cancellation_zone in self.__tasks:
            if safe_cancellation_zone.is_use():
                task.cancel()

    def __create_process(self, index: int, process_data: Process) -> SpawnProcess:
        process = SpawnProcess(
            target=ProcessWrapper.run,
            kwargs={
                'index': index,
                'data': process_data.model_dump(),
                'redis_settings': self.__redis_settings.model_dump(),
                'scripts': self.__scripts
            },
            daemon=True
        )
        process.start()
        return process

    async def __process_task(self, index: int, safe_cancellation_zone: SafeCancellationZone):
        process_data = self.__processes_data[index]
        names = ', '.join(e.settings().name for e in process_data.workers)

        tasks: dict[str, asyncio.Task] = {}

        try:
            while not self.__close_event.get():
                with safe_cancellation_zone:
                    data = await self.__redis.brpop([f'process:{index}:state'])

                state = ProcessState.model_validate_json(data[1])

                if state.status == 'waiting':
                    continue

                # noinspection PyTypeChecker
                tasks |= {
                    'timeout': asyncio.create_task(asyncio.sleep(state.end_time - time.time())),
                    'wait_complete': asyncio.create_task(self.__redis.brpop([f'process:{index}:state'])),
                }

                with safe_cancellation_zone:
                    await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)

                if tasks['wait_complete'].done():
                    if not tasks['timeout'].done():
                        tasks['timeout'].cancel()

                    continue

                if not tasks['wait_complete'].done():
                    tasks['wait_complete'].cancel()

                logger.warning(f'Начат перезапуск процесса. Состав: {names}')

                self.__processes[index].terminate()
                logger.debug(f'Отправлен сигнал SIGTERM процессу. Состав: {names}')

                end_time = time.time() + 3

                with safe_cancellation_zone:
                    while True:
                        if time.time() > end_time:
                            break

                        if not self.__processes[index].is_alive():
                            break

                        await asyncio.sleep(0.1)

                if self.__processes[index].is_alive():
                    self.__processes[index].kill()
                    logger.warning(f'Отправлен сигнал SIGKILL процессу. Состав: {names}')

                self.__processes[index].join()
                self.__processes[index].close()

                self.__processes[index] = self.__create_process(index, process_data)
                await asyncio.sleep(0.1)

                logger.warning(f'Завершен перезапуск процесса. Состав: {names}')
        except asyncio.CancelledError:
            pass
        except Exception as error:
            logger.exception(f'Проверка процесса неожиданно завершилась. Состав: {names}. {error}')

        for task in tasks.values():
            if not task.done():
                task.cancel()

    async def __register_move_by_value_script(self) -> str:
        return await self.__redis.script_load(
            """
            local source_list = KEYS[1]
            local destination_list = KEYS[2]
            local value = ARGV[1]

            redis.call('LREM', source_list, 1, value)
            redis.call('RPUSH', destination_list, value)
            """
        )

    async def __register_set_state_script(self) -> str:
        return await self.__redis.script_load(
            """
            local key = KEYS[1]
            local value = ARGV[1]

            redis.call("DEL", key)
            redis.call("LPUSH", key, value)
            """
        )

    async def run(self):
        # TODO: Все ивенты из taken_events перенести в error_events

        # TODO: Сообщить о error_events (наличие и кол-во)

        # TODO: Сообщить о events (кол-во) у workers

        logger.info('Manager запушен')

        signal.signal(signal.SIGINT, self.__set_closed_flag)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        self.__scripts['move_by_value'] = await self.__register_move_by_value_script()
        self.__scripts['set_state'] = await self.__register_set_state_script()
        logger.debug('Скрипты успешно зарегистрированы')

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
            safe_cancellation_zone = SafeCancellationZone(self.__close_event)
            self.__tasks.append((
                asyncio.create_task(self.__process_task(index, safe_cancellation_zone)),
                safe_cancellation_zone
            ))

        await asyncio.gather(*map(lambda x: x[0], self.__tasks), return_exceptions=True)

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
