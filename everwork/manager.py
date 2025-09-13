import asyncio
import signal
import time
from multiprocessing import context
from typing import Annotated, Literal
from uuid import uuid4

from loguru import logger
from orjson import dumps, loads
from pydantic import validate_call, AfterValidator, RedisDsn, BaseModel
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis

from everwork.process import Process
from everwork.process_wrapper import ProcessWrapper
from everwork.utils import SafeCancellationZone, CloseEvent
from everwork.worker import ExecutorMode, BaseWorker


def check_worker_names(processes: list[Process]) -> list[Process]:
    names = set()

    for process in processes:
        for worker in process.workers:
            if worker.settings.name in names:
                raise ValueError(f"{worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return processes


class ProcessState(BaseModel):
    status: Literal['waiting', 'running']
    end_time: float | None


class Manager:

    @validate_call
    def __init__(self, redis_dsn: RedisDsn, processes: Annotated[list[Process], AfterValidator(check_worker_names)]) -> None:
        self.__redis_dsn = redis_dsn
        self.__redis = Redis.from_url(
            url=redis_dsn.encoded_string(),
            protocol=3,
            decode_responses=True
        )

        self.__processes: list[Process] = processes

        self.__process_instances: dict[str, context.SpawnProcess] = {}
        self.__worker_definitions: dict[str, list[type[BaseWorker]]] = {}

        for process_data in self.__processes:
            for _ in range(process_data.replicas):
                self.__worker_definitions[str(uuid4())] = process_data.workers

        self.__close_event = CloseEvent()
        self.__tasks: list[tuple[asyncio.Task, SafeCancellationZone]] = []

        self.__scripts: dict[str, str] = {}

    def __set_closed_flag(self, *_) -> None:
        self.__close_event.set()

        for task, safe_cancellation_zone in self.__tasks:
            if safe_cancellation_zone.is_use():
                task.cancel()

    async def __init_process(self) -> None:
        await self.__redis.delete(*(f'process:{uuid}:state' for uuid in self.__worker_definitions.keys()))

    async def __init_workers(self) -> None:
        keys: list[str] = []

        for process in self.__processes:
            for worker in process.workers:
                keys.append(f'worker:{worker.settings.name}:is_worker_on')

        response = await self.__redis.mget(keys)

        value = {key: 0 for key, is_worker_on in zip(keys, response) if is_worker_on is None}

        if not value:
            return

        await self.__redis.mset(value)

    async def __register_limit_args(self) -> None:
        pipeline = self.__redis.pipeline()

        for process_data in self.__processes:
            for worker in process_data.workers:
                if not isinstance(worker.settings.mode, ExecutorMode):
                    continue

                if worker.settings.mode.limited_args is None:
                    continue

                await pipeline.delete(f'worker:{worker.settings.name}:taken_limit_args')
                await pipeline.delete(f'worker:{worker.settings.name}:limit_args')
                await pipeline.rpush(
                    f'worker:{worker.settings.name}:limit_args',
                    *(dumps(args) for args in worker.settings.mode.limited_args)
                )

        await pipeline.execute()

    def __create_process(self, uuid: str) -> context.SpawnProcess:
        process = context.SpawnProcess(
            target=ProcessWrapper.run,
            kwargs=to_jsonable_python({
                'uuid': uuid,
                'workers': self.__worker_definitions[uuid],
                'redis_dsn': self.__redis_dsn,
                'scripts': self.__scripts
            }),
            daemon=True
        )
        process.start()
        return process

    async def __monitor_process(self, uuid: str, safe_cancellation_zone: SafeCancellationZone) -> None:
        worker_names = ', '.join(worker.settings.name for worker in self.__worker_definitions[uuid])

        tasks: dict[str, asyncio.Task] = {}

        try:
            while not self.__close_event.is_set():
                with safe_cancellation_zone:
                    data = await self.__redis.brpop([f'process:{uuid}:state'])

                state = ProcessState.model_validate(loads(data[1]))

                if state.status == 'waiting':
                    continue

                # noinspection PyTypeChecker
                tasks = {
                    'timeout': asyncio.create_task(asyncio.sleep(state.end_time - time.time())),
                    'wait_complete': asyncio.create_task(self.__redis.brpop([f'process:{uuid}:state'])),
                }

                with safe_cancellation_zone:
                    await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)

                if tasks['wait_complete'].done():
                    if not tasks['timeout'].done():
                        tasks['timeout'].cancel()

                    continue

                if not tasks['wait_complete'].done():
                    tasks['wait_complete'].cancel()

                logger.warning(f'Начат перезапуск процесса. Состав: {worker_names}')

                self.__process_instances[uuid].terminate()
                logger.debug(f'Отправлен сигнал SIGTERM процессу. Состав: {worker_names}')

                end_time = time.time() + 3

                with safe_cancellation_zone:
                    while True:
                        if time.time() > end_time:
                            break

                        if not self.__process_instances[uuid].is_alive():
                            break

                        await asyncio.sleep(0.1)

                if self.__process_instances[uuid].is_alive():
                    self.__process_instances[uuid].kill()
                    logger.warning(f'Отправлен сигнал SIGKILL процессу. Состав: {worker_names}')

                self.__process_instances[uuid].join()
                self.__process_instances[uuid].close()

                with safe_cancellation_zone:
                    await self.__redis.delete(f'process:{uuid}:state')

                self.__process_instances[uuid] = self.__create_process(uuid)

                await asyncio.sleep(0.1)

                logger.warning(f'Завершен перезапуск процесса. Состав: {worker_names}')
        except asyncio.CancelledError:
            pass
        except Exception as error:
            logger.exception(f'Мониторинг процесса неожиданно завершился. Состав: {worker_names}. {error}')

        for task in tasks.values():
            if not task.done():
                task.cancel()

    async def __register_set_state_script(self) -> str:
        return await self.__redis.script_load(
            """
            local key = KEYS[1]
            local value = ARGV[1]

            redis.call("LTRIM", key, 1, 0)
            redis.call("LPUSH", key, value)
            """
        )

    async def run(self) -> None:
        logger.info('Manager запушен')

        signal.signal(signal.SIGINT, self.__set_closed_flag)
        signal.signal(signal.SIGTERM, self.__set_closed_flag)

        self.__scripts['set_state'] = await self.__register_set_state_script()

        logger.debug('Скрипты успешно зарегистрированы')

        await self.__init_process()

        logger.info('Процессы инициализированы')

        await self.__init_workers()
        await self.__register_limit_args()

        logger.info('Workers, limit args инициализированы')

        logger.info('Начато создание процессов')

        for uuid in self.__worker_definitions.keys():
            self.__process_instances[uuid] = self.__create_process(uuid)

        await asyncio.sleep(0.1)

        logger.info('Закончено создание процессов')

        for uuid in self.__worker_definitions.keys():
            safe_cancellation_zone = SafeCancellationZone(self.__close_event)
            self.__tasks.append((
                asyncio.create_task(self.__monitor_process(uuid, safe_cancellation_zone)),
                safe_cancellation_zone
            ))

        logger.info('Запушены задачи по отслеживанию работы процессов')

        await asyncio.gather(*map(lambda x: x[0], self.__tasks), return_exceptions=True)

        logger.info('Manager начал процесс завершения')

        for process in self.__process_instances.values():
            process.terminate()

        logger.debug('Процессам послан сигнал SIGTERM')

        end_time = time.time() + max(
            max(worker.settings.max_working_timeout for worker in process.workers) for process in self.__processes
        )

        while True:
            if time.time() > end_time:
                logger.warning('Процессы не успели завершиться')
                break

            if all(not process.is_alive() for process in self.__process_instances.values()):
                logger.debug('Процессы успешно завершились')
                break

            await asyncio.sleep(0.1)

        for process in self.__process_instances.values():
            process.kill()

        logger.debug('Процессам послан сигнал SIGKILL')

        for process in self.__process_instances.values():
            process.join()

        for process in self.__process_instances.values():
            process.close()

        logger.debug('Процессы закрыты')

        await self.__redis.close()

        logger.info('Manager завершен')
