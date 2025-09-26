import hashlib
import signal
from typing import Annotated
from uuid import uuid4

from loguru import logger
from orjson import dumps
from pydantic import validate_call, AfterValidator, RedisDsn
from redis.asyncio import Redis

from everwork.process_supervisor import ProcessSupervisor
from everwork.utils import ShutdownEvent
from everwork.worker_base import BaseWorker, ExecutorMode, ProcessGroup, TriggerMode

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


def check_worker_names(process_groups: list[ProcessGroup]) -> list[ProcessGroup]:
    names = set()

    for process_group in process_groups:
        for worker in process_group.workers:
            if worker.settings.name in names:
                raise ValueError(f"{worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return process_groups


class Manager:

    @validate_call
    def __init__(
        self,
        redis_dsn: RedisDsn,
        process_groups: Annotated[list[ProcessGroup], AfterValidator(check_worker_names)]
    ) -> None:
        self.__redis_dsn = redis_dsn
        self.__process_groups = process_groups

        self.__redis = Redis.from_url(
            url=redis_dsn.encoded_string(),
            protocol=3,
            decode_responses=True
        )

        self.__worker_definitions: dict[str, list[type[BaseWorker]]] = {}

        for process_group in self.__process_groups:
            for _ in range(process_group.replicas):
                self.__worker_definitions[str(uuid4())] = process_group.workers

        self.__shutdown_event = ShutdownEvent()
        self.__process_supervisors: list[ProcessSupervisor] = []

        self.__scripts: dict[str, str] = {}

    def __set_shutdown_flag(self, *_) -> None:
        self.__shutdown_event.set()

        for process_supervisor in self.__process_supervisors:
            process_supervisor.close()

    async def __init_process(self) -> None:
        await self.__redis.delete(*(f'process:{uuid}:state' for uuid in self.__worker_definitions.keys()))

    async def __init_workers(self) -> None:
        keys: list[str] = []

        for process_group in self.__process_groups:
            for worker in process_group.workers:
                keys.append(f'worker:{worker.settings.name}:is_worker_on')

        response = await self.__redis.mget(keys)

        value = {key: 0 for key, is_worker_on in zip(keys, response) if is_worker_on is None}

        if value:
            await self.__redis.mset(value)

        for process_group in self.__process_groups:
            for worker in process_group.workers:
                if isinstance(worker.settings.mode, TriggerMode) and worker.settings.mode.source_streams is None:
                    continue

                for stream_name in (worker.settings.mode.source_streams | {f'worker:{worker.settings.name}:stream'}):
                    groups = await self.__redis.xinfo_groups(stream_name)

                    if any(group['name'] == worker.settings.name for group in groups):
                        continue

                    await self.__redis.xgroup_create(stream_name, worker.settings.name, mkstream=True)

    async def __register_limit_args(self) -> None:
        pipeline = self.__redis.pipeline()

        for process_group in self.__process_groups:
            for worker in process_group.workers:
                if not isinstance(worker.settings.mode, ExecutorMode) or worker.settings.mode.limited_args is None:
                    continue

                await pipeline.delete(f'worker:{worker.settings.name}:taken_limit_args')
                await pipeline.delete(f'worker:{worker.settings.name}:limit_args')
                await pipeline.rpush(
                    f'worker:{worker.settings.name}:limit_args',
                    *(hashlib.sha256(dumps(args)).hexdigest() for args in worker.settings.mode.limited_args)
                )

        await pipeline.execute()

    async def __register_set_state_script(self) -> str:
        return await self.__redis.script_load(
            """
            local key = KEYS[1]
            local value = ARGV[1]

            redis.call("LTRIM", key, 1, 0)
            redis.call("LPUSH", key, value)
            """
        )

    async def __register_handle_error_script(self) -> str:
        raise NotImplementedError

    async def __register_handle_cancel_script(self) -> str:
        raise NotImplementedError

    async def __register_handle_success_script(self) -> str:
        raise NotImplementedError

    async def run(self) -> None:
        logger.info('Manager запушен')

        signal.signal(signal.SIGINT, self.__set_shutdown_flag)
        signal.signal(signal.SIGTERM, self.__set_shutdown_flag)
        logger.debug('Установлены обработчики сигналов')

        self.__scripts['set_state'] = await self.__register_set_state_script()
        self.__scripts['handle_error'] = await self.__register_handle_error_script()
        self.__scripts['handle_cancel'] = await self.__register_handle_cancel_script()
        self.__scripts['handle_success'] = await self.__register_handle_success_script()
        logger.debug('Зарегистрированы скрипты')

        await self.__init_process()
        logger.info('Инициализированы процессы')

        await self.__init_workers()
        logger.info('Инициализированы workers')

        await self.__register_limit_args()
        logger.info('Инициализированы limit args')

        logger.info('Начато создание наблюдателей')

        for uuid, workers in self.__worker_definitions.items():
            process_supervisor = ProcessSupervisor(uuid, workers, self.__shutdown_event, self.__redis_dsn, self.__scripts)
            process_supervisor.run()

            self.__process_supervisors.append(process_supervisor)

        logger.info('Наблюдатели запущены')

        for process_supervisor in self.__process_supervisors:
            process_supervisor.wait()

        self.__redis.close()
        logger.debug('Менеджер закрыл Redis')

        logger.info('Менеджер успешно завершил работу')
