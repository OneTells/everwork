import asyncio
import signal
from itertools import chain
from multiprocessing import get_start_method
from platform import system
from typing import Annotated
from uuid import UUID

from loguru import logger
from orjson import loads, dumps
from pydantic import validate_call, AfterValidator, RedisDsn
from redis.asyncio import Redis

from .base_worker import ProcessGroup
from .process_supervisor import ProcessSupervisor
from .utils import ShutdownEvent


def _check_worker_names(process_groups: list[ProcessGroup]) -> list[ProcessGroup]:
    names = set()

    for process_group in process_groups:
        for worker in process_group.workers:
            if worker.settings.name in names:
                raise ValueError(f"{worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return process_groups


class ProcessManager:

    @validate_call
    def __init__(
        self,
        uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)],
        redis_dsn: RedisDsn,
        process_groups: Annotated[list[ProcessGroup], AfterValidator(_check_worker_names)]
    ) -> None:
        self.__uuid = uuid
        self.__redis_dsn = redis_dsn.encoded_string()
        self.__process_groups = process_groups

        self.__shutdown_event = ShutdownEvent()
        self.__process_supervisors: list[ProcessSupervisor] = []

        for process_group in self.__process_groups:
            for _ in range(process_group.replicas):
                self.__process_supervisors.append(
                    ProcessSupervisor(self.__redis_dsn, process_group.workers, self.__shutdown_event)
                )

    def __handle_shutdown_signal(self, *_) -> None:
        # logger.info('Получен сигнал о закрытии менеджера процессов')

        self.__shutdown_event.set()

        for process_supervisor in self.__process_supervisors:
            process_supervisor.close()

    async def __init_workers(self, redis: Redis) -> None:
        managers_data = await redis.get(f'managers:{self.__uuid}')
        old_workers_map: dict[str, list[str]] = {} if managers_data is None else loads(managers_data)

        new_workers_map = {
            w.settings.name: list(w.settings.source_streams) + [f'workers:{w.settings.name}:stream']
            for g in self.__process_groups for w in g.workers
        }

        async with redis.pipeline() as pipe:
            if old_worker_names := old_workers_map.keys() - new_workers_map.keys():
                await pipe.delete(
                    *(f'workers:{worker_name}:is_worker_on' for worker_name in old_worker_names),
                    *(f'workers:{worker_name}:last_time' for worker_name in old_worker_names),
                )

            if new_worker_names := new_workers_map.keys() - old_workers_map.keys():
                await pipe.mset({f'workers:{worker_name}:is_worker_on': 0 for worker_name in new_worker_names})

            await pipe.set(f'managers:{self.__uuid}', dumps(new_workers_map))
            await pipe.sadd('managers', self.__uuid)

            if new_workers_map:
                await pipe.sadd('streams', *(chain.from_iterable(new_workers_map.values())))

            await pipe.execute()

    async def __init_stream_groups(self, redis: Redis) -> None:
        async with redis.pipeline() as pipe:
            for process_group in self.__process_groups:
                for worker in process_group.workers:
                    for stream in (worker.settings.source_streams | {f'workers:{worker.settings.name}:stream'}):
                        if await redis.exists(stream):
                            groups = await redis.xinfo_groups(stream)

                            if any(group['name'] == worker.settings.name for group in groups):
                                continue

                        await pipe.xgroup_create(stream, worker.settings.name, mkstream=True)

            await pipe.execute()

    async def run(self) -> None:
        if system() != 'Linux':
            logger.critical('Библиотека работает только на Linux')
            return

        if get_start_method() not in ('spawn', 'forkserver'):
            logger.critical('Библиотека работает только с spawn или forkserver методом запуска процессов')
            return

        logger.info('Менеджер процессов запушен')

        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)

        async with Redis.from_url(self.__redis_dsn, protocol=3, decode_responses=True) as redis:
            await self.__init_workers(redis)
            await self.__init_stream_groups(redis)

        logger.info('Компоненты инициализированы')

        for process_supervisor in self.__process_supervisors:
            process_supervisor.run()

        logger.info('Наблюдатели процессов запущены')

        await asyncio.gather(*map(lambda x: x.task, self.__process_supervisors))

        logger.info('Менеджер процессов завершил работу')

        await logger.complete()
