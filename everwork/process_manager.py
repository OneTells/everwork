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
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.exceptions import RedisError

from _utils import _IdentityEvent
from ._process_supervisor import _ProcessSupervisor
from .worker import ProcessGroup, WorkerSettings, Process


def _check_worker_names(processes: list[ProcessGroup | Process]) -> list[ProcessGroup | Process]:
    names = set()

    for process_or_group in processes:
        if isinstance(process_or_group, ProcessGroup):
            process = process_or_group.process
        else:
            process = process_or_group

        for worker in process.workers:
            if worker.settings.name in names:
                raise ValueError(f"{worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return processes


def _expand_process_groups(processes: list[ProcessGroup | Process]) -> list[Process]:
    result: list[Process] = []

    for process_or_group in processes:
        if isinstance(process_or_group, Process):
            result.append(process_or_group)
            continue

        for _ in range(process_or_group.replicas):
            result.append(process_or_group.process.model_copy(deep=True))

    return result


class ProcessManager:

    @validate_call
    def __init__(
        self,
        uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)],
        redis_dsn: RedisDsn,
        processes: Annotated[
            list[ProcessGroup | Process],
            AfterValidator(_check_worker_names),
            AfterValidator(_expand_process_groups)
        ]
    ) -> None:
        self.__uuid = uuid
        self.__redis_dsn = redis_dsn.encoded_string()
        self.__processes: list[Process] = processes

        self.__shutdown_event = _IdentityEvent()

    def __handle_shutdown_signal(self, *_) -> None:
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(self.__shutdown_event.set) # type: ignore

    async def __init_workers(self, redis: Redis) -> None:
        managers_data = await redis.get(f'managers:{self.__uuid}')
        old_workers_settings: dict[str, WorkerSettings] = (
            {} if managers_data is None else {k: WorkerSettings.model_validate(v) for k, v in loads(managers_data).items()}
        )

        new_workers_settings: dict[str, WorkerSettings] = {
            w.settings.name: w.settings for p in self.__processes for w in p.workers
        }

        async with redis.pipeline() as pipe:
            if old_worker_names := (old_workers_settings.keys() - new_workers_settings.keys()):
                await pipe.delete(
                    *(f'workers:{worker_name}:is_worker_on' for worker_name in old_worker_names),
                    *(f'workers:{worker_name}:last_time' for worker_name in old_worker_names),
                )

            if new_worker_names := (new_workers_settings.keys() - old_workers_settings.keys()):
                await pipe.mset({f'workers:{worker_name}:is_worker_on': 0 for worker_name in new_worker_names})

            await pipe.set(f'managers:{self.__uuid}', dumps(to_jsonable_python(new_workers_settings)))
            await pipe.sadd('managers', self.__uuid)

            if new_workers_settings:
                await pipe.sadd('streams', *chain.from_iterable(map(lambda x: x.source_streams, new_workers_settings.values())))

            await pipe.execute()

    async def __init_stream_groups(self, redis: Redis) -> None:
        stream_groups = set()

        for process in self.__processes:
            for worker in process.workers:
                for stream in worker.settings.source_streams:
                    stream_groups.add((stream, worker.settings.name))

        existing_groups: dict[str, set[str]] = {}

        for stream, _ in stream_groups:
            if stream in existing_groups:
                continue

            if not (await redis.exists(stream)):
                continue

            groups = await redis.xinfo_groups(stream)
            existing_groups[stream] = {group['name'] for group in groups}

        async with redis.pipeline() as pipe:
            for stream, group_name in stream_groups:
                if group_name in existing_groups.get(stream, set()):
                    continue

                await pipe.xgroup_create(stream, group_name, mkstream=True)

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

        try:
            async with Redis.from_url(self.__redis_dsn, protocol=3, decode_responses=True) as redis:
                await self.__init_workers(redis)
                await self.__init_stream_groups(redis)
        except RedisError as error:
            logger.critical(f'Ошибка при работе с redis в наблюдателе процессов: {error}')
            return

        logger.info('Компоненты инициализированы')

        async with asyncio.TaskGroup() as tg:
            for process in self.__processes:
                tg.create_task(_ProcessSupervisor(self.__redis_dsn, process, self.__shutdown_event).run())

            logger.info('Наблюдатели процессов запущены')

        logger.info('Менеджер процессов завершил работу')
