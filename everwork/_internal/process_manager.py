import asyncio
import signal
from asyncio import Event, get_running_loop
from itertools import chain
from multiprocessing import get_start_method
from platform import system
from typing import Annotated, final
from uuid import UUID

from loguru import logger
from orjson import dumps, loads
from pydantic import AfterValidator, ConfigDict, RedisDsn, validate_call
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.backoff import AbstractBackoff, FullJitterBackoff
from redis.exceptions import RedisError

from everwork._internal.process_supervisor import ProcessSupervisor
from everwork._internal.utils.redis_retry import GracefulShutdownRetry
from everwork.schemas import Process, ProcessGroup
from everwork.workers.base import WorkerSettings


def check_environment() -> bool:
    current_system = system()

    if current_system != "Linux":
        logger.critical(
            f"Библиотека работает только на Linux. "
            f"Текущая система: {current_system}"
        )
        return False

    start_method = get_start_method()

    if start_method not in ("spawn", "forkserver"):
        logger.critical(
            f"Поддерживаемые методы запуска процессов: spawn, forkserver. "
            f"Текущий метод: {start_method}"
        )
        return False

    return True


def validate_worker_names(processes: list[ProcessGroup | Process]) -> list[ProcessGroup | Process]:
    names: set[str] = set()

    for item in processes:
        process = item.process if isinstance(item, ProcessGroup) else item

        for worker in process.workers:
            if worker.settings.name in names:
                raise ValueError(f"Имя воркера {worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return processes


def expand_groups(processes: list[ProcessGroup | Process]) -> list[Process]:
    result: list[Process] = []

    for item in processes:
        if isinstance(item, Process):
            result.append(item)
            continue

        for _ in range(item.replicas):
            result.append(item.process.model_copy(deep=True))

    return result


class SignalHandler:

    def __init__(self, shutdown_event: Event):
        self._shutdown_event = shutdown_event

    def _handle_signal(self, *_) -> None:
        loop = get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

    def register(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)


class RedisInitializer:

    def __init__(
        self,
        manager_uuid: str,
        processes: list[Process],
        redis_dsn: str,
        redis_backoff_strategy: AbstractBackoff,
        shutdown_event: Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._processes = processes
        self._redis_dsn = redis_dsn
        self._redis_backoff_strategy = redis_backoff_strategy
        self._shutdown_event = shutdown_event

    async def _init_workers(self, redis: Redis) -> None:
        old_data = await redis.get(f'managers:{self._manager_uuid}')

        old_workers: dict[str, WorkerSettings] = (
            {} if old_data is None else {
                k: WorkerSettings.model_validate(v) for k, v in loads(old_data).items()
            }
        )

        workers: dict[str, WorkerSettings] = {
            worker.settings.name: worker.settings
            for process in self._processes
            for worker in process.workers
        }

        async with redis.pipeline() as pipe:
            if old_worker_names := (old_workers.keys() - workers.keys()):
                await pipe.delete(
                    *(f'workers:{worker_name}:is_worker_on' for worker_name in old_worker_names),
                    *(f'workers:{worker_name}:last_time' for worker_name in old_worker_names),
                )

            if new_worker_names := (workers.keys() - old_workers.keys()):
                await pipe.mset({f'workers:{worker_name}:is_worker_on': 0 for worker_name in new_worker_names})

            await pipe.set(f'managers:{self._manager_uuid}', dumps(to_jsonable_python(workers)))
            await pipe.sadd('managers', self._manager_uuid)

            if workers:
                await pipe.sadd(
                    'streams',
                    *chain.from_iterable(settings.source_streams for settings in workers.values())
                )

            await pipe.execute()

    async def _init_stream_groups(self, redis: Redis) -> None:
        stream_groups = {
            (stream, worker.settings.name)
            for process in self._processes
            for worker in process.workers
            for stream in worker.settings.source_streams
        }

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

    async def initialize(self) -> None:
        retry = GracefulShutdownRetry(self._redis_backoff_strategy, self._shutdown_event)

        try:
            async with Redis.from_url(self._redis_dsn, retry=retry, protocol=3, decode_responses=True) as redis:
                await self._init_workers(redis)
                await self._init_stream_groups(redis)
        except RedisError as error:
            logger.critical(f'Ошибка при работе с redis в наблюдателе процессов: {error}')
            raise


@final
class ProcessManager:

    @validate_call(config=ConfigDict(arbitrary_types_allowed=True))
    def __init__(
        self,
        *,
        uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)],
        processes: Annotated[
            list[ProcessGroup | Process],
            AfterValidator(validate_worker_names),
            AfterValidator(expand_groups)
        ],
        redis_dsn: RedisDsn,
        redis_backoff_strategy: AbstractBackoff = FullJitterBackoff(cap=30.0, base=1.0)
    ) -> None:
        self._uuid = uuid
        self._processes: list[Process] = processes
        self._redis_dsn = redis_dsn.encoded_string()
        self._redis_backoff_strategy = redis_backoff_strategy

    async def _initialize_components(self, shutdown_event: asyncio.Event) -> None:
        initializer = RedisInitializer(
            self._uuid,
            self._processes,
            self._redis_dsn,
            self._redis_backoff_strategy,
            shutdown_event
        )
        await initializer.initialize()

    async def _start_supervisors(self, shutdown_event: asyncio.Event) -> None:
        async with asyncio.TaskGroup() as task_group:
            for process in self._processes:
                supervisor = ProcessSupervisor(self._redis_dsn, process, shutdown_event)
                task_group.create_task(supervisor.run())

            logger.info('Наблюдатели процессов запущены')

    async def run(self) -> None:
        if not check_environment():
            return

        logger.info('Менеджер процессов запущен')

        shutdown_event = asyncio.Event()
        SignalHandler(shutdown_event).register()

        await self._initialize_components(shutdown_event)
        logger.info('Компоненты инициализированы')

        await self._start_supervisors(shutdown_event)
        logger.info('Менеджер процессов завершил работу')
