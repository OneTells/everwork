import asyncio
from typing import Annotated, final
from uuid import UUID

from loguru import logger
from pydantic import AfterValidator, ConfigDict, RedisDsn, validate_call
from redis.backoff import AbstractBackoff, FullJitterBackoff

from everwork.schemas import Process, ProcessGroup
from ._internal.process_manager.redis_initializer import RedisInitializer
from ._internal.process_manager.signal_handler import SignalHandler
from ._internal.process_manager.utils import check_environment, expand_groups, validate_worker_names
from ._internal.process_supervisor.process_supervisor import ProcessSupervisor


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
