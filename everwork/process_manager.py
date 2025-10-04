import asyncio
import signal
from typing import Annotated

from loguru import logger
from pydantic import validate_call, AfterValidator, RedisDsn
from redis.asyncio import Redis

from everwork.process_supervisor import ProcessSupervisor
from everwork.utils import ShutdownEvent
from everwork.worker_base import ProcessGroup

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


class ProcessManager:

    @validate_call
    def __init__(
        self,
        redis_dsn: RedisDsn,
        process_groups: Annotated[list[ProcessGroup], AfterValidator(check_worker_names)]
    ) -> None:
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
        logger.info('Вызван метод закрытия наблюдателя процесса')

        self.__shutdown_event.set()

        for process_supervisor in self.__process_supervisors:
            process_supervisor.close()

    async def __init_workers(self, redis: Redis) -> None:
        keys: list[str] = []

        for process_group in self.__process_groups:
            for worker in process_group.workers:
                keys.append(f'worker:{worker.settings.name}:is_worker_on')

        response = await redis.mget(keys)

        value = {key: 0 for key, is_worker_on in zip(keys, response) if is_worker_on is None}

        if value:
            await redis.mset(value)

        pipeline = redis.pipeline()

        for process_group in self.__process_groups:
            for worker in process_group.workers:
                for stream_name in (worker.settings.source_streams | {f'worker:{worker.settings.name}:stream'}):
                    groups = await redis.xinfo_groups(stream_name)

                    if any(group['name'] == worker.settings.name for group in groups):
                        continue

                    await pipeline.xgroup_create(stream_name, worker.settings.name, mkstream=True)

        await pipeline.execute()

    async def run(self) -> None:
        logger.info('Процесс менеджер запушен')

        signal.signal(signal.SIGINT, self.__handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self.__handle_shutdown_signal)

        async with Redis.from_url(url=self.__redis_dsn, protocol=3, decode_responses=True) as redis:
            await self.__init_workers(redis)

        logger.info('Инициализированы workers')

        for process_supervisor in self.__process_supervisors:
            process_supervisor.run()

        logger.info('Наблюдатели процесса запущены')

        await asyncio.gather(*map(lambda x: x.task, self.__process_supervisors))

        logger.info('Процесс менеджер завершил работу')
