import asyncio
from typing import Any

from loguru import logger
from redis.asyncio import Redis

from .base_worker import BaseWorker
from .resource_supervisor import ResourceSupervisor
from .utils import ThreadSafeEventChannel

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop, get_running_loop


class ResourceManager:

    def __init__(
        self,
        redis_dsn: str,
        workers: list[type[BaseWorker]],
        response_channel: ThreadSafeEventChannel[tuple[str, dict[str, Any]]],
        answer_channel: ThreadSafeEventChannel[bool]
    ) -> None:
        self.__redis = Redis.from_url(redis_dsn, protocol=3, decode_responses=True)

        self.__workers = workers
        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__response_channel = response_channel
        self.__answer_channel = answer_channel

        self.__shutdown_event = asyncio.Event()

        self.__loop: asyncio.AbstractEventLoop | None = None

    async def __run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Менеджер ресурсов запущен')

        self.__answer_channel.set_loop(get_running_loop())

        lock = asyncio.Lock()

        async with self.__redis:
            async with asyncio.TaskGroup() as tg:
                for worker in self.__workers:
                    tg.create_task(
                        ResourceSupervisor(
                            self.__redis,
                            worker,
                            self.__response_channel,
                            self.__answer_channel,
                            lock,
                            self.__shutdown_event
                        ).run()
                    )

                logger.debug('Наблюдатели ресурсов запущены')

        self.__response_channel.cancel()
        self.__answer_channel.cancel()

        logger.debug(f'[{self.__worker_names}] Менеджер ресурсов завершил работу')

    def run(self) -> None:
        with asyncio.Runner(loop_factory=new_event_loop) as runner:
            self.__loop = runner.get_loop()
            runner.run(self.__run())

    def cancel(self) -> None:
        # noinspection PyTypeChecker
        self.__loop.call_soon_threadsafe(self.__shutdown_event.set)
