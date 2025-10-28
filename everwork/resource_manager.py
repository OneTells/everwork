import asyncio
from threading import Thread
from typing import Any

from loguru import logger
from redis.asyncio import Redis

from .base_worker import BaseWorker
from .resource_supervisor import ResourceSupervisor
from .utils import SingleValueChannel

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class ResourceManager:

    def __init__(
        self,
        redis_dsn: str,
        workers: list[type[BaseWorker]],
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool],
        shutdown_event: asyncio.Event
    ) -> None:
        self.__redis_dsn = redis_dsn
        self.__workers = workers
        self.__response_channel = response_channel
        self.__answer_channel = answer_channel
        self.__shutdown_event = shutdown_event

        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

    async def run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Менеджер ресурсов запущен')

        self.__answer_channel.bind_to_event_loop(asyncio.get_running_loop())

        lock = asyncio.Lock()

        async with Redis.from_url(self.__redis_dsn, protocol=3, decode_responses=True) as redis:
            async with asyncio.TaskGroup() as tg:
                for worker in self.__workers:
                    tg.create_task(
                        ResourceSupervisor(
                            redis,
                            worker,
                            self.__response_channel,
                            self.__answer_channel,
                            lock,
                            self.__shutdown_event
                        ).run()
                    )

                logger.debug('Наблюдатели ресурсов запущены')

        self.__response_channel.close()
        self.__answer_channel.close()

        logger.debug(f'[{self.__worker_names}] Менеджер ресурсов завершил работу')


class ResourceManagerRunner:

    def __init__(
        self,
        redis_dsn: str,
        workers: list[type[BaseWorker]],
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool]
    ) -> None:
        self.__shutdown_event = asyncio.Event()
        self.__resource_manager = ResourceManager(redis_dsn, workers, response_channel, answer_channel, self.__shutdown_event)

        self.__loop = new_event_loop()
        self.__thread = Thread(target=self.__run)

    def __run(self) -> None:
        with asyncio.Runner(loop_factory=lambda: self.__loop) as runner:
            runner.run(self.__resource_manager.run())

    def start(self) -> None:
        self.__thread.start()

    def join(self) -> None:
        self.__thread.join()

    def cancel(self) -> None:
        # noinspection PyTypeChecker
        self.__loop.call_soon_threadsafe(self.__shutdown_event.set)
