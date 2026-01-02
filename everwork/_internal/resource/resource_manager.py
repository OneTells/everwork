import asyncio
from threading import Thread
from typing import Any

from loguru import logger
from redis.asyncio import Redis

from everwork._internal.resource.resource_supervisor import ResourceSupervisor
from everwork._internal.utils.redis_retry import GracefulShutdownRetry
from everwork._internal.utils.single_value_channel import SingleValueChannel
from everwork.schemas import Process

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class ResourceCoordinator:

    def __init__(
        self,
        redis_dsn: str,
        process: Process,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool],
        shutdown_event: asyncio.Event
    ) -> None:
        self._redis_dsn = redis_dsn
        self._process = process
        self._response_channel = response_channel
        self._answer_channel = answer_channel
        self._shutdown_event = shutdown_event

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)

    async def _run_supervisors(self, redis: Redis, lock: asyncio.Lock) -> None:
        async with asyncio.TaskGroup() as task_group:
            for worker in self._process.workers:
                task_group.create_task(
                    ResourceSupervisor(
                        redis,
                        worker,
                        self._response_channel,
                        self._answer_channel,
                        lock,
                        self._shutdown_event
                    ).run()
                )

            logger.debug(f'[{self._worker_names}] Наблюдатели ресурсов запущены')

    async def run(self) -> None:
        logger.debug(f'[{self._worker_names}] Координатор ресурсов запущен')

        self._answer_channel.bind_to_event_loop(asyncio.get_running_loop())

        retry = GracefulShutdownRetry(self._process.redis_backoff_strategy, self._shutdown_event)
        lock = asyncio.Lock()

        async with Redis.from_url(self._redis_dsn, retry=retry, protocol=3, decode_responses=True) as redis:
            await self._run_supervisors(redis, lock)

        self._response_channel.close()
        self._answer_channel.close()

        logger.debug(f'[{self._worker_names}] Координатор ресурсов завершил работу')


class ResourceManager:

    def __init__(
        self,
        redis_dsn: str,
        process: Process,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool]
    ) -> None:
        self._shutdown_event = asyncio.Event()
        self._loop = new_event_loop()

        self._thread = Thread(
            target=self._run,
            kwargs={
                'redis_dsn': redis_dsn,
                'process': process,
                'response_channel': response_channel,
                'answer_channel': answer_channel,
                'shutdown_event': self._shutdown_event,
                'loop': self._loop
            }
        )

    def start(self) -> None:
        self._thread.start()

    def join(self) -> None:
        self._thread.join()

    def cancel(self) -> None:
        self._loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

    @staticmethod
    def _run(
        redis_dsn: str,
        process: Process,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[bool],
        shutdown_event: asyncio.Event,
        loop: asyncio.AbstractEventLoop
    ) -> None:
        with asyncio.Runner(loop_factory=lambda: loop) as runner:
            runner.run(ResourceCoordinator(redis_dsn, process, response_channel, answer_channel, shutdown_event).run())
