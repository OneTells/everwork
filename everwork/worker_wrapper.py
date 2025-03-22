import asyncio

from redis.asyncio import Redis

from everwork.exceptions import WorkerOff, WorkerExit
from everwork.shemes import ExecutorMode
from everwork.utils import get_is_worker_on, waiting_worker_timeout, trigger_timeout, set_last_work
from everwork.worker import Settings


class WorkerWrapper:

    def __init__(self, settings: Settings, redis: Redis):
        self.__settings = settings
        self.__redis = redis

    async def __run_trigger_mode(self):
        while True:
            try:
                is_worker_on = await get_is_worker_on(self.__redis, self.__settings.name)

                if not is_worker_on:
                    raise WorkerOff()

                await waiting_worker_timeout(self.__redis, self.__settings.name, self.__settings.timeout)

                is_worker_on = await get_is_worker_on(self.__redis, self.__settings.name)

                if not is_worker_on:
                    raise WorkerOff()

                is_triggering = trigger_timeout(self.__redis, self.__settings.name, self.__settings.mode.timeout)

                if not is_triggering:
                    raise WorkerExit()

                await set_last_work(self.__redis, self.__settings.name)

                pass
            except WorkerOff:
                await asyncio.sleep(60)

    async def __run_trigger_with_events_mode(self):
        pass

    async def __run_executor_mode(self):
        pass

    async def run(self) -> None:
        if isinstance(self.__settings.mode, ExecutorMode):
            return await self.__run_executor_mode()

        if self.__settings.mode.with_queue_events:
            return await self.__run_trigger_with_events_mode()

        return await self.__run_trigger_mode()
