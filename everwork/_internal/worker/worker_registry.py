from loguru import logger

from everwork._internal.worker.utils.argument_resolver import TypedArgumentResolver
from everwork.schemas import Process
from everwork.workers import AbstractWorker


class WorkerRegistry:

    def __init__(self, process: Process) -> None:
        self._process = process

        self._instances: dict[str, AbstractWorker] = {}
        self._resolvers: dict[str, TypedArgumentResolver] = {}

    async def initialize(self) -> None:
        for worker_cls in self._process.workers:
            try:
                worker = worker_cls()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker_cls.settings.slug}) Ошибка при инициализации: {error}')
                continue

            self._instances[worker.settings.slug] = worker
            self._resolvers[worker.settings.slug] = TypedArgumentResolver(worker.__call__)

    async def startup_all(self) -> None:
        for worker in self._instances.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.slug}) Ошибка при startup: {error}')

    async def shutdown_all(self) -> None:
        for worker in self._instances.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.slug}) Ошибка при shutdown: {error}')

    def get_worker(self, name: str) -> AbstractWorker:
        return self._instances[name]

    def get_resolver(self, name: str) -> TypedArgumentResolver:
        return self._resolvers[name]
