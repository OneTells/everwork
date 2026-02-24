from loguru import logger

from everwork._internal.worker.utils.argument_resolver import ArgumentResolver
from everwork.schemas.process import Process
from everwork.workers.base import AbstractWorker


class WorkerRegistry:

    def __init__(self, process: Process) -> None:
        self._process = process

        self._instances: dict[str, AbstractWorker] = {}
        self._resolvers: dict[str, ArgumentResolver] = {}

    async def initialize(self) -> None:
        for worker_cls in self._process.workers:
            try:
                worker = worker_cls()
            except Exception as error:
                logger.exception(f'({worker_cls.settings.id}) Ошибка при инициализации: {error}')
                continue

            self._instances[worker.settings.id] = worker
            self._resolvers[worker.settings.id] = ArgumentResolver(worker.__call__)

    async def startup_all(self) -> None:
        for worker in self._instances.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'({worker.settings.id}) Ошибка при startup: {error}')

    async def shutdown_all(self) -> None:
        for worker in self._instances.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'({worker.settings.id}) Ошибка при shutdown: {error}')

    def get_worker(self, name: str) -> AbstractWorker:
        return self._instances[name]

    def get_resolver(self, name: str) -> ArgumentResolver:
        return self._resolvers[name]
