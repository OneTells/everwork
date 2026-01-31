from inspect import signature

from loguru import logger

from everwork.schemas import Process
from everwork.workers import AbstractWorker


class WorkerRegistry:

    def __init__(self, process: Process) -> None:
        self._process = process

        self._worker_instances: dict[str, AbstractWorker] = {}
        self._worker_call_signatures: dict[str, list[str]] = {}

    async def initialize(self) -> None:
        for worker_cls in self._process.workers:
            try:
                worker = worker_cls()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker_cls.settings.slug}) Ошибка при инициализации: {error}')
                continue

            self._worker_instances[worker.settings.slug] = worker
            self._worker_call_signatures[worker.settings.slug] = list(signature(worker.__call__).parameters.keys())

    async def startup_all(self) -> None:
        for worker in self._worker_instances.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.slug}) Ошибка при startup: {error}')

    async def shutdown_all(self) -> None:
        for worker in self._worker_instances.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.slug}) Ошибка при shutdown: {error}')

    def get_worker(self, name: str) -> AbstractWorker:
        return self._worker_instances[name]

    def get_worker_params(self, name: str) -> list[str]:
        return self._worker_call_signatures[name]
