from inspect import signature

from loguru import logger

from everwork.schemas import Process
from everwork.workers import AbstractWorker


class WorkerRegistry:

    def __init__(self, process: Process) -> None:
        self._process = process

        self._workers: dict[str, AbstractWorker] = {}
        self._worker_params: dict[str, list[str]] = {}

    async def initialize(self) -> None:
        for worker_cls in self._process.workers:
            try:
                worker = worker_cls()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker_cls.settings.name}) Ошибка при инициализации: {error}')
                continue

            self._workers[worker.settings.name] = worker
            self._worker_params[worker.settings.name] = list(signature(worker.__call__).parameters.keys())

    async def startup_all(self) -> None:
        for worker in self._workers.values():
            try:
                await worker.startup()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Ошибка при startup: {error}')

    async def shutdown_all(self) -> None:
        for worker in self._workers.values():
            try:
                await worker.shutdown()
            except Exception as error:
                logger.exception(f'[{self._process.uuid}] ({worker.settings.name}) Ошибка при shutdown: {error}')

    def get_worker(self, name: str) -> AbstractWorker:
        return self._workers[name]

    def get_worker_params(self, name: str) -> list[str]:
        return self._worker_params[name]
