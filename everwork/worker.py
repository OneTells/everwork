from abc import ABC, abstractmethod

from everwork.settings import WorkerSettings


class BaseWorker(ABC):

    @staticmethod
    @abstractmethod
    def settings() -> WorkerSettings:
        raise NotImplementedError

    @abstractmethod
    async def startup(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __call__(self) -> list | None:
        raise NotImplementedError
