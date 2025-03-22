from abc import ABC, abstractmethod

from everwork.shemes import Settings, Event


class BaseWorker(ABC):

    @staticmethod
    @abstractmethod
    def settings() -> Settings:
        raise NotImplementedError

    @abstractmethod
    async def startup(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __call__(self, *args, **kwargs) -> list[Event] | None:
        raise NotImplementedError
