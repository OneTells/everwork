from abc import ABC, abstractmethod
from typing import Any

from everwork import WorkerEvent


class AbstractBroker[T](ABC):

    @abstractmethod
    async def push_event(self, event: WorkerEvent | list[WorkerEvent]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fetch_event(self) -> tuple[dict[str, Any], T]:
        raise NotImplementedError

    @abstractmethod
    async def ack_event(self, data: T) -> None:
        raise NotImplementedError

    @abstractmethod
    async def requeue_event(self, data: T) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject_event(self, data: T) -> None:
        raise NotImplementedError
