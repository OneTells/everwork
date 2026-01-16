from abc import ABC, abstractmethod
from typing import Any

from everwork.schemas import WorkerEvent


class AbstractBroker[T](ABC):

    @abstractmethod
    async def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def push_event(
        self,
        event: WorkerEvent | list[WorkerEvent]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fetch_event(
        self,
        worker_name: str,
        source_streams: str,
        worker_executor_id: str
    ) -> tuple[dict[str, Any], T]:
        raise NotImplementedError

    @abstractmethod
    async def ack_event(
        self,
        worker_name: str,
        worker_executor_id: str,
        event_identifier: T
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def requeue_event(
        self,
        worker_name: str,
        worker_executor_id: str,
        event_identifier: T
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject_event(
        self,
        worker_name: str,
        worker_executor_id: str,
        event_identifier: T
    ) -> None:
        raise NotImplementedError
