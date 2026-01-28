from abc import ABC, abstractmethod
from typing import Any, Iterable, Self

from everwork.schemas import EventPayload


class AbstractBroker(ABC):

    @abstractmethod
    async def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        await self.initialize()
        return self

    async def __aexit__(self, _: Any) -> None:
        await self.close()

    # Ивент

    @abstractmethod
    async def push_event(
        self,
        event: EventPayload | list[EventPayload]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fetch_event(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        source_streams: Iterable[str]
    ) -> tuple[dict[str, Any], str]:
        raise NotImplementedError

    @abstractmethod
    async def ack_event(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event_id: str
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def return_event(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event_id: str
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fail_event(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event_id: str,
        error: BaseException
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject_event(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event_id: str
    ) -> None:
        raise NotImplementedError
