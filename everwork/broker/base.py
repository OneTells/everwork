from abc import ABC, abstractmethod
from typing import Any, Iterable, Self

from everwork.schemas import Event, EventPayload, Process


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

    # Создание / удаление структуры

    @abstractmethod
    async def build(
        self,
        manager_uuid: str,
        processes: list[Process]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def cleanup(
        self,
        manager_uuid: str
    ) -> None:
        raise NotImplementedError

    # Ивент

    @abstractmethod
    async def fetch(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        sources: Iterable[str]
    ) -> Event:
        raise NotImplementedError

    @abstractmethod
    async def push(
        self,
        event_payload: EventPayload | list[EventPayload]
    ) -> None:
        raise NotImplementedError

    # Обработка ивента

    @abstractmethod
    async def ack(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event: Event
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fail(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event: Event,
        error: BaseException
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event: Event
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def retry(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event: Event
    ) -> None:
        raise NotImplementedError
