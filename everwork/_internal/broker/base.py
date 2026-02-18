from abc import ABC, abstractmethod
from typing import Any, Self, Sequence

from everwork._internal.schemas import AckResponse, FailResponse, RejectResponse, Request, RetryResponse
from everwork.schemas import Event, Process


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

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # Создание / удаление структуры

    @abstractmethod
    async def build(self, processes: Sequence[Process]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, processes: Sequence[Process]) -> None:
        raise NotImplementedError

    # Ивент

    @abstractmethod
    async def fetch(self, process_uuid: str, worker_id: str, sources: Sequence[str]) -> Request:
        raise NotImplementedError

    @abstractmethod
    async def push(self, event: Event | Sequence[Event]) -> None:
        raise NotImplementedError

    # Обработка ивента

    @abstractmethod
    async def ack(self, worker_id: str, request: Request, response: AckResponse) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fail(self, worker_id: str, request: Request, response: FailResponse) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject(self, worker_id: str, request: Request, response: RejectResponse) -> None:
        raise NotImplementedError

    @abstractmethod
    async def retry(self, worker_id: str, request: Request, response: RetryResponse) -> None:
        raise NotImplementedError
