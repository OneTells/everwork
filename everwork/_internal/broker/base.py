from abc import ABC, abstractmethod
from typing import Any, Iterable, Self

from everwork._internal.schemas import AckResponse, FailResponse, RejectResponse, Request, RetryResponse
from everwork.schemas import Event, WorkerSettings


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
    async def build(self, worker_settings: list[WorkerSettings]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, worker_settings: list[WorkerSettings]) -> None:
        raise NotImplementedError

    # Ивент

    @abstractmethod
    async def fetch(self, process_uuid: str, worker_slug: str, sources: Iterable[str]) -> Request:
        raise NotImplementedError

    @abstractmethod
    async def push(self, event: Event | list[Event]) -> None:
        raise NotImplementedError

    # Обработка ивента

    @abstractmethod
    async def ack(self, worker_slug: str, request: Request, response: AckResponse) -> None:
        raise NotImplementedError

    @abstractmethod
    async def fail(self, worker_slug: str, request: Request, response: FailResponse) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject(self, worker_slug: str, request: Request, response: RejectResponse) -> None:
        raise NotImplementedError

    @abstractmethod
    async def retry(self, worker_slug: str, request: Request, response: RetryResponse) -> None:
        raise NotImplementedError
