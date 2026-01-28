from abc import ABC, abstractmethod
from typing import Any, Literal, Self

from everwork.schemas import Process


class AbstractBackend(ABC):

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

    # Менеджер

    @abstractmethod
    async def startup_manager(
        self,
        manager_uuid: str,
        processes: list[Process]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def shutdown_manager(
        self,
        manager_uuid: str
    ) -> None:
        raise NotImplementedError

    # Воркер

    @abstractmethod
    async def get_worker_status(
        self,
        manager_uuid: str,
        worker_name: str
    ) -> Literal['on', 'off']:
        raise NotImplementedError

    # Исполнитель воркера

    @abstractmethod
    async def mark_worker_executor_as_busy(
        self,
        manager_uuid: str,
        process_uuid: str,
        worker_name: str,
        event_identifier: Any
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def mark_worker_executor_as_available(
        self,
        manager_uuid: str,
        process_uuid: str
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def mark_worker_executor_for_reboot(
        self,
        manager_uuid: str,
        process_uuid: str
    ) -> None:
        raise NotImplementedError
