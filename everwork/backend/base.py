from abc import ABC, abstractmethod
from typing import Any, Literal, Self

from pydantic import AwareDatetime

from everwork.schemas import Event, Process


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
        event: Event
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

    # Триггеры

    @abstractmethod
    async def get_time_point(
        self,
        manager_uuid: str,
        worker_name: str,
        trigger_hash: str
    ) -> AwareDatetime | None:
        raise NotImplementedError

    @abstractmethod
    async def set_time_point(
        self,
        manager_uuid: str,
        worker_name: str,
        trigger_hash: str,
        time_point: AwareDatetime
    ) -> None:
        raise NotImplementedError
