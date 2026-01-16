from abc import ABC, abstractmethod
from typing import Any, Literal, Self

from everwork.schemas import WorkerSettings


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
    async def initialize_manager(self, manager_uuid: str, worker_settings: list[WorkerSettings]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def set_manager_status(self, manager_uuid: str, status: Literal['on', 'off']) -> None:
        raise NotImplementedError

    # Воркер

    @abstractmethod
    async def get_worker_status(self, worker_name: str) -> Literal['on', 'off']:
        raise NotImplementedError

    @abstractmethod
    async def set_worker_status(self, worker_name: str, status: Literal['on', 'off']) -> None:
        raise NotImplementedError

    # Воркер исполнитель

    @abstractmethod
    async def initialize_worker_executor(self, worker_executor_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_worker_executor_status(self, worker_executor_id: str) -> Literal['working', 'free', 'reboot']:
        raise NotImplementedError

    @abstractmethod
    async def set_worker_executor_status(self, worker_executor_id: str, status: Literal['working', 'free', 'reboot']) -> None:
        raise NotImplementedError

    # Триггер

    @abstractmethod
    async def initialize_trigger(self, trigger_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_trigger_status(self, trigger_id: str) -> Literal['on', 'off']:
        raise NotImplementedError

    @abstractmethod
    async def set_trigger_status(self, trigger_id: str, status: Literal['on', 'off']) -> None:
        raise NotImplementedError
