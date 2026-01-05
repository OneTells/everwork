from abc import ABC, abstractmethod
from typing import Literal

from everwork.schemas import Process


class AbstractBackend(ABC):

    @abstractmethod
    async def initialize(self, manager_uuid: str, processes: list[Process]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_worker_status(self, name: str) -> Literal['on', 'off']:
        raise NotImplementedError
