from abc import ABC, abstractmethod
from inspect import isabstract
from typing import Any, ClassVar

from pydantic import validate_call

from .schemas.worker import WorkerSettings


class AbstractWorker(ABC):
    settings: ClassVar[WorkerSettings]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if isabstract(cls) or hasattr(cls, "settings"):
            return

        settings = cls._get_settings()

        if not isinstance(cls.settings, WorkerSettings):
            raise TypeError(
                f"Метод _get_settings() должен возвращать экземпляр WorkerSettings. "
                f"Получено: {type(cls.settings)}"
            )

        cls.settings = settings
        cls.__call__ = validate_call(cls.__call__)

    @classmethod
    @abstractmethod
    def _get_settings(cls) -> WorkerSettings:
        raise NotImplementedError

    async def shutdown(self) -> None:
        pass

    async def startup(self) -> None:
        pass

    @abstractmethod
    async def __call__(self, **kwargs: Any) -> None:
        raise NotImplementedError
