from abc import ABC, abstractmethod
from inspect import isabstract
from typing import Any, ClassVar

from pydantic import ConfigDict, validate_call

from everwork.schemas import WorkerSettings
from .._internal.utils.typing_checker import check_method_typing


class AbstractWorker(ABC):
    settings: ClassVar[WorkerSettings]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if isabstract(cls) or hasattr(cls, "settings"):
            return

        settings = cls._get_settings()

        if not isinstance(settings, WorkerSettings):
            raise TypeError(f"({cls.__name__}) Настройки должны возвращать экземпляр WorkerSettings")

        problems = check_method_typing(cls.__call__)

        if problems is not None:
            raise TypeError(f"({cls.__name__}) Используются невалидные параметры: {problems}")

        cls.settings = settings
        cls.__call__ = validate_call(config=ConfigDict(arbitrary_types_allowed=True))(cls.__call__)

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
