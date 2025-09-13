from asyncio import CancelledError
from typing import Literal, Any, Dict, Self

from pydantic import BaseModel


def timer(*, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000


class CloseEvent:

    def __init__(self) -> None:
        self.__value = False

    def is_set(self) -> bool:
        return self.__value

    def set(self) -> None:
        self.__value = True


class SafeCancellationZone:

    def __init__(self, event: CloseEvent) -> None:
        self.__event = event
        self.__is_use = False

    def is_use(self) -> bool:
        return self.__is_use

    def __enter__(self) -> Self:
        if self.__event.is_set():
            raise CancelledError()

        self.__is_use = True
        return self

    def __exit__(self, *_) -> None:
        self.__is_use = False


class Resources(BaseModel):
    kwargs: Dict[str, Any] | None = None

    event_id: str | None
    event_raw: str | None

    limit_args_raw: str | None

    status: Literal['success', 'cancel', 'error']
