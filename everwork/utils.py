from asyncio import CancelledError
from typing import Self


def timer(*, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000


class ShutdownEvent:

    def __init__(self) -> None:
        self.__value = False

    def is_set(self) -> bool:
        return self.__value

    def set(self) -> None:
        self.__value = True


class ShutdownSafeZone:

    def __init__(self, shutdown_event: ShutdownEvent) -> None:
        self.__shutdown_event = shutdown_event
        self.__is_use = False

    def is_use(self) -> bool:
        return self.__is_use

    def __enter__(self) -> Self:
        if self.__shutdown_event.is_set():
            raise CancelledError()

        self.__is_use = True
        return self

    def __exit__(self, *_) -> None:
        self.__is_use = False
