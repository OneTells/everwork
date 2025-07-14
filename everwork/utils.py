import asyncio


def timer(*, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000


class CloseEvent:

    def __init__(self):
        self.__is_close = False

    def get(self):
        return self.__is_close

    def set(self):
        self.__is_close = True


class SafeCancellationZone:

    def __init__(self, close_event: CloseEvent):
        self.__is_use = False
        self.__close_event = close_event

    def is_use(self):
        return self.__is_use

    def __enter__(self):
        self.__is_use = True

        if self.__close_event.get():
            self.__is_use = False
            raise asyncio.CancelledError()

        return self

    def __exit__(self, *_):
        self.__is_use = False
