from typing import Callable, Type


class _SimpleLazyFactory[T, **P]:
    __slots__ = ("_cls", "_args", "_kwargs")

    def __init__(self, cls: Type[T], args: tuple, kwargs: dict):
        self._cls = cls
        self._args = args
        self._kwargs = kwargs

    def __call__(self) -> T:
        return self._cls(*self._args, **self._kwargs)


def lazy_init[T, **P](cls: Type[T]) -> Callable[P, _SimpleLazyFactory]:

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> _SimpleLazyFactory:
        return _SimpleLazyFactory(cls, args, kwargs)

    return wrapper
