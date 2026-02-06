from typing import Any, Callable, Type


class _LazyFactory[T]:
    __slots__ = ("_cls", "_args", "_kwargs")

    def __init__(self, cls: Type[T], args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self._cls = cls
        self._args = args
        self._kwargs = kwargs

    def __call__(self) -> T:
        return self._cls(*self._args, **self._kwargs)

    def __reduce__(self) -> tuple[Callable, tuple]:
        return _LazyFactory, (self._cls, self._args, self._kwargs)


def lazy_init[T, **P](cls: Type[T]) -> Callable[P, _LazyFactory]:

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> _LazyFactory:
        return _LazyFactory(cls, args, kwargs)

    return wrapper
