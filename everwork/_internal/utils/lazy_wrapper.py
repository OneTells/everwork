import importlib
from typing import Any, Callable, Type


class _LazyFactory[T]:
    __slots__ = ("_module", "_class_name", "_args", "_kwargs")

    def __init__(self, module: str, class_name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self._module = module
        self._class_name = class_name
        self._args = args
        self._kwargs = kwargs

    def __call__(self) -> T:
        module_obj = importlib.import_module(self._module)
        cls = getattr(module_obj, self._class_name)
        return cls(*self._args, **self._kwargs)

    def __reduce__(self) -> tuple[Callable, tuple]:
        return _LazyFactory, (self._module, self._class_name, self._args, self._kwargs)


def lazy_init[T, **P](cls: Type[T]) -> Callable[P, _LazyFactory[T]]:

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> _LazyFactory[T]:
        return _LazyFactory(cls.__module__, cls.__name__, args, kwargs)

    return wrapper
