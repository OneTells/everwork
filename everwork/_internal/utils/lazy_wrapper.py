from typing import Callable, ParamSpec, Type, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def lazy_init(cls: Type[T]) -> Callable[P, Callable[[], T]]:

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Callable[[], T]:

        def create_instance() -> T:
            return cls(*args, **kwargs)

        return create_instance

    return wrapper
