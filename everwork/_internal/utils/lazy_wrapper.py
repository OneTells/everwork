from typing import Callable, Type


def lazy_init[T, **P](cls: Type[T]) -> Callable[P, Callable[[], T]]:

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Callable[[], T]:
        def create_instance() -> T:
            return cls(*args, **kwargs)

        return create_instance

    return wrapper
