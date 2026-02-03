from inspect import signature
from typing import Any, Callable


def check_method_typing(method: Callable[..., Any]) -> None:
    for param in signature(method).parameters.values():
        if param.kind == param.VAR_POSITIONAL:
            raise TypeError(f"Нельзя использовать позиционные аргументы")
