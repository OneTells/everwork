from inspect import signature
from typing import Any, Callable, get_type_hints


def check_method_typing(method: Callable[..., Any]) -> str | None:
    params = list(signature(method).parameters.values())
    type_hints = get_type_hints(method)

    problems = []

    for param in params[1:]:
        if param.kind == param.VAR_POSITIONAL:
            problems.append(f"нельзя использовать *{param.name}")
        elif param.kind == param.VAR_KEYWORD:
            problems.append(f"нельзя использовать **{param.name}")
        elif param.name not in type_hints:
            problems.append(f"параметр '{param.name}' без типа")

    if problems:
        return ', '.join(problems)

    return None
