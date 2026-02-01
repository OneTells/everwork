import inspect
from typing import Any, Callable, get_type_hints


class TypedArgumentResolver:

    def __init__(self, function: Callable[..., Any]):
        self._typed_params = get_type_hints(function)

        self._param_names = []
        self._defaults = {}

        for param_name, param in inspect.signature(function).parameters.items():
            self._param_names.append(param_name)

            if param.default != inspect.Parameter.empty:
                self._defaults[param_name] = param.default

    def get_kwargs(
        self,
        kwargs: dict[str, Any],
        reserved_objects: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        filtered_kwargs: dict[str, Any] = {}
        reserved_kwargs: dict[str, Any] = {}
        default_kwargs: dict[str, Any] = {}

        for param_name in self._param_names:
            reserved_obj = reserved_objects.get(param_name)

            if reserved_obj is not None:
                param_type = self._typed_params.get(param_name)

                if param_type is None or isinstance(reserved_obj, param_type):
                    reserved_kwargs[param_name] = reserved_obj
                    continue

                if param_name in kwargs:
                    filtered_kwargs[param_name] = kwargs[param_name]
                    continue

                reserved_kwargs[param_name] = reserved_obj
                continue

            if param_name in kwargs:
                filtered_kwargs[param_name] = kwargs[param_name]
                continue

            if param_name in self._defaults:
                default_kwargs[param_name] = self._defaults[param_name]
                continue

            raise TypeError(f"Отсутствует необходимый аргумент: '{param_name}'")

        return filtered_kwargs, reserved_kwargs, default_kwargs
