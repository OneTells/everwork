from multiprocessing import get_start_method
from platform import system

from loguru import logger

from everwork.schemas import Process, ProcessGroup


def check_environment() -> bool:
    current_system = system()

    if current_system != "Linux":
        logger.critical(
            f"Библиотека работает только на Linux. "
            f"Текущая система: {current_system}"
        )
        return False

    start_method = get_start_method()

    if start_method not in ("spawn", "forkserver"):
        logger.critical(
            f"Поддерживаемые методы запуска процессов: spawn, forkserver. "
            f"Текущий метод: {start_method}"
        )
        return False

    return True


def validate_worker_names(processes: list[ProcessGroup | Process]) -> list[ProcessGroup | Process]:
    names: set[str] = set()

    for item in processes:
        process = item.process if isinstance(item, ProcessGroup) else item

        for worker in process.workers:
            if worker.settings.name in names:
                raise ValueError(f"Имя воркера {worker.settings.name} не уникально")

            names.add(worker.settings.name)

    return processes


def expand_groups(processes: list[ProcessGroup | Process]) -> list[Process]:
    result: list[Process] = []

    for item in processes:
        if isinstance(item, Process):
            result.append(item)
            continue

        for _ in range(item.replicas):
            result.append(item.process.model_copy(deep=True))

    return result
