from typing import Annotated, Self, Literal, Any

from pydantic import BaseModel, Field, model_validator

from everwork.worker import BaseWorker, TriggerMode, ExecutorMode


class Process(BaseModel):
    workers: list[type[BaseWorker]]
    replicas: Annotated[int, Field(ge=1)] = 1

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.replicas == 1:
            if any(
                isinstance(worker.settings().mode, ExecutorMode)
                and worker.settings().mode.limited_args is not None for worker in self.workers
            ):
                raise ValueError('При использовании LimitArgs должна быть репликация')

            return self

        if any(isinstance(worker.settings().mode, TriggerMode) for worker in self.workers):
            raise ValueError('Репликация работает только с workers в режиме ExecutorMode')

        if len(self.workers) > 1:
            raise ValueError('Репликация не работает с несколькими worker в режиме ExecutorMode')

        return self


class RedisSettings(BaseModel):
    host: str
    port: int
    password: str
    db: str | int
    protocol: Annotated[int, Field(ge=2, le=3)] = 3
    decode_responses: bool = True


class ProcessState(BaseModel):
    status: Literal['waiting', 'running']
    end_time: float | None

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.status == 'waiting' and self.end_time is not None:
            raise ValueError('В статусе waiting поле end_time должно быть None')

        if self.status == 'running' and self.end_time is None:
            raise ValueError('В статусе running поле end_time не должно быть None')

        return self


class Resources(BaseModel):
    kwargs: dict[str, Any] | None = None

    event_id: str | None = None
    event_raw: str | None = None

    limit_args_raw: str | None = None

    status: Literal['success', 'cancel', 'error'] = 'success'


def check_worker_names(processes: list[Process]) -> list[Process]:
    worker_names = []

    for process in processes:
        worker_names += [worker.settings().name for worker in process.workers]

    if len(worker_names) != len(set(worker_names)):
        raise ValueError('Все имена workers должны быть уникальны')

    return processes
