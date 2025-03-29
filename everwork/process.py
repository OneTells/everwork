from typing import Annotated, Self, Literal

from pydantic import BaseModel, Field, model_validator

from everwork.worker import BaseWorker, TriggerMode, ExecutorMode


class Timeout(BaseModel):
    inactive: Annotated[float, Field(ge=0, default=5)]
    active: Annotated[float, Field(ge=0, default=0.5)]
    active_lifetime: Annotated[float, Field(ge=0, default=60)]


class Settings(BaseModel):
    replicas: Annotated[int, Field(ge=1)]
    timeout: Annotated[Timeout, Field(default_factory=Timeout)]


class Process(BaseModel):
    workers: Annotated[list[type[BaseWorker]], Field()]
    settings: Annotated[Settings, Field()]

    @model_validator(mode='after')
    def check_replicas(self) -> Self:
        if self.settings.replicas == 1:
            if any(
                isinstance(worker.settings().mode, ExecutorMode)
                and worker.settings().mode.limited_args is not None for worker in self.workers
            ):
                raise ValueError('При использовании LimitArgs должна быть репликация')

            return self

        if any(isinstance(worker.settings().mode, TriggerMode) for worker in self.workers):
            raise ValueError('Репликация работает только в режиме ExecutorMode')

        if len(self.workers) > 1:
            raise ValueError('Репликация в режиме ExecutorMode не работает с несколькими worker')

        return self


class RedisSettings(BaseModel):
    host: Annotated[str, Field()]
    port: Annotated[int, Field()]
    password: Annotated[str, Field()]
    db: Annotated[str | int, Field()]


class ProcessState(BaseModel):
    status: Annotated[Literal['waiting', 'running'], Field()]
    end_time: Annotated[int | None, Field()]

    @model_validator(mode='after')
    def check_replicas(self) -> Self:
        if self.status == 'waiting' and self.end_time is not None:
            raise ValueError('В статусе waiting поле end_time должно быть None')

        if self.status == 'running' and self.end_time is None:
            raise ValueError('В статусе running поле end_time не должно быть None')

        return self
