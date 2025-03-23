from typing import Annotated, Self

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
            return self

        if any(isinstance(worker.settings().mode, TriggerMode) for worker in self.workers):
            raise ValueError('Репликация работает только в режиме ExecutorMode')

        if len([worker for worker in self.workers if isinstance(worker.settings().mode, ExecutorMode)]) > 1:
            raise ValueError('Репликация в режиме ExecutorMode не работает с несколькими worker')

        return self


class RedisSettings(BaseModel):
    host: Annotated[str, Field()]
    port: Annotated[int, Field()]
    password: Annotated[str, Field()]
    db: Annotated[str | int, Field()]
