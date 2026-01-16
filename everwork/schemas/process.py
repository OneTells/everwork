from typing import Annotated, final, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from everwork.workers import AbstractWorker


@final
class Process(BaseModel):
    model_config = ConfigDict(frozen=True)

    workers: Annotated[list[type[AbstractWorker]], Field(min_length=1)]

    shutdown_timeout: Annotated[float, Field(gt=0, lt=180)] = 20


@final
class ProcessGroup(BaseModel):
    model_config = ConfigDict(frozen=True)

    process: Process
    replicas: Annotated[int, Field(ge=1, lt=300)] = 1

    @model_validator(mode='after')
    def _validate_replication_compatibility(self) -> Self:
        if self.replicas == 1:
            return self

        if len(self.process.workers) > 1:
            raise ValueError('Репликация поддерживается только для одного воркера')

        return self
