from typing import Annotated, final, Self
from uuid import UUID, uuid4

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from everwork.workers import AbstractWorker


@final
class Process(BaseModel):
    model_config = ConfigDict(frozen=True)

    uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)] = Field(default_factory=lambda: str(uuid4()))
    workers: Annotated[list[type[AbstractWorker]], Field(min_length=1)]

    shutdown_timeout: Annotated[float, Field(gt=0, lt=180)] = 30

    @model_validator(mode='after')
    def _validate_workers(self) -> Self:
        if list(set(self.workers)) != self.workers:
            raise ValueError('Все воркеры процесса должны быть уникальными')

        return self


@final
class ProcessGroup(BaseModel):
    model_config = ConfigDict(frozen=True)

    process: Process
    replicas: Annotated[int, Field(ge=1, lt=300)] = 1
