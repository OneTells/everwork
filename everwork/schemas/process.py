from typing import Annotated, final, Self, Sequence
from uuid import UUID, uuid4

from pydantic import AfterValidator, BaseModel, Field, model_validator

from everwork.workers.base import AbstractWorker


@final
class Process(BaseModel):
    uuid: Annotated[str, AfterValidator(lambda x: UUID(x) and x)] = Field(default_factory=lambda: str(uuid4()))
    workers: Annotated[Sequence[type[AbstractWorker]], Field(min_length=1)]

    shutdown_timeout: Annotated[float, Field(gt=0, lt=180)] = 30

    @model_validator(mode='after')
    def _validate_workers(self) -> Self:
        if len(set(self.workers)) != len(self.workers):
            raise ValueError('Все воркеры процесса должны быть уникальными')

        return self


@final
class ProcessGroup(BaseModel):
    process: Process
    replicas: Annotated[int, Field(ge=1, lt=300)] = 1
