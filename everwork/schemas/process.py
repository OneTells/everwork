from typing import Annotated, final
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from everwork.workers import AbstractWorker


@final
class Process(BaseModel):
    model_config = ConfigDict(frozen=True)

    uuid: UUID = uuid4()
    workers: Annotated[list[type[AbstractWorker]], Field(min_length=1)]

    shutdown_timeout: Annotated[float, Field(gt=0, lt=180)] = 20


@final
class ProcessGroup(BaseModel):
    model_config = ConfigDict(frozen=True)

    process: Process
    replicas: Annotated[int, Field(ge=1, lt=300)] = 1
