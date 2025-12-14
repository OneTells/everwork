from typing import Annotated, Any, final, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

type StreamName = Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]


@final
class ExecutorMode(BaseModel):
    model_config = ConfigDict(frozen=True)


@final
class TriggerMode(BaseModel):
    model_config = ConfigDict(frozen=True)

    execution_interval: Annotated[float, Field(gt=0)]


@final
class WorkerSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: StreamName
    source_streams: set[StreamName] = Field(default_factory=set, max_length=100)
    mode: ExecutorMode | TriggerMode

    execution_timeout: Annotated[float, Field(gt=0.1, lt=86400)] = 180
    worker_status_check_interval: Annotated[float, Field(gt=0.1, lt=3600)] = 60

    @model_validator(mode='after')
    def _configure_stream_sources(self) -> Self:
        self.source_streams = {f'workers:{self.name}:stream', *self.source_streams}
        return self


@final
class WorkerEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    target_stream: StreamName
    data: dict[str, Any] = Field(default_factory=dict)
