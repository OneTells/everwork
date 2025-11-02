from typing import Annotated, Any, Self

from pydantic import BaseModel, Field, model_validator


class ExecutorMode(BaseModel):
    pass


class TriggerMode(BaseModel):
    execution_interval: Annotated[float, Field(gt=0)]


class EventPublisherSettings(BaseModel):
    max_batch_size: Annotated[int, Field(ge=1)] = 500


class WorkerSettings(BaseModel):
    name: Annotated[str, Field(min_length=1)]

    source_streams: set[str] = Field(default_factory=set)
    mode: ExecutorMode | TriggerMode

    event_publisher_settings: EventPublisherSettings = Field(default_factory=EventPublisherSettings)

    execution_timeout: Annotated[float, Field(gt=0)] = 180
    worker_poll_interval: Annotated[float, Field(gt=0)] = 60

    @model_validator(mode='after')
    def _configure_stream_sources(self) -> Self:
        self.source_streams.add(f'workers:{self.name}:stream')
        return self


class WorkerEvent(BaseModel):
    target_stream: Annotated[str, Field(min_length=1)]
    data: dict[str, Any] = Field(default_factory=dict)
