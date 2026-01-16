from typing import Annotated, Any, final, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

type NameType = Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]


@final
class EventStorageSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_events_in_memory: Annotated[int, Field(ge=1, le=10000)] = 5000


@final
class EventPublisherSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_batch_size: Annotated[int, Field(ge=1, le=10000)] = 500


@final
class WorkerSettings(BaseModel):
    name: NameType

    source_streams: set[NameType] = Field(default_factory=set, max_length=100)

    execution_timeout: Annotated[float, Field(gt=0.1, lt=86400)] = 180
    worker_status_check_interval: Annotated[float, Field(gt=0.1, lt=3600)] = 60

    event_storage: EventStorageSettings = Field(default_factory=EventStorageSettings)
    event_publisher: EventPublisherSettings = Field(default_factory=EventPublisherSettings)

    @model_validator(mode='after')
    def _configure_stream_sources(self) -> Self:
        self.source_streams = {f'{self.name}:stream', *self.source_streams}
        return self


@final
class WorkerEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    stream: NameType
    data: dict[str, Any] = Field(default_factory=dict)
