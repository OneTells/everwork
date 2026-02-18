import hashlib
from functools import cached_property
from typing import Annotated, final, Self, Sequence

from pydantic import BaseModel, computed_field, Field, model_validator

from everwork.schemas.trigger import Trigger

type SourceType = Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]


@final
class EventSettings(BaseModel):
    max_events_in_memory: Annotated[int, Field(ge=1, le=10000)] = 5000
    max_batch_size: Annotated[int, Field(ge=1, le=10000)] = 500


@final
class WorkerSettings(BaseModel):
    title: Annotated[str, Field(min_length=1, max_length=300)]
    description: Annotated[str, Field(max_length=1000)] = ''

    sources: set[SourceType] = Field(default_factory=set, max_length=100)

    execution_timeout: Annotated[float, Field(gt=0.1, lt=86400)] = 180
    status_check_interval: Annotated[float, Field(gt=0.1, lt=3600)] = 60

    event_settings: EventSettings = Field(default_factory=EventSettings)

    triggers: Sequence[Trigger] = Field(default_factory=tuple)

    @computed_field
    @cached_property
    def id(self) -> str:
        return hashlib.sha256(self.title.encode()).hexdigest()[:16]

    @computed_field
    @cached_property
    def default_source(self) -> str:
        return f'worker:{self.id}:source'

    @model_validator(mode='after')
    def _configure_sources(self) -> Self:
        if self.default_source not in self.sources:
            self.sources.add(self.default_source)

        return self
