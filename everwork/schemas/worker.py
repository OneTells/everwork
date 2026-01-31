from functools import cached_property
from typing import Annotated, final, Self

from pydantic import BaseModel, computed_field, Field, model_validator
from slugify import slugify

from everwork.schemas import Trigger

type SourceType = Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]


@final
class EventSettings(BaseModel):
    max_events_in_memory: Annotated[int, Field(ge=1, le=10000)] = 5000
    max_batch_size: Annotated[int, Field(ge=1, le=10000)] = 500


@final
class WorkerSettings(BaseModel):
    title: Annotated[str, Field(min_length=1, max_length=300)]
    description: Annotated[str, Field(min_length=1, max_length=1000)]

    sources: set[SourceType] = Field(default_factory=set, max_length=100)

    execution_timeout: Annotated[float, Field(gt=0.1, lt=86400)] = 180
    worker_status_check_interval: Annotated[float, Field(gt=0.1, lt=3600)] = 60

    event_settings: EventSettings = Field(default_factory=EventSettings)

    triggers: list[Trigger] = Field(default_factory=list)

    @computed_field
    @cached_property
    def slug(self) -> str:
        return slugify(self.title, separator='_', regex_pattern=r'[^a-z0-9_]+')

    @computed_field
    @cached_property
    def default_source(self) -> str:
        return f'{self.slug}:source'

    @model_validator(mode='after')
    def _configure_sources(self) -> Self:
        if self.default_source not in self.sources:
            self.sources.add(self.default_source)

        return self
