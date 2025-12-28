from typing import Annotated, final

from pydantic import BaseModel, ConfigDict, Field


@final
class EventStorageSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_events_in_memory: Annotated[int, Field(ge=1, le=10000)] = 5000


@final
class EventPublisherSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_batch_size: Annotated[int, Field(ge=1, le=10000)] = 500
