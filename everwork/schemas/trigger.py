from typing import Annotated, Any, final

from pydantic import BaseModel, Field


@final
class Interval(BaseModel):
    weeks: Annotated[int, Field(ge=0)] = 0
    days: Annotated[int, Field(ge=0)] = 0
    hours: Annotated[int, Field(ge=0)] = 0
    minutes: Annotated[int, Field(ge=0)] = 0
    seconds: Annotated[int, Field(ge=0)] = 0


@final
class Cron(BaseModel):
    expression: str


@final
class Trigger(BaseModel):
    title: Annotated[str, Field(min_length=1, max_length=300)]
    description: Annotated[str, Field(min_length=1, max_length=1000)]

    schedule: Interval | Cron
    kwargs: dict[str, Any] = Field(default_factory=dict)

    is_catchup: bool = True
    lifetime: float | None = None
