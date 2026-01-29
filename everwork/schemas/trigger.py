from datetime import timedelta
from typing import Annotated, Any, final

from croniter import croniter
from pydantic import AfterValidator, BaseModel, ConfigDict, Field


@final
class Interval(BaseModel):
    model_config = ConfigDict(frozen=True)

    weeks: Annotated[int, Field(ge=0)] = 0
    days: Annotated[int, Field(ge=0)] = 0
    hours: Annotated[int, Field(ge=0)] = 0
    minutes: Annotated[int, Field(ge=0)] = 0
    seconds: Annotated[int, Field(ge=0)] = 0


@final
class CronTab(BaseModel):
    model_config = ConfigDict(frozen=True)

    expression: Annotated[str, AfterValidator(lambda v: croniter.is_valid(v))]


@final
class Trigger(BaseModel):
    model_config = ConfigDict(frozen=True)

    schedule: Interval | CronTab
    kwargs: dict[str, Any] = Field(default_factory=dict)

    is_catchup: bool = True
    lifetime: timedelta | None = None
