import hashlib
from functools import cached_property
from typing import Annotated, Any, final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from pydantic import AfterValidator, BaseModel, computed_field, Field


@final
class Interval(BaseModel):
    weeks: Annotated[int, Field(ge=0)] = 0
    days: Annotated[int, Field(ge=0)] = 0
    hours: Annotated[int, Field(ge=0)] = 0
    minutes: Annotated[int, Field(ge=0)] = 0
    seconds: Annotated[int, Field(ge=0)] = 0


def validate_cron_expression(expression: str) -> str:
    if not croniter.is_valid(expression):
        raise ValueError(f'Invalid cron expression: {expression}')

    return expression


def validate_timezone(timezone: str) -> str:
    try:
        ZoneInfo(timezone)
    except ZoneInfoNotFoundError:
        raise ValueError(f"Invalid timezone: '{timezone}'")

    return timezone


@final
class Cron(BaseModel):
    expression: Annotated[str, AfterValidator(validate_cron_expression)]
    timezone: Annotated[str, AfterValidator(validate_timezone)] = 'UTC'


@final
class Trigger(BaseModel):
    title: Annotated[str, Field(min_length=1, max_length=300)]
    description: Annotated[str, Field(max_length=1000)] = ''

    schedule: Interval | Cron
    kwargs: dict[str, Any] = Field(default_factory=dict)

    is_catchup: bool = True
    lifetime: float | None = None

    status_check_interval: Annotated[float, Field(gt=0.1, lt=3600)] = 60
    status_cache_ttl: Annotated[float | None, Field(ge=0)] = 5

    @computed_field
    @cached_property
    def id(self) -> str:
        return hashlib.sha256(self.title.encode()).hexdigest()[:16]
