from datetime import datetime, UTC
from typing import Any, Annotated

from pydantic import BaseModel, Field, AwareDatetime


class FloodControlSetting(BaseModel):
    n_times_per_period: Annotated[int, Field(ge=1)]

    hours: Annotated[int, Field(ge=0, default=0)]
    minutes: Annotated[int, Field(ge=0, default=0)]
    seconds: Annotated[float, Field(ge=0, default=0)]

    def get_interval_ns(self) -> int:
        return int((self.hours * 3600 + self.minutes * 60 + self.seconds) * 1e9)


class ExecutorMode(BaseModel):
    replicas: Annotated[int, Field(ge=1)]

    # flood_control: Annotated[list[FloodControlSetting], Field(default=None)]
    limited_args: Annotated[list[dict[str, Any]], Field(default=None)]


class TriggerMode(BaseModel):
    timeout: Annotated[float, Field(gt=0)]
    with_queue_events: Annotated[bool, Field(default=False)]


class Timeout(BaseModel):
    inactive: Annotated[float, Field(ge=0, default=5)]
    active: Annotated[float, Field(ge=0, default=0.5)]
    active_lifetime: Annotated[float, Field(ge=0, default=60)]


class Settings(BaseModel):
    name: Annotated[str, Field()]
    mode: Annotated[ExecutorMode | TriggerMode, Field()]
    timeout_reset: Annotated[float, Field(gt=0, default=180)]
    timeout: Annotated[Timeout, Field(default_factory=Timeout)]


class RedisSettings(BaseModel):
    host: Annotated[str, Field()]
    password: Annotated[str, Field()]
    db: Annotated[str | int, Field()]


class Event(BaseModel):
    target: Annotated[str, Field()]
    kwargs: Annotated[dict[str, Any], Field(default=None)]
    created_at: Annotated[AwareDatetime, Field(default_factory=lambda: datetime.now(UTC))]
