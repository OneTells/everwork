from enum import Enum
from typing import Any

from pydantic import BaseModel


class ProcessSettings(BaseModel):
    replicas: int


class WorkerMode(Enum):
    trigger = "trigger"
    # trigger_with_queue_events = "trigger_with_queue_events"
    executor = "executor"


class FloodControlData(BaseModel):
    n_times_per_period: int

    hours: int = 0
    minutes: int = 0
    seconds: float = 0


class WorkerSettings(BaseModel):
    name: str
    mode: WorkerMode
    replicas: int

    trigger_timeout: float | None = None

    timeout_reset: float = 180
    inactive_timeout: float = 1
    active_mode_timeout: float = 0
    active_mode_lifetime: float = 60

    flood_control: list[FloodControlData] | None = None
    limited_args: list[dict[str, Any]] | None = None
