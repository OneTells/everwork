from abc import ABC
from typing import Annotated, final, Literal

from pydantic import BaseModel, ConfigDict, Field


class AbstractTrigger(BaseModel, ABC):
    model_config = ConfigDict(frozen=True)

    type: Literal['interval', 'cron']


@final
class IntervalTrigger(AbstractTrigger):
    type: Literal['interval'] = 'interval'

    weeks: Annotated[int, Field(ge=1)] = 0
    days: Annotated[int, Field(ge=1)] = 0
    hours: Annotated[int, Field(ge=1)] = 0
    minutes: Annotated[int, Field(ge=1)] = 0
    seconds: Annotated[int, Field(ge=1)] = 0
