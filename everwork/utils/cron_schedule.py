from abc import ABC, abstractmethod
from datetime import datetime

from croniter import croniter
from pydantic import AwareDatetime


class AbstractCronSchedule(ABC):

    def __init__(self, expression: str) -> None:
        self._expression = expression

    @abstractmethod
    def get_next(self, time_point: AwareDatetime) -> AwareDatetime:
        raise NotImplementedError


class CronSchedule(AbstractCronSchedule):

    def __init__(self, expression: str) -> None:
        super().__init__(expression)
        self._cron = croniter(self._expression)

    def get_next(self, time_point: AwareDatetime) -> AwareDatetime:
        return self._cron.get_next(ret_type=datetime, start_time=time_point, update_current=False)
