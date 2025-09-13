from typing import Annotated, Self

from pydantic import BaseModel, Field, model_validator

from everwork.worker import BaseWorker, TriggerMode, ExecutorMode


class Process(BaseModel):
    workers: list[type[BaseWorker]]
    replicas: Annotated[int, Field(ge=1)] = 1

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.replicas == 1:
            if any(
                isinstance(worker.settings.mode, ExecutorMode)
                and worker.settings.mode.limited_args is not None for worker in self.workers
            ):
                raise ValueError('При использовании LimitArgs должна быть репликация')

            return self

        if any(isinstance(worker.settings.mode, TriggerMode) for worker in self.workers):
            raise ValueError('Репликация работает только с workers в режиме ExecutorMode')

        if len(self.workers) > 1:
            raise ValueError('Репликация не работает с несколькими worker в режиме ExecutorMode')

        return self
