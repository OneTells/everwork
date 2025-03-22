from pydantic import BaseModel

from everwork.worker import BaseWorker, ExecutorMode


class Process(BaseModel):
    workers: list[type[BaseWorker]]
    replicas: int = 1


class Manager:

    def __init__(self, processes: list[Process]):
        self.__processes = processes

    async def register_limit_args(self):
        for process in self.__processes:
            for worker in process.workers:
                mode = worker.settings().mode

                if not isinstance(mode, ExecutorMode):
                    continue

                pass

    async def start(self):
        pass

    async def stop(self):
        pass
