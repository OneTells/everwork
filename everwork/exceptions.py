class WorkerError(Exception):
    pass


class WorkerOff(WorkerError):
    pass


class WorkerExit(WorkerError):
    pass
