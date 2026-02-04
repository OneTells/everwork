class EverworkError(Exception):
    ...


class TaskError(EverworkError):
    ...


class Reject(TaskError):

    def __init__(self, detail: str) -> None:
        self.detail = detail


class Retry(TaskError):
    ...
