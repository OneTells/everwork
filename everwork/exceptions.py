class EverworkError(Exception):
    ...


class TaskError(EverworkError):
    ...


class Fail(TaskError):

    def __init__(self, detail: str) -> None:
        self.detail = detail


class Reject(TaskError):
    ...


class Retry(TaskError):
    ...
