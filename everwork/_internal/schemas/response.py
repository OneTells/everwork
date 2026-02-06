from dataclasses import dataclass
from typing import Literal

from everwork._internal.utils.event_storage import AbstractReader


@dataclass(slots=True, frozen=True)
class Response:
    status: Literal['ack', 'fail', 'reject', 'retry']


@dataclass(slots=True, frozen=True)
class AckResponse(Response):
    reader: AbstractReader

    status: Literal['ack'] = 'ack'


@dataclass(slots=True, frozen=True)
class FailResponse(Response):
    detail: str
    error: BaseException

    status: Literal['fail'] = 'fail'


@dataclass(slots=True, frozen=True)
class RejectResponse(Response):
    detail: str

    status: Literal['reject'] = 'reject'


@dataclass(slots=True, frozen=True)
class RetryResponse(Response):
    status: Literal['retry'] = 'retry'
