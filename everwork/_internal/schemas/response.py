from typing import Literal

from pydantic import BaseModel

from everwork._internal.utils.event_storage import AbstractReader


class Response(BaseModel):
    status: Literal['ack', 'fail', 'reject', 'retry']


class AckResponse(Response):
    status: Literal['ack'] = 'ack'

    reader: AbstractReader


class FailResponse(Response):
    status: Literal['fail'] = 'fail'

    detail: str
    error: BaseException


class RejectResponse(Response):
    status: Literal['reject'] = 'reject'

    detail: str


class RetryResponse(Response):
    status: Literal['retry'] = 'retry'
