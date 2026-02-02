from typing import Literal

from pydantic import BaseModel

from .._internal.utils.event_storage import AbstractReader


class Response(BaseModel):
    type: Literal['ack', 'fail', 'reject', 'retry']


class AckResponse(Response):
    type: Literal['ack'] = 'ack'

    reader: AbstractReader


class FailResponse(Response):
    type: Literal['fail'] = 'fail'

    description: str
    error: BaseException


class RejectResponse(Response):
    type: Literal['reject'] = 'reject'

    description: str


class RetryResponse(Response):
    type: Literal['retry'] = 'retry'
