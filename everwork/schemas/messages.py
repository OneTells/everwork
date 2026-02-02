from typing import Literal

from pydantic import BaseModel

from everwork.schemas import Event
from .._internal.utils.event_storage import AbstractReader


class Request(BaseModel):
    event_id: str
    event: Event


class Response(BaseModel):
    type: Literal['ack', 'fail', 'reject', 'retry']

    event: Event

    reader: AbstractReader | None = None
    error: BaseException | None = None
