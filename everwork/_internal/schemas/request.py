from pydantic import BaseModel

from everwork.schemas import Event


class Request(BaseModel):
    event_id: str
    event: Event
