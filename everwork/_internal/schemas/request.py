from pydantic import BaseModel

from everwork.schemas.event import Event


class Request(BaseModel):
    event_id: str
    event: Event
