from datetime import datetime, UTC
from typing import Annotated, Any

from pydantic import AwareDatetime, BaseModel, Field


class Event(BaseModel):
    source: Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]
    kwargs: dict[str, Any] = Field(default_factory=dict)

    headers: dict[str, Any] = Field(default_factory=dict)
    expires: AwareDatetime | None = None

    retries: int = 0
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
