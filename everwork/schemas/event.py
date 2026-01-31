from datetime import datetime, UTC
from typing import Annotated, Any, final

from pydantic import AwareDatetime, BaseModel, Field


@final
class EventPayload(BaseModel):
    source: Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]
    kwargs: dict[str, Any] = Field(default_factory=dict)
    expires: AwareDatetime | None = None

    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
