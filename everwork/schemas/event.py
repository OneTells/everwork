from datetime import datetime, UTC
from typing import Annotated, Any, final

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

type NameType = Annotated[str, Field(min_length=1, max_length=300, pattern=r'^[a-zA-Z0-9_\-:]+$')]


@final
class EventPayload(BaseModel):
    model_config = ConfigDict(frozen=True)

    stream: NameType
    kwargs: dict[str, Any] = Field(default_factory=dict)
    expires: AwareDatetime

    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
