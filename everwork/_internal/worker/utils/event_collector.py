from typing import Any, final

from pydantic import AwareDatetime

from everwork._internal.utils.event_storage import AbstractStorage
from everwork.schemas import EventPayload


@final
class EventCollector:

    def __init__(self, storage: AbstractStorage) -> None:
        self._storage = storage

    def add(
        self,
        source: str,
        kwargs: dict[str, Any],
        headers: dict[str, Any] | None = None,
        expires: AwareDatetime | None = None
    ) -> None:
        event = EventPayload(
            source=source,
            kwargs=kwargs,
            headers=headers,
            expires=expires
        )
        self._storage.write(event)

    def add_event(self, event: EventPayload) -> None:
        self._storage.write(event)

    def add_events(self, events: list[EventPayload]) -> None:
        self._storage.write(events)
