from typing import Any, final

from pydantic import AwareDatetime

from everwork.schemas import Event
from .._internal.utils.event_storage import AbstractStorage


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
        event = Event(
            source=source,
            kwargs=kwargs,
            headers=headers,
            expires=expires
        )
        self._storage.write(event)

    def add_event(self, event: Event) -> None:
        self._storage.write(event)

    def add_events(self, events: list[Event]) -> None:
        self._storage.write(events)
