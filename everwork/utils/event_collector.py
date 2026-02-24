from typing import Any, final, Sequence

from pydantic import AwareDatetime

from .._internal.utils.event_storage import AbstractStorage
from ..schemas.event import Event


@final
class EventCollector:

    def __init__(self, storage: AbstractStorage) -> None:
        self._storage = storage

    def add(
        self,
        source: str,
        kwargs: dict[str, Any],
        expires: AwareDatetime | None = None
    ) -> None:
        event = Event(
            source=source,
            kwargs=kwargs,
            expires=expires
        )
        self._storage.write(event)

    def add_event(self, event: Event) -> None:
        self._storage.write(event)

    def add_events(self, events: Sequence[Event]) -> None:
        self._storage.write(events)
