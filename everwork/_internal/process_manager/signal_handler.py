import signal
from asyncio import Event, get_running_loop


class SignalHandler:

    def __init__(self, shutdown_event: Event):
        self._shutdown_event = shutdown_event

    def _handle_signal(self, *_) -> None:
        loop = get_running_loop()
        loop.call_soon_threadsafe(self._shutdown_event.set)  # type: ignore

    def register(self) -> None:
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
