from typing import Any

from everwork._internal.utils.single_value_channel import SingleValueChannel


class ExecutorTransmitter:

    def __init__(
        self,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[BaseException | None]
    ) -> None:
        self._response_channel = response_channel
        self._answer_channel = answer_channel

    async def execute(self, worker_name: str, kwargs: dict[str, Any]) -> BaseException | None:
        self._response_channel.send((worker_name, kwargs))
        return await self._answer_channel.receive()

    def close(self) -> None:
        self._response_channel.close()
        self._answer_channel.close()


class ExecutorReceiver:

    def __init__(
        self,
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[BaseException | None]
    ) -> None:
        self._response_channel = response_channel
        self._answer_channel = answer_channel

    async def get_response(self) -> tuple[str, dict[str, Any]]:
        return await self._response_channel.receive()

    def send_answer(self, answer: BaseException | None) -> None:
        self._answer_channel.send(answer)
