import asyncio
from typing import Any, Callable

from loguru import logger

from everwork._internal.resource.resource_supervisor import ResourceSupervisor
from everwork._internal.utils.single_value_channel import SingleValueChannel
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.schemas import Process


class ResourceManager:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        backend_factory: Callable[[], AbstractBackend],
        broker_factory: Callable[[], AbstractBroker],
        response_channel: SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: SingleValueChannel[BaseException | None],
        shutdown_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process
        self._backend_factory = backend_factory
        self._broker_factory = broker_factory
        self._response_channel = response_channel
        self._answer_channel = answer_channel
        self._shutdown_event = shutdown_event

    async def _run_supervisors(self) -> None:
        lock = asyncio.Lock()

        async with self._backend_factory() as backend:
            async with self._broker_factory() as broker:
                async with asyncio.TaskGroup() as task_group:
                    for worker in self._process.workers:
                        supervisor = ResourceSupervisor(
                            self._manager_uuid,
                            self._process,
                            worker,
                            backend,
                            broker,
                            self._response_channel,
                            self._answer_channel,
                            lock,
                            self._shutdown_event
                        )

                        task_group.create_task(supervisor.run())

    async def run(self) -> None:
        logger.debug(
            f'[{self._process.uuid}] Менеджер ресурсов запущен. '
            f'Состав: {', '.join(worker.settings.name for worker in self._process.workers)}'
        )

        logger.debug(f'[{self._process.uuid}] Менеджер ресурсов запустил все супервайзеры ресурсов')
        await self._run_supervisors()
        logger.debug(f'[{self._process.uuid}] Менеджер ресурсов завершил все супервайзеры ресурсов')

        self._response_channel.close()
        self._answer_channel.close()

        logger.debug(f'[{self._process.uuid}] Менеджер ресурсов завершил работу')
