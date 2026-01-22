import asyncio
from typing import Any

from everwork._internal.utils.external_executor import ExecutorReceiver
from everwork._internal.utils.heartbeat_notifier import HeartbeatNotifier
from everwork._internal.utils.single_value_channel import ChannelClosed
from everwork._internal.worker.worker_invoker import WorkerInvoker
from everwork._internal.worker.worker_registry import WorkerRegistry
from everwork.backend import AbstractBackend
from everwork.broker import AbstractBroker
from everwork.events import EventCollector, EventPublisher, HybridEventStorage
from everwork.schemas import Process
from everwork.workers import AbstractWorker


class WorkerTaskProcessor:

    def __init__(
        self,
        manager_uuid: str,
        process: Process,
        worker_registry: WorkerRegistry,
        receiver: ExecutorReceiver,
        backend: AbstractBackend,
        broker: AbstractBroker,
        storage: HybridEventStorage,
        notifier: HeartbeatNotifier,
        is_executing_event: asyncio.Event
    ) -> None:
        self._manager_uuid = manager_uuid
        self._process = process

        self._worker_registry = worker_registry

        self._receiver = receiver
        self._backend = backend
        self._storage = storage

        self._collector = EventCollector(storage)

        self._worker_invoker = WorkerInvoker(
            self._manager_uuid,
            self._process,
            notifier,
            EventPublisher(storage, broker),
            is_executing_event,
        )

    def _prepare_worker_kwargs(self, worker: AbstractWorker, kwargs_raw: dict[str, Any]) -> dict[str, Any]:
        worker_params = self._worker_registry.get_worker_params(worker.settings.name)

        kwargs = {k: v for k, v in kwargs_raw.items() if k in worker_params}

        if 'collector' in worker_params:
            self._storage.max_events_in_memory = worker.settings.event_storage.max_events_in_memory
            kwargs['collector'] = self._collector

        return kwargs

    async def process_task(self) -> bool:
        try:
            worker_name, kwargs_raw = await self._receiver.get_response()
        except ChannelClosed:
            return False

        worker = self._worker_registry.get_worker(worker_name)
        kwargs = self._prepare_worker_kwargs(worker, kwargs_raw)

        await self._backend.mark_worker_executor_as_busy(self._manager_uuid, self._process.uuid, worker.settings.name)

        error_answer = await self._worker_invoker.execute(worker, kwargs)
        self._receiver.send_answer(error_answer)

        await self._backend.mark_worker_executor_as_available(self._manager_uuid, self._process.uuid)

        return True
