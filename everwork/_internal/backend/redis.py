from datetime import datetime, UTC
from typing import Literal, Sequence

from orjson import dumps
from pydantic import AwareDatetime, RedisDsn
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff

from everwork._internal.backend.base import AbstractBackend
from everwork._internal.utils.lazy_wrapper import lazy_init
from everwork.schemas import Process


@lazy_init
class RedisBackend(AbstractBackend):

    def __init__(self, redis_dsn: RedisDsn) -> None:
        self._redis = Redis.from_url(
            redis_dsn.encoded_string(),
            retry=Retry(ConstantBackoff(0), 0),
            protocol=3,
            decode_responses=True
        )

    async def initialize(self) -> None:
        await self._redis.initialize()

    async def close(self) -> None:
        await self._redis.aclose()

    async def build(self, manager_uuid: str, processes: Sequence[Process]) -> None:
        async for key in self._redis.scan_iter(match=f'worker_executor:{manager_uuid}:*', count=100):
            await self._redis.delete(key)

        worker_statuses: list[str | None] = await self._redis.mget(
            worker_statuses_keys := {
                f'worker:{worker.settings.id}:status'
                for process in processes
                for worker in process.workers
            }
        )

        trigger_statuses: list[str | None] = await self._redis.mget(
            trigger_statuses_keys := {
                f'trigger:{worker.settings.id}:{trigger.id}:status'
                for process in processes
                for worker in process.workers
                for trigger in worker.settings.triggers
            }
        )

        worker_executor_status = dumps({'status': 'initializing', 'content': {}, 'set_at': datetime.now(UTC).isoformat()})

        await self._redis.mset(
            {key: 'off' for key, value in zip(worker_statuses_keys, worker_statuses) if value is None}
            | {key: 'on' for key, value in zip(trigger_statuses_keys, trigger_statuses) if value is None}
            | {f'worker_executor:{manager_uuid}:{p.uuid}:status': worker_executor_status for p in processes}
            | {
                f'manager:{manager_uuid}:structure': dumps(
                    {
                        'processes': to_jsonable_python(
                            [{**p.model_dump(), 'workers': [w.settings for w in p.workers]} for p in processes]
                        ),
                        'started_at': datetime.now(UTC)
                    }
                )
            }
            | {f'manager:{manager_uuid}:status': 'on'}
        )

        await self._redis.sadd('managers:structure', manager_uuid)

    async def cleanup(self, manager_uuid: str, processes: Sequence[Process]) -> None:
        await self._redis.mset({f'manager:{manager_uuid}:status': 'off'})

    async def get_worker_status(self, worker_id: str) -> Literal['on', 'off']:
        return await self._redis.get(f'worker:{worker_id}:status')

    async def mark_worker_executor_as_busy(self, manager_uuid: str, process_uuid: str, worker_id: str, event_id: str) -> None:
        await self._redis.set(
            f'worker_executor:{manager_uuid}:{process_uuid}:status',
            dumps(
                {
                    'status': 'busy',
                    'content': {'worker_id': worker_id, 'event_id': event_id},
                    'set_at': datetime.now(UTC).isoformat()
                }
            )
        )

    async def mark_worker_executor_as_available(self, manager_uuid: str, process_uuid: str) -> None:
        await self._redis.set(
            f'worker_executor:{manager_uuid}:{process_uuid}:status',
            dumps(
                {
                    'status': 'available',
                    'content': {},
                    'set_at': datetime.now(UTC).isoformat()
                }
            )
        )

    async def mark_worker_executor_for_reboot(self, manager_uuid: str, process_uuid: str) -> None:
        await self._redis.set(
            f'worker_executor:{manager_uuid}:{process_uuid}:status',
            dumps(
                {
                    'status': 'reboot',
                    'content': {},
                    'set_at': datetime.now(UTC).isoformat()
                }
            )
        )

    async def get_trigger_status(self, worker_id: str, trigger_id: str) -> Literal['on', 'off']:
        return await self._redis.get(f'trigger:{worker_id}:{trigger_id}:status')

    async def get_time_point(self, worker_id: str, trigger_id: str) -> AwareDatetime | None:
        payload: str | None = await self._redis.get(f'trigger:{worker_id}:{trigger_id}:time_point')
        return datetime.fromisoformat(payload) if payload is not None else None

    async def set_time_point(self, worker_id: str, trigger_id: str, time_point: AwareDatetime) -> None:
        await self._redis.set(f'trigger:{worker_id}:{trigger_id}:time_point', time_point.isoformat())
