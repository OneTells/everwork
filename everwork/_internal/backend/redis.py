import hashlib
from datetime import datetime, UTC
from typing import Literal

from orjson import dumps
from pydantic import AwareDatetime, RedisDsn
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff

from everwork._internal.backend.base import AbstractBackend
from everwork.schemas import Process


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

    async def build(self, manager_uuid: str, processes: list[Process]) -> None:
        worker_statuses: list[str | None] = await self._redis.mget(
            worker_statuses_keys := {
                f'worker:{worker.settings.slug}:status'
                for process in processes
                for worker in process.workers
            }
        )

        trigger_statuses: list[str | None] = await self._redis.mget(
            trigger_statuses_keys := {
                f'trigger:{worker.settings.slug}:{hashlib.sha256(dumps(trigger.model_dump())).hexdigest()}:status'
                for process in processes
                for worker in process.workers
                for trigger in worker.settings.triggers
            }
        )

        await self._redis.mset(
            {key: 'off' for key, value in zip(worker_statuses_keys, worker_statuses) if value is None}
            | {key: 'off' for key, value in zip(trigger_statuses_keys, trigger_statuses) if value is None}
            | {
                f'manager:{manager_uuid}': dumps(
                    {
                        'processes': to_jsonable_python([{**p, 'workers': [w.settings for w in p.workers]} for p in processes]),
                        'started_at': datetime.now(UTC)
                    }
                )
            }
            | {f'manager:{manager_uuid}:status': 'on'}
        )

        await self._redis.sadd('managers', manager_uuid)

    async def cleanup(self, manager_uuid: str, processes: list[Process]) -> None:
        await self._redis.mset({f'manager:{manager_uuid}:status': 'off'})

    async def get_worker_status(self, manager_uuid: str, worker_slug: str) -> Literal['on', 'off']:
        return await self._redis.get(f'worker:{worker_slug}:status')

    async def mark_worker_executor_as_busy(self, manager_uuid: str, process_uuid: str, worker_slug: str, event_id: str) -> None:
        await self._redis.set(
            f'worker_executor:{manager_uuid}:{process_uuid}:status',
            dumps(
                {
                    'status': 'busy',
                    'content': {'worker_slug': worker_slug, 'event_id': event_id},
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

    async def get_trigger_status(self, manager_uuid: str, worker_slug: str, trigger_hash: str) -> Literal['on', 'off']:
        return await self._redis.get(f'trigger:{worker_slug}:{trigger_hash}:status')

    async def get_time_point(self, manager_uuid: str, worker_slug: str, trigger_hash: str) -> AwareDatetime | None:
        payload: str | None = await self._redis.get(f'trigger:{worker_slug}:{trigger_hash}:time_point')
        return datetime.fromisoformat(payload) if payload is not None else None

    async def set_time_point(self, manager_uuid: str, worker_slug: str, trigger_hash: str, time_point: AwareDatetime) -> None:
        await self._redis.set(f'trigger:{worker_slug}:{trigger_hash}:time_point', time_point.isoformat())
