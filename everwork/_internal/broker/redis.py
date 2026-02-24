import traceback
from itertools import chain
from typing import Sequence

from loguru import logger
from orjson import dumps, loads
from pydantic import RedisDsn
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff
from redis.exceptions import NoScriptError

from everwork._internal.broker.base import AbstractBroker
from everwork._internal.schemas import AckResponse, FailResponse, RejectResponse, Request, RetryResponse
from everwork._internal.utils.lazy_wrapper import lazy_init
from everwork.schemas.event import Event
from everwork.schemas.process import Process


@lazy_init
class RedisBroker(AbstractBroker):

    def __init__(self, redis_dsn: RedisDsn) -> None:
        self._redis = Redis.from_url(
            redis_dsn.encoded_string(),
            retry=Retry(ConstantBackoff(0), 0),
            protocol=3,
            decode_responses=True
        )

        self._scripts: dict[str, str] = {}

    async def initialize(self) -> None:
        await self._redis.initialize()

    async def close(self) -> None:
        await self._redis.aclose()

    async def build(self, processes: Sequence[Process]) -> None:
        stream_groups = {
            (source, worker.settings.id)
            for process in processes
            for worker in process.workers
            for source in worker.settings.sources
        }

        existing_groups: dict[str, set[str]] = {}

        for stream, _ in stream_groups:
            if stream in existing_groups:
                continue

            if not (await self._redis.exists(stream)):
                continue

            groups = await self._redis.xinfo_groups(stream)
            existing_groups[stream] = {group['name'] for group in groups}

        async with self._redis.pipeline() as pipe:
            for stream, group_name in stream_groups:
                if group_name in existing_groups.get(stream, set()):
                    continue

                await pipe.xgroup_create(stream, group_name, mkstream=True)

            await pipe.execute()

        logger.info(1)

    async def cleanup(self, processes: Sequence[Process]) -> None:
        return

    async def fetch(self, process_uuid: str, worker_id: str, sources: Sequence[str]) -> Request:
        data = await self._redis.xreadgroup(
            groupname=worker_id,
            consumername=process_uuid,
            streams={source: '>' for source in sources},
            count=1,
            block=0
        )

        event_id, event_kwargs = tuple(data.items())[0][1][0][0]

        return Request(event_id=event_id, event=Event.model_validate(loads(event_kwargs['payload'])))

    async def push(self, event: Event | Sequence[Event]) -> None:
        events = [event] if isinstance(event, Event) else event

        if not events:
            return

        if self._scripts.get('push') is None:
            await self._load_push_script()

        args = tuple(chain.from_iterable((event.source, dumps(to_jsonable_python(event))) for event in events))

        try:
            await self._redis.evalsha(self._scripts['push'], 0, *args)
        except NoScriptError:
            await self._load_push_script()
            await self._redis.evalsha(self._scripts['push'], 0, *args)

    async def ack(self, worker_id: str, request: Request, response: AckResponse) -> None:
        await self._redis.xack(request.event.source, worker_id, request.event_id)

    async def fail(self, worker_id: str, request: Request, response: FailResponse) -> None:
        if self._scripts.get('fail') is None:
            await self._load_fail_script()

        keys_and_args = [
            request.event.source,
            worker_id,
            request.event_id,
            dumps(
                {
                    'worker_id': worker_id,
                    'request': request.model_dump(mode='json'),
                    'response': {
                        'detail': response.detail,
                        'exception': "".join(
                            traceback.format_exception(
                                type(response.error),
                                response.error,
                                response.error.__traceback__
                            )
                        )
                    }
                }
            )
        ]

        try:
            await self._redis.evalsha(self._scripts['fail'], 1, *keys_and_args)
        except NoScriptError:
            await self._load_fail_script()
            await self._redis.evalsha(self._scripts['fail'], 1, *keys_and_args)

    async def reject(self, worker_id: str, request: Request, response: RejectResponse) -> None:
        if self._scripts.get('reject') is None:
            await self._load_reject_script()

        keys_and_args = [request.event.source, worker_id, request.event_id]

        try:
            await self._redis.evalsha(self._scripts['reject'], 1, *keys_and_args)
        except NoScriptError:
            await self._load_reject_script()
            await self._redis.evalsha(self._scripts['reject'], 1, *keys_and_args)

    async def retry(self, worker_id: str, request: Request, response: RetryResponse) -> None:
        if self._scripts.get('retry') is None:
            await self._load_retry_script()

        request.event.retries += 1

        keys_and_args = [
            f'worker:{worker_id}:source',
            request.event.source,
            worker_id,
            request.event_id,
            dumps(to_jsonable_python(request.event))
        ]

        try:
            await self._redis.evalsha(self._scripts['retry'], 2, *keys_and_args)
        except NoScriptError:
            await self._load_retry_script()
            await self._redis.evalsha(self._scripts['retry'], 2, *keys_and_args)

    async def _load_push_script(self) -> None:
        self._scripts['push'] = await self._redis.script_load(
            """
            for i = 1, #ARGV, 2 do
                local stream_key = ARGV[i]
                local event_data = ARGV[i+1]
                redis.call('XADD', stream_key, '*', 'payload', event_data)
            end
            """
        )

    async def _load_fail_script(self) -> None:
        self._scripts['fail'] = await self._redis.script_load(
            """
            redis.call('RPUSH', 'everwork:fails', ARGV[3])
            redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            """
        )

    async def _load_reject_script(self) -> None:
        self._scripts['reject'] = await self._redis.script_load(
            """
            redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            """
        )

    async def _load_retry_script(self) -> None:
        self._scripts['retry'] = await self._redis.script_load(
            """
            redis.call('XADD', KEYS[1], '*', 'payload', ARGV[3])
            redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
            """
        )
