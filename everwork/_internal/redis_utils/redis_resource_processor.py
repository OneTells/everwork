from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from everwork._internal.resource.resource_handler import Resources
from everwork.workers.base import AbstractWorker


class RedisResourceProcessor:

    def __init__(self, redis: Redis, worker: type[AbstractWorker]) -> None:
        self._redis = redis
        self._worker = worker

        self._scripts: dict[str, str] = {}

    async def _load_handle_cancel_script(self) -> None:
        self._scripts['handle_cancel'] = await self._redis.script_load(
            """
            local messages = redis.call('XRANGE', KEYS[1], KEYS[3], KEYS[3], 'COUNT', 1)
            if #messages > 0 then
                redis.call('XACK', KEYS[1], KEYS[2], KEYS[3])
                redis.call('XADD', 'workers:' .. KEYS[2] .. ':stream', '*', unpack(messages[1][2]))
            end
            """
        )

    async def initialize(self) -> None:
        await self._load_handle_cancel_script()

    async def handle_success(self, resources: Resources | None) -> None:
        if resources is None:
            return

        await self._redis.xack(resources.stream, self._worker.settings.name, resources.message_id)

    async def handle_cancel(self, resources: Resources | None) -> None:
        if resources is None:
            return

        keys = [resources.stream, self._worker.settings.name, resources.message_id]

        try:
            await self._redis.evalsha(self._scripts['handle_cancel'], 3, *keys)
        except NoScriptError:
            await self._load_handle_cancel_script()
            await self._redis.evalsha(self._scripts['handle_cancel'], 3, *keys)

    async def handle_error(self, resources: Resources | None) -> None:
        if resources is None:
            return

        logger.warning(
            f'({self._worker.settings.name}) Не удалось обработать сообщение из потока. '
            f'Поток: {resources.stream}. '
            f'ID сообщения: {resources.message_id}'
        )

        await self._redis.xack(resources.stream, self._worker.settings.name, resources.message_id)
