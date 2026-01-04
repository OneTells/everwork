from loguru import logger
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from everwork.schemas import Process


class RedisTaskChecker:

    def __init__(self, redis_dsn: str, process: Process) -> None:
        self._redis_dsn = redis_dsn
        self._process = process

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)

    async def _process_stream(self, stream: str, group_name: str, redis: Redis, pipe: Pipeline) -> None:
        pending_info = await redis.xpending(stream, group_name)

        if pending_info['pending'] == 0:
            return

        pending_messages = await redis.xpending_range(
            name=stream,
            groupname=group_name,
            min=pending_info['min'],
            max=pending_info['max'],
            count=pending_info['pending']
        )

        message_ids = (msg['message_id'] for msg in pending_messages)
        await pipe.xack(stream, group_name, *message_ids)

        for message in pending_messages:
            logger.info(message)
            logger.warning(
                f'[{self._worker_names}] Обнаружено зависшее сообщение. '
                f'Поток: {stream}. '
                f'Воркер (группа): {group_name}. '
                f'ID сообщения: {message["message_id"]}. '
                f'Время обработки (ms): {message["time_since_delivered"]}'
            )

    async def check_for_hung_tasks(self) -> None:
        try:
            async with Redis.from_url(self._redis_dsn, protocol=3, decode_responses=True) as redis:
                async with redis.pipeline() as pipe:
                    for worker in self._process.workers:
                        for stream in worker.settings.source_streams:
                            await self._process_stream(stream, worker.settings.name, redis, pipe)

                    await pipe.execute()
        except Exception as error:
            logger.error(f'[{self._worker_names}] Не удалось проверить зависшие сообщения: {error}')
