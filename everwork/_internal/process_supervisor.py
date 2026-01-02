import asyncio
import time
from multiprocessing import connection

from loguru import logger
from orjson import loads
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from everwork._internal.worker_executor import WorkerProcess
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
            logger.warning(
                f'[{self._worker_names}] Обнаружено зависшее сообщение. '
                f'Поток: {stream}. '
                f'Воркер (группа): {group_name}. '
                f'ID сообщения: {message["message_id"]}. '
                f'Время обработки (ms): {message["elapsed"]}'
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


async def wait_for_pipe_data(
    pipe_connection: connection.Connection,
    shutdown_event: asyncio.Event,
    timeout: float | None = None
) -> bool:
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    shutdown_task = loop.create_task(shutdown_event.wait())

    def callback() -> None:
        if not future.done() and pipe_connection.poll(0):
            future.set_result(True)

    fd = pipe_connection.fileno()
    loop.add_reader(fd, callback)  # type: ignore

    try:
        await asyncio.wait(
            (shutdown_task, future),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        loop.remove_reader(fd)

        if not shutdown_task.done():
            shutdown_task.cancel()

    return future.done()


class EventStartMessage(BaseModel):
    worker_name: str
    end_time: float


class ProcessSupervisor:

    def __init__(self, redis_dsn: str, process: Process, shutdown_event: asyncio.Event) -> None:
        self._redis_dsn = redis_dsn
        self._process = process
        self._shutdown_event = shutdown_event

        self._worker_names = ', '.join(worker.settings.name for worker in process.workers)
        self._worker_process = WorkerProcess(self._redis_dsn, self._process)

        self._task_checker = RedisTaskChecker(self._redis_dsn, self._process)

    async def _restart_worker_manager(self, worker_name: str) -> None:
        logger.warning(f'[{self._worker_names}] Воркер {worker_name} завис. Начат перезапуск процесса')

        await self._worker_process.close()
        await self._task_checker.check_for_hung_tasks()

        if self._shutdown_event.is_set():
            logger.warning(f'[{self._worker_names}] Процесс завершен')
            return

        await self._worker_process.start()
        logger.warning(f'[{self._worker_names}] Процесс перезапущен')

    async def _run_monitoring_cycle(self) -> None:
        while not self._shutdown_event.is_set():
            payload = self._worker_process.pipe_reader.recv_bytes()
            state = EventStartMessage.model_validate(loads(payload))

            is_exist_message = await wait_for_pipe_data(
                self._worker_process.pipe_reader,
                self._shutdown_event,
                state.end_time - time.time()
            )

            if self._shutdown_event.is_set():
                return

            if is_exist_message:
                self._worker_process.pipe_reader.recv_bytes()
                continue

            await self._restart_worker_manager(state.worker_name)

    async def run(self) -> None:
        logger.debug(f'[{self._worker_names}] Запущен наблюдатель процесса')

        await self._task_checker.check_for_hung_tasks()
        await self._worker_process.start()

        try:
            await self._run_monitoring_cycle()
        except Exception as error:
            logger.critical(f'[{self._worker_names}] Наблюдатель процесса завершился с ошибкой: {error}')

        logger.debug(f'[{self._worker_names}] Начато завершение наблюдателя процесса')

        await self._worker_process.close()
        await self._task_checker.check_for_hung_tasks()

        logger.debug(f'[{self._worker_names}] Наблюдатель процесса завершил работу')
