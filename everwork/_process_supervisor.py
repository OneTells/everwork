import asyncio
import time
from multiprocessing import Pipe, connection

from loguru import logger
from orjson import loads
from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import RedisError

from ._worker_manager import _WorkerManagerRunner
from .worker import Process


class _EventStartMessage(BaseModel):
    worker_name: str
    end_time: float


async def _wait_for_data(
    pipe_connection: connection.Connection,
    shutdown_event: asyncio.Event,
    timeout: float | None = None
) -> bool:
    if timeout is not None and timeout <= 0:
        return pipe_connection.poll(0)

    loop = asyncio.get_running_loop()

    future = loop.create_future()
    shutdown_task = asyncio.create_task(shutdown_event.wait())

    def callback() -> None:
        if not future.done() and pipe_connection.poll(0):
            future.set_result(True)

    loop.add_reader(pipe_connection.fileno(), callback)  # type: ignore

    try:
        await asyncio.wait((shutdown_task, future), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        loop.remove_reader(pipe_connection.fileno())

        if not shutdown_task.done():
            shutdown_task.cancel()

    return future.done()


class _ProcessSupervisor:

    def __init__(self, redis_dsn: str, process: Process, shutdown_event: asyncio.Event) -> None:
        self.__redis_dsn = redis_dsn
        self.__process = process
        self.__shutdown_event = shutdown_event

        self.__worker_names = ', '.join(worker.settings.name for worker in process.workers)

        self.__pipe_reader_connection: connection.Connection | None = None
        self.__pipe_writer_connection: connection.Connection | None = None

        self.__worker_manager_runner = _WorkerManagerRunner(self.__redis_dsn, self.__process)

    def __start_worker_manager(self) -> None:
        connections = Pipe(duplex=False)
        self.__pipe_reader_connection: connection.Connection = connections[0]
        self.__pipe_writer_connection: connection.Connection = connections[1]

        self.__worker_manager_runner.start(self.__pipe_writer_connection)

    def __close_worker_manager(self) -> None:
        if self.__worker_manager_runner.is_close():
            return

        self.__worker_manager_runner.close()

        self.__pipe_reader_connection.close()
        self.__pipe_writer_connection.close()

    async def __check_for_hung_tasks(self):
        try:
            async with Redis.from_url(self.__redis_dsn, protocol=3, decode_responses=True) as redis:
                async with redis.pipeline() as pipe:
                    for worker in self.__process.workers:
                        for stream in worker.settings.source_streams:
                            pending_info = await redis.xpending(stream, worker.settings.name)

                            if pending_info['pending'] == 0:
                                continue

                            pending_messages = await redis.xpending_range(
                                name=stream,
                                groupname=worker.settings.name,
                                min=pending_info['min'],
                                max=pending_info['max'],
                                count=pending_info['pending']
                            )

                            await pipe.xack(stream, worker.settings.name, *map(lambda x: x['message_id'], pending_messages))

                            for message in pending_messages:
                                logger.warning(
                                    f'[{self.__worker_names}] Обнаружено зависшее сообщение. '
                                    f'Поток: {stream}. '
                                    f'Воркер (группа): {worker.settings.name}. '
                                    f'ID сообщения: {message["message_id"]}. '
                                    f'Время обработки (ms): {message["elapsed"]}'
                                )

                    await pipe.execute()
        except RedisError as error:
            logger.error(f'[{self.__worker_names}] Не удалось проверить зависшие сообщения: {error}')

    async def __run_monitoring(self):
        while not self.__shutdown_event.is_set():
            print(6)
            await _wait_for_data(
                self.__pipe_reader_connection,
                self.__shutdown_event
            )

            print(2)
            if self.__shutdown_event.is_set():
                print(6)
                return
            print(3)
            state = _EventStartMessage.model_validate(loads(self.__pipe_reader_connection.recv_bytes()))

            is_exist_message = await _wait_for_data(
                self.__pipe_reader_connection,
                self.__shutdown_event,
                state.end_time - time.time()
            )
            print(4)
            if self.__shutdown_event.is_set():
                print(7)
                return
            print(5)
            if is_exist_message:
                self.__pipe_reader_connection.recv_bytes()
                continue

            logger.warning(f'[{self.__worker_names}] Воркер {state.worker_name} завис. Начат перезапуск процесса')

            await asyncio.to_thread(self.__close_worker_manager)
            await self.__check_for_hung_tasks()

            if self.__shutdown_event.is_set():
                return

            await asyncio.to_thread(self.__start_worker_manager)

            logger.warning(f'[{self.__worker_names}] Завершен перезапуск процесса')

    async def run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Запущен наблюдатель процесса')

        await self.__check_for_hung_tasks()
        await asyncio.to_thread(self.__start_worker_manager)

        try:
            await self.__run_monitoring()
        except Exception as error:
            logger.critical(f'[{self.__worker_names}] Наблюдатель процесса неожиданно завершился: {error}')

        logger.debug(f'[{self.__worker_names}] Наблюдатель процесса начал завершение')

        await asyncio.to_thread(self.__close_worker_manager)
        await self.__check_for_hung_tasks()

        logger.debug(f'[{self.__worker_names}] Наблюдатель процесса завершил работу')
