import asyncio
import os
import signal
import time
from multiprocessing import Process, Pipe, connection

from loguru import logger
from orjson import loads
from pydantic import BaseModel
from redis.asyncio import Redis

from .base_worker import BaseWorker
from .worker_manager import WorkerManager


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


class ProcessSupervisor:
    __SHUTDOWN_TIMEOUT_SECONDS = 20

    def __init__(self, redis_dsn: str, workers: list[type[BaseWorker]], shutdown_event: asyncio.Event) -> None:
        self.__redis_dsn = redis_dsn
        self.__workers = workers
        self.__shutdown_event = shutdown_event

        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__pipe_reader_connection: connection.Connection | None = None
        self.__pipe_writer_connection: connection.Connection | None = None

        self.__process: Process | None = None

    def __start_process(self) -> None:
        connections = Pipe(duplex=False)
        self.__pipe_reader_connection: connection.Connection = connections[0]
        self.__pipe_writer_connection: connection.Connection = connections[1]

        self.__process = Process(
            target=WorkerManager.run,
            kwargs={
                'redis_dsn': self.__redis_dsn,
                'workers': self.__workers,
                'pipe_connection': self.__pipe_writer_connection,
                'logger_': logger
            },
            daemon=True
        )
        self.__process.start()

    def __close_process(self) -> None:
        if self.__process is None:
            return

        if self.__process.is_alive():
            logger.debug(f'[{self.__worker_names}] Подан сигнал о закрытии процесса')
            os.kill(self.__process.pid, signal.SIGUSR1)

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self.__workers)

        while True:
            if time.time() > end_time:
                break

            if not self.__process.is_alive():
                break

            time.sleep(0.01)

        if self.__process.is_alive():
            logger.debug(f'[{self.__worker_names}] Подан сигнал о немедленном закрытии процесса')
            os.kill(self.__process.pid, signal.SIGTERM)

        end_time = time.time() + self.__SHUTDOWN_TIMEOUT_SECONDS

        while True:
            if time.time() > end_time:
                break

            if not self.__process.is_alive():
                break

            time.sleep(0.01)

        if self.__process.is_alive():
            logger.critical(
                f'[{self.__worker_names}] Не удалось мягко завершить процесс, отправлен сигнал SIGKILL. '
                f'Возможно зависание воркеров, необходимо перезапустить менеджер'
            )
            os.kill(self.__process.pid, signal.SIGKILL)

        self.__process.join()
        self.__process.close()
        self.__process = None

        self.__pipe_reader_connection.close()
        self.__pipe_writer_connection.close()

    async def __check_for_hung_tasks(self):
        async with Redis.from_url(self.__redis_dsn, protocol=3, decode_responses=True) as redis:
            async with redis.pipeline() as pipe:
                for worker in self.__workers:
                    for stream in (worker.settings.source_streams | {f'workers:{worker.settings.name}:stream'}):
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

    async def run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Запущен наблюдатель процесса')

        await self.__check_for_hung_tasks()
        await asyncio.to_thread(self.__start_process)

        while not self.__shutdown_event.is_set():
            await _wait_for_data(
                self.__pipe_reader_connection,
                self.__shutdown_event
            )

            if self.__shutdown_event.is_set():
                break

            state = _EventStartMessage.model_validate(loads(self.__pipe_reader_connection.recv_bytes()))

            is_exist_data = await _wait_for_data(
                self.__pipe_reader_connection,
                self.__shutdown_event,
                state.end_time - time.time()
            )

            if self.__shutdown_event.is_set():
                break

            if is_exist_data:
                self.__pipe_reader_connection.recv_bytes()
                continue

            logger.warning(f'[{self.__worker_names}] Воркер {state.worker_name} завис. Начат перезапуск процесса')

            await asyncio.to_thread(self.__close_process)
            await self.__check_for_hung_tasks()

            if self.__shutdown_event.is_set():
                break

            await asyncio.to_thread(self.__start_process)

            logger.warning(f'[{self.__worker_names}] Завершен перезапуск процесса')

        logger.debug(f'[{self.__worker_names}] Наблюдатель процесса начал завершение')

        await asyncio.to_thread(self.__close_process)
        await self.__check_for_hung_tasks()

        logger.debug(f'[{self.__worker_names}] Наблюдатель процесса завершил работу')
