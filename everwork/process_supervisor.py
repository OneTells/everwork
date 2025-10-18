import asyncio
import time
from multiprocessing import Pipe, connection, context

from loguru import logger
from orjson import loads
from pydantic import BaseModel
from redis.asyncio import Redis

from .base_worker import BaseWorker
from .utils import ShutdownSafeZone, ShutdownEvent
from .worker_manager import WorkerManager


class _EventStartMessage(BaseModel):
    worker_name: str
    end_time: float


class ProcessSupervisor:

    def __init__(self, redis_dsn: str, workers: list[type[BaseWorker]], shutdown_event: ShutdownEvent) -> None:
        self.__redis_dsn = redis_dsn

        self.__workers = workers
        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__shutdown_event = shutdown_event
        self.__shutdown_safe_zone = ShutdownSafeZone(shutdown_event)

        self.task: asyncio.Task | None = None

        connections = Pipe(duplex=False)
        self.__pipe_reader_connection: connection.Connection = connections[0]
        self.__pipe_writer_connection: connection.Connection = connections[1]

        self.__data = {
            'redis_dsn': redis_dsn,
            'workers': workers,
            'pipe_connection': self.__pipe_writer_connection,
            'logger_': logger
        }
        self.__process: context.ForkServerProcess | None = None

    def __start_process(self) -> None:
        # self.__process = context.ForkServerProcess(target=WorkerManager.run, kwargs=self.__data, daemon=True)
        self.__process = context.SpawnProcess(target=WorkerManager.run, kwargs=self.__data, daemon=True)
        self.__process.start()

        logger.debug(f'[{self.__worker_names}] Процесс запущен')

    def __close_process(self) -> None:
        if self.__process is None:
            return

        logger.debug(f'[{self.__worker_names}] Начат процесс завершения процесса')

        self.__process.terminate()

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self.__workers)

        while True:
            if time.time() > end_time:
                logger.warning(f'[{self.__worker_names}] Процесс не завершился за отведенное время')
                break

            if not self.__process.is_alive():
                break

            time.sleep(0.01)

        if self.__process.is_alive():
            self.__process.kill()
            logger.warning(f'[{self.__worker_names}] Процессу отправлен сигнал SIGKILL')

        logger.debug(f'[{self.__worker_names}] Закрытие процесса и очистка ресурсов')

        self.__process.join()
        self.__process.close()
        self.__process = None

        while self.__pipe_reader_connection.poll():
            self.__pipe_reader_connection.recv_bytes()
            continue

        logger.debug(f'[{self.__worker_names}] Процесс завершен')

    async def __check_for_hung_tasks(self):
        logger.debug(f'[{self.__worker_names}] Начат процесс проверки на зависшие задачи')

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

        logger.debug(f'[{self.__worker_names}] Завершен процесс проверки на зависшие задачи')

    async def __run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Запущен наблюдатель процесса')

        await self.__check_for_hung_tasks()

        await asyncio.to_thread(self.__start_process)

        try:
            while not self.__shutdown_event.is_set():
                logger.debug(f'[{self.__worker_names}] Ожидание исполнения задачи одним из воркеров')

                with self.__shutdown_safe_zone:
                    await asyncio.to_thread(self.__pipe_reader_connection.poll, None)

                state = _EventStartMessage.model_validate(loads(self.__pipe_reader_connection.recv_bytes()))

                logger.debug(f'[{self.__worker_names}] Начать процесс отслеживание работы {state.worker_name}')

                with self.__shutdown_safe_zone:
                    is_not_empty = await asyncio.to_thread(self.__pipe_reader_connection.poll, state.end_time - time.time())

                if is_not_empty:
                    self.__pipe_reader_connection.recv_bytes()
                    continue

                logger.warning(f'[{self.__worker_names}] Воркер {state.worker_name} завис. Начат перезапуск процесса')

                await asyncio.to_thread(self.__close_process)

                await self.__check_for_hung_tasks()

                if self.__shutdown_event.is_set():
                    break

                await asyncio.to_thread(self.__start_process)
                await asyncio.sleep(0.5)

                logger.warning(f'[{self.__worker_names}] Завершен перезапуск процесса')
        except asyncio.CancelledError:
            logger.debug(f'[{self.__worker_names}] Мониторинг процесса отменен')
        except Exception as error:
            logger.exception(f'[{self.__worker_names}] Мониторинг процесса неожиданно завершился: {error}')

        logger.debug(f'[{self.__worker_names}] Наблюдатель процесса начал завершение')

        await asyncio.to_thread(self.__close_process)

        await self.__check_for_hung_tasks()

        self.__pipe_reader_connection.close()
        self.__pipe_writer_connection.close()

        logger.debug(f'[{self.__worker_names}] Наблюдатель процесса завершил работу')

    def run(self) -> None:
        self.task = asyncio.create_task(self.__run())

    def close(self) -> None:
        logger.debug(f'[{self.__worker_names}] Вызван метод закрытия наблюдателя процесса')

        if not self.__shutdown_safe_zone.is_use():
            logger.debug(f'[{self.__worker_names}] Безопасная зона не используется')
            return

        self.task.cancel()
        logger.debug(f'[{self.__worker_names}] Задача остановлена')
