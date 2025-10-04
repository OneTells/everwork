import asyncio
import time
from multiprocessing import Pipe, connection, context

from loguru import logger
from orjson import loads
from pydantic import BaseModel

from everwork.utils import ShutdownSafeZone, ShutdownEvent
from everwork.worker_base import BaseWorker
from everwork.worker_manager import WorkerManager

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class EventStartMessage(BaseModel):
    worker_name: str
    end_time: float


class ProcessSupervisor:

    def __init__(self, redis_dsn: str, workers: list[type[BaseWorker]], shutdown_event: ShutdownEvent) -> None:
        self.__workers = workers
        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__shutdown_event = shutdown_event
        self.__shutdown_safe_zone = ShutdownSafeZone(shutdown_event)

        self.task: asyncio.Task | None = None

        connections = Pipe(duplex=False)
        self.__pipe_reader_connection: connection.Connection = connections[0]
        self.__pipe_writer_connection: connection.Connection = connections[1]

        self.__data = {'redis_dsn': redis_dsn, 'workers': workers, 'pipe_connection': self.__pipe_writer_connection}
        self.__process: context.SpawnProcess | None = None

    def __start_process(self) -> None:
        self.__process = context.SpawnProcess(target=WorkerManager.run, kwargs=self.__data, daemon=True)
        self.__process.start()

        logger.debug(f'[{self.__worker_names}] Процесс запущен')

    def __close_process(self) -> None:
        logger.debug(f'[{self.__worker_names}] Начат процесс завершения процесса')

        self.__process.terminate()

        end_time = time.time() + max(worker.settings.execution_timeout for worker in self.__workers)

        with self.__shutdown_safe_zone:
            while True:
                if time.time() > end_time:
                    logger.warning(f'[{self.__worker_names}] Процесс не завершился за отведенное время')
                    break

                if not self.__process.is_alive():
                    break

                time.sleep(0.1)

        if self.__process.is_alive():
            self.__process.kill()
            logger.warning(f'[{self.__worker_names}] Процессу отправлен сигнал SIGKILL')

        self.__process.join()
        self.__process.close()

        logger.debug(f'[{self.__worker_names}] Процесс завершен')

    async def __run(self) -> None:
        logger.debug(f'[{self.__worker_names}] Запущен наблюдатель процесса')

        await asyncio.to_thread(self.__start_process)

        try:
            while not self.__shutdown_event.is_set():
                logger.debug(f'[{self.__worker_names}] Ожидание исполнения задачи одним из воркеров')

                with self.__shutdown_safe_zone:
                    await asyncio.to_thread(self.__pipe_reader_connection.poll, None)

                state = EventStartMessage.model_validate(loads(self.__pipe_reader_connection.recv_bytes()))

                logger.debug(f'[{self.__worker_names}] Начать процесс отслеживание работы {state.worker_name}')

                with self.__shutdown_safe_zone:
                    is_not_empty = await asyncio.to_thread(self.__pipe_reader_connection.poll, state.end_time - time.time())

                if is_not_empty:
                    self.__pipe_reader_connection.recv_bytes()
                    continue

                logger.warning(f'[{self.__worker_names}] Воркер {state.worker_name} завис. Начат перезапуск процесса')

                await asyncio.to_thread(self.__close_process)
                await asyncio.to_thread(self.__start_process)

                logger.warning(f'[{self.__worker_names}] Завершен перезапуск процесса')
        except asyncio.CancelledError:
            logger.debug(f'[{self.__worker_names}] Мониторинг процесса отменен')
        except Exception as error:
            logger.exception(f'[{self.__worker_names}] Мониторинг процесса неожиданно завершился: {error}')

        await asyncio.to_thread(self.__close_process)

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
