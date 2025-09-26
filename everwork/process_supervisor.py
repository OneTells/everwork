import asyncio
import time
from contextlib import suppress
from multiprocessing import context
from threading import Thread

from loguru import logger
from orjson import loads
from pydantic import RedisDsn, BaseModel
from pydantic_core import to_jsonable_python
from redis.asyncio import Redis

from everwork.utils import ShutdownSafeZone, ShutdownEvent
from everwork.worker_base import BaseWorker
from everwork.worker_manager import WorkerManager

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class ProcessState(BaseModel):
    worker_name: str
    end_time: float


class ProcessSupervisor:
    __SHUTDOWN_TIMEOUT_SECONDS = 3

    def __init__(
        self,
        uuid: str,
        workers: list[type[BaseWorker]],
        shutdown_event: ShutdownEvent,
        redis_dsn: RedisDsn,
        scripts: dict[str, str]
    ) -> None:
        self.__uuid = uuid
        self.__worker_names = ', '.join(worker.settings.name for worker in workers)

        self.__shutdown_event = shutdown_event
        self.__shutdown_safe_zone = ShutdownSafeZone(shutdown_event)
        self.__runner = asyncio.Runner(loop_factory=new_event_loop)

        self.__redis = Redis.from_url(url=redis_dsn.encoded_string(), protocol=3, decode_responses=True)

        self.__thread = Thread(target=lambda: self.__runner.run(self.__run()), daemon=True)

        self.__data = to_jsonable_python({'uuid': uuid, 'workers': workers, 'redis_dsn': redis_dsn, 'scripts': scripts})
        self.__process: context.SpawnProcess

    def __start_process(self) -> None:
        logger.debug(f'Начат запуск процесса. Состав: {self.__worker_names}')

        self.__process = context.SpawnProcess(target=WorkerManager.run, kwargs=self.__data, daemon=True)
        self.__process.start()

        logger.debug(f'Процесс запущен. Состав: {self.__worker_names}')

    async def __close_process(self) -> None:
        logger.debug(f'Начат процесс завершения процесса. Состав: {self.__worker_names}')

        self.__process.terminate()
        logger.debug(f'Процессу отправлен сигнал SIGTERM. Состав: {self.__worker_names}')

        end_time = time.time() + self.__SHUTDOWN_TIMEOUT_SECONDS

        logger.debug(f'Ожидание завершения процесса. Состав: {self.__worker_names}')

        with self.__shutdown_safe_zone:
            while True:
                if time.time() > end_time:
                    logger.warning(f'Процесс не завершился за отведенное время. Состав: {self.__worker_names}')
                    break

                if not self.__process.is_alive():
                    logger.debug(f'Процесс завершился за отведенное время. Состав: {self.__worker_names}')
                    break

                await asyncio.sleep(0.1)

        if self.__process.is_alive():
            self.__process.kill()
            logger.warning(f'Процессу отправлен сигнал SIGKILL. Состав: {self.__worker_names}')

        self.__process.join()
        logger.debug(f'Ресурсы процесса освобождены. Состав: {self.__worker_names}')

        self.__process.close()
        logger.debug(f'Завершен процесс. Состав: {self.__worker_names}')

    async def __run(self) -> None:
        logger.debug(f'Запущен наблюдатель. Состав: {self.__worker_names}')

        self.__start_process()

        try:
            while not self.__shutdown_event.is_set():
                logger.debug(f'Ожидание состояния процесса. Состав: {self.__worker_names}')

                with self.__shutdown_safe_zone:
                    data = await self.__redis.brpop([f'process:{self.__uuid}:state'])

                logger.debug(f'Получено состояние процесса. Состав: {self.__worker_names}')

                state = ProcessState.model_validate(loads(data[1]))

                logger.debug(f'Начать процесс отслеживание работы {state.worker_name}. Состав: {self.__worker_names}')

                with self.__shutdown_safe_zone:
                    with suppress(TimeoutError):
                        async with asyncio.timeout(state.end_time - time.time()):
                            await self.__redis.brpop([f'process:{self.__uuid}:state'])

                        logger.debug(f'Worker {state.worker_name} успешно отработал. Состав: {self.__worker_names}')
                        continue

                logger.warning(f'Worker {state.worker_name} завис. Начат перезапуск процесса. Состав: {self.__worker_names}')

                await self.__close_process()

                with self.__shutdown_safe_zone:
                    await self.__redis.delete(f'process:{self.__uuid}:state')

                logger.debug(f'Состояние процесса очищено. Состав: {self.__worker_names}')

                self.__start_process()

                logger.warning(f'Завершен перезапуск процесса. Состав: {self.__worker_names}')
        except asyncio.CancelledError:
            logger.debug(f'Мониторинг процесса отменен. Состав: {self.__worker_names}')
        except Exception as error:
            logger.exception(f'Мониторинг процесса неожиданно завершился. Состав: {self.__worker_names}. {error}')

        await self.__close_process()

        await self.__redis.close()
        logger.debug(f'Наблюдатель закрыл Redis. Состав: {self.__worker_names}')

        logger.debug(f'Наблюдатель завершил работу. Состав: {self.__worker_names}')

    def run(self) -> None:
        self.__thread.start()

    def wait(self) -> None:
        self.__thread.join()

    def close(self) -> None:
        logger.debug(f'Вызван метод закрытия системы по управлению процессом. Состав: {self.__worker_names}')

        if not self.__shutdown_safe_zone.is_use():
            logger.debug(f'Безопасная зона не используется. Состав: {self.__worker_names}')
            return

        def stop_event_loop() -> None:
            logger.debug(f'Вызвана остановка цикла событий. Состав: {self.__worker_names}')
            self.__runner.close()
            logger.debug(f'Цикл событий остановлен. Состав: {self.__worker_names}')

        # noinspection PyTypeChecker
        self.__runner.get_loop().call_soon_threadsafe(stop_event_loop)
        logger.debug(f'Запланирована остановка цикла событий. Состав: {self.__worker_names}')
