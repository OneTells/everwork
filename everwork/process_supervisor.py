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
        self.__process = context.SpawnProcess(target=WorkerManager.run, kwargs=self.__data, daemon=True)
        self.__process.start()

        logger.debug(f'{self.__worker_names}: Процесс запущен')

    async def __close_process(self) -> None:
        logger.debug(f'{self.__worker_names}: Начат процесс завершения процесса')

        self.__process.terminate()

        end_time = time.time() + self.__SHUTDOWN_TIMEOUT_SECONDS

        with self.__shutdown_safe_zone:
            while True:
                if time.time() > end_time:
                    logger.warning(f'{self.__worker_names}: Процесс не завершился за отведенное время')
                    break

                if not self.__process.is_alive():
                    break

                await asyncio.sleep(0.1)

        if self.__process.is_alive():
            self.__process.kill()
            logger.warning(f'{self.__worker_names}: Процессу отправлен сигнал SIGKILL')

        self.__process.join()
        self.__process.close()

        logger.debug(f'{self.__worker_names}: Процесс завершен')

    async def __run(self) -> None:
        logger.debug(f'{self.__worker_names}: Запущен наблюдатель процесса')

        self.__start_process()

        try:
            while not self.__shutdown_event.is_set():
                logger.debug(f'{self.__worker_names}: Ожидание получения состояния процесса')

                with self.__shutdown_safe_zone:
                    data = await self.__redis.brpop([f'process:{self.__uuid}:state'])

                state = ProcessState.model_validate(loads(data[1]))

                logger.debug(f'{self.__worker_names}: Начать процесс отслеживание работы {state.worker_name}')

                with self.__shutdown_safe_zone:
                    with suppress(TimeoutError):
                        async with asyncio.timeout(state.end_time - time.time()):
                            await self.__redis.brpop([f'process:{self.__uuid}:state'])

                        logger.debug(f'{self.__worker_names}: Worker {state.worker_name} успешно отработал')
                        continue

                logger.warning(f'{self.__worker_names}: Worker {state.worker_name} завис. Начат перезапуск процесса')

                await self.__close_process()

                with self.__shutdown_safe_zone:
                    await self.__redis.delete(f'process:{self.__uuid}:state')

                self.__start_process()

                logger.warning(f'{self.__worker_names}: Завершен перезапуск процесса')
        except asyncio.CancelledError:
            logger.debug(f'{self.__worker_names}: Мониторинг процесса отменен')
        except Exception as error:
            logger.exception(f'{self.__worker_names}: Мониторинг процесса неожиданно завершился: {error}')

        await self.__close_process()

        await self.__redis.close()

        logger.debug(f'{self.__worker_names}: Наблюдатель процесса завершил работу')

    def run(self) -> None:
        self.__thread.start()

    def wait(self) -> None:
        self.__thread.join()

    def close(self) -> None:
        logger.debug(f'{self.__worker_names}: Вызван метод закрытия наблюдателя процесса')

        if not self.__shutdown_safe_zone.is_use():
            logger.debug(f'{self.__worker_names}: Безопасная зона не используется')
            return

        def stop_event_loop() -> None:
            self.__runner.close()
            logger.debug(f'{self.__worker_names}: Цикл событий остановлен')

        # noinspection PyTypeChecker
        self.__runner.get_loop().call_soon_threadsafe(stop_event_loop)
