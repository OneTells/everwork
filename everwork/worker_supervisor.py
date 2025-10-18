import asyncio
import time
from multiprocessing import connection
from threading import Thread, Lock

from loguru import logger
from orjson import dumps
from pydantic import validate_call
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from .base_worker import BaseWorker
from .resource_handler import BaseResourceHandler
from .utils import ShutdownSafeZone, ShutdownEvent

try:
    from uvloop import new_event_loop
except ImportError:
    from asyncio import new_event_loop


class WorkerSupervisor:
    __WORKER_POLL_INTERVAL_SECONDS = 60

    def __init__(
        self,
        redis_dsn: str,
        worker: type[BaseWorker],
        shutdown_event: ShutdownEvent,
        lock: Lock,
        pipe_connection: connection.Connection,
        resource_handler: type[BaseResourceHandler]
    ) -> None:
        self.__redis = Redis.from_url(redis_dsn, protocol=3, decode_responses=True)

        self.__worker = worker()
        self.__function = validate_call(validate_return=True)(self.__worker.__call__)

        self.__shutdown_event = shutdown_event
        self.__shutdown_safe_zone = ShutdownSafeZone(shutdown_event)

        self.__lock = lock
        self.__pipe_connection = pipe_connection

        self.__resource_handler = resource_handler(self.__redis, self.__worker.settings, self.__shutdown_safe_zone)

        self.__loop: asyncio.AbstractEventLoop | None = None
        self.__thread: Thread | None = None

        self.__scripts: dict[str, str] = {}

    async def __load_handle_cancel_script(self) -> None:
        self.__scripts['handle_cancel'] = await self.__redis.script_load(
            """
            local messages = redis.call('XRANGE', KEYS[1], KEYS[3], KEYS[3], 'COUNT', 1)
            if #messages > 0 then
                redis.call('XACK', KEYS[1], KEYS[2], KEYS[3])
                redis.call('XADD', 'workers:' .. KEYS[2] .. ':stream', '*', unpack(messages[1][2]))
            end
            """
        )

    async def __handle_error(self) -> None:
        if self.__resource_handler.resources is None:
            return

        await self.__redis.xack(
            name=self.__resource_handler.resources.stream,
            groupname=self.__worker.settings.name,
            message_ids=[self.__resource_handler.resources.message_id]
        )

    async def __handle_cancel(self) -> None:
        if self.__resource_handler.resources is None:
            return

        keys = [
            self.__resource_handler.resources.stream,
            self.__worker.settings.name,
            self.__resource_handler.resources.message_id
        ]

        try:
            await self.__redis.evalsha(self.__scripts['handle_cancel'], 3, *keys)
        except NoScriptError:
            await self.__load_handle_cancel_script()
            await self.__redis.evalsha(self.__scripts['handle_cancel'], 3, *keys)

    async def __handle_success(self) -> None:
        if self.__resource_handler.resources is None:
            return

        await self.__redis.xack(
            name=self.__resource_handler.resources.stream,
            groupname=self.__worker.settings.name,
            message_ids=[self.__resource_handler.resources.message_id]
        )

    def __notify_event_start(self) -> None:
        self.__pipe_connection.send_bytes(dumps({
            'worker_name': self.__worker.settings.name,
            'end_time': time.time() + self.__worker.settings.execution_timeout
        }))

    def __notify_event_end(self) -> None:
        self.__pipe_connection.send_bytes(b'')

    async def __get_is_worker_on(self) -> bool:
        value = await self.__redis.get(f'workers:{self.__worker.settings.name}:is_worker_on')
        return value == '1'

    async def __run(self):
        logger.debug(f'({self.__worker.settings.name}) Запушен наблюдатель воркера')

        self.__worker.initialize(self.__redis)

        await self.__load_handle_cancel_script()
        logger.info(f'({self.__worker.settings.name}) Скрипты зарегистрированы')

        try:
            await self.__worker.startup()
        except Exception as error:
            logger.exception(f'({self.__worker.settings.name}) Не удалось выполнить startup: {error}')
            return

        try:
            while not self.__shutdown_event.is_set():
                logger.debug(f'({self.__worker.settings.name}) Проверяю состояния воркера')

                if not (await self.__get_is_worker_on()):
                    with self.__shutdown_safe_zone:
                        await asyncio.sleep(self.__WORKER_POLL_INTERVAL_SECONDS)

                    continue

                logger.debug(f'({self.__worker.settings.name}) Начинаю получение ивента')

                try:
                    kwargs = await self.__resource_handler.get_kwargs()
                except asyncio.CancelledError:
                    logger.debug(f'({self.__worker.settings.name}) Получение ивента отменено')
                    await self.__handle_cancel()
                    continue

                if (
                    self.__shutdown_event.is_set()
                    or not (await self.__get_is_worker_on())
                ):
                    logger.debug(f'({self.__worker.settings.name}) Ивент получен, но был отменен')
                    await self.__handle_cancel()
                    continue

                with self.__lock:
                    logger.debug(f'({self.__worker.settings.name}) Начата обработка ивента')

                    self.__notify_event_start()

                    try:
                        async with self.__worker.event_publisher:
                            await self.__function(**kwargs)  # type: ignore
                    except Exception as error:
                        if self.__resource_handler.resources is not None:
                            logger.exception(
                                f'({self.__worker.settings.name}) Не удалось обработать сообщение из потока. '
                                f'Поток: {self.__resource_handler.resources.stream}. '
                                f'ID сообщения: {self.__resource_handler.resources.message_id}. '
                                f'Ошибка: {error}'
                            )
                        else:
                            logger.exception(f'({self.__worker.settings.name}) Не удалось обработать ивент. Ошибка: {error}')

                        await self.__handle_error()
                    else:
                        await self.__handle_success()

                    self.__notify_event_end()

                    del kwargs
                    self.__resource_handler.clear()

                    logger.debug(f'({self.__worker.settings.name}) Завершена обработка ивента')
        except asyncio.CancelledError:
            logger.debug(f'({self.__worker.settings.name}) Мониторинг воркера отменен')
        except Exception as error:
            logger.exception(f'({self.__worker.settings.name}) Мониторинг воркера неожиданно завершился: {error}')

        logger.debug(f'({self.__worker.settings.name}) Наблюдатель воркера начал завершение')

        try:
            await self.__worker.shutdown()
        except Exception as error:
            logger.exception(f'({self.__worker.settings.name}) Не удалось выполнить shutdown: {error}')

        logger.debug(f'({self.__worker.settings.name}) Наблюдатель воркера завершил работ')

    def run(self) -> None:
        async def __run_wrapper() -> None:
            async with self.__redis:
                await self.__run()

            await logger.complete()

        def __run_in_thread() -> None:
            with asyncio.Runner(loop_factory=new_event_loop) as runner:
                self.__loop = runner.get_loop()
                runner.run(__run_wrapper())

        self.__thread = Thread(target=__run_in_thread, daemon=True)
        self.__thread.start()

    def wait(self) -> None:
        self.__thread.join()

    def close(self) -> None:
        logger.debug(f'({self.__worker.settings.name}) Вызван метод закрытия наблюдателя воркера')

        if not self.__shutdown_safe_zone.is_use():
            logger.debug(f'({self.__worker.settings.name}) Безопасная зона не используется')
            return

        def cancel_all_tasks() -> None:
            for task in asyncio.all_tasks(self.__loop):
                task.cancel()

            logger.debug(f'({self.__worker.settings.name}) Все задачи в цикле отменены')

        # noinspection PyTypeChecker
        self.__loop.call_soon_threadsafe(cancel_all_tasks)
