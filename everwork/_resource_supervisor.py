import asyncio
from typing import Any

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import RedisError, NoScriptError

from ._redis_retry import _RetryShutdownException
from ._resource_handler import _TriggerResourceHandler, _ExecutorResourceHandler
from ._utils import _SingleValueChannel, _wait_for_or_cancel
from .worker import AbstractWorker, TriggerMode


class _ResourceSupervisor:

    def __init__(
        self,
        redis: Redis,
        worker: type[AbstractWorker],
        response_channel: _SingleValueChannel[tuple[str, dict[str, Any]]],
        answer_channel: _SingleValueChannel[bool],
        lock: asyncio.Lock,
        shutdown_event: asyncio.Event
    ) -> None:
        self.__redis = redis
        self.__worker = worker
        self.__response_channel = response_channel
        self.__answer_channel = answer_channel
        self.__lock = lock
        self.__shutdown_event = shutdown_event

        if isinstance(worker.settings.mode, TriggerMode):
            resource_handler = _TriggerResourceHandler
        else:
            resource_handler = _ExecutorResourceHandler

        self.__resource_handler = resource_handler(self.__redis, self.__worker.settings, self.__shutdown_event)

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
            self.__resource_handler.resources.stream,
            self.__worker.settings.name,
            self.__resource_handler.resources.message_id
        )

        self.__resource_handler.resources = None

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

        self.__resource_handler.resources = None

    async def __handle_success(self) -> None:
        if self.__resource_handler.resources is None:
            return

        await self.__redis.xack(
            self.__resource_handler.resources.stream,
            self.__worker.settings.name,
            self.__resource_handler.resources.message_id
        )

        self.__resource_handler.resources = None

    async def __get_is_worker_on(self) -> bool:
        value = await self.__redis.get(f'workers:{self.__worker.settings.name}:is_worker_on')
        return value == '1'

    async def __process_worker_messages(self) -> None:
        while not self.__shutdown_event.is_set():
            if not (await self.__get_is_worker_on()):
                try:
                    await _wait_for_or_cancel(
                        asyncio.sleep(self.__worker.settings.poll_interval),
                        self.__shutdown_event
                    )
                except asyncio.CancelledError:
                    break

                continue

            try:
                kwargs = await self.__resource_handler.get_kwargs()
            except asyncio.CancelledError:
                await self.__handle_cancel()
                break

            if self.__shutdown_event.is_set() or not (await self.__get_is_worker_on()):
                await self.__handle_cancel()
                continue

            async with self.__lock:
                if self.__shutdown_event.is_set() or not (await self.__get_is_worker_on()):
                    await self.__handle_cancel()
                    continue

                self.__response_channel.send((self.__worker.settings.name, kwargs))

                is_success = await self.__answer_channel.receive()

                if not is_success:
                    if self.__resource_handler.resources is not None:
                        logger.warning(
                            f'({self.__worker.settings.name}) Не удалось обработать сообщение из потока. '
                            f'Поток: {self.__resource_handler.resources.stream}. '
                            f'ID сообщения: {self.__resource_handler.resources.message_id}'
                        )
                    await self.__handle_error()
                else:
                    await self.__handle_success()

                del kwargs

    async def run(self) -> None:
        logger.debug(f'({self.__worker.settings.name}) Запушен наблюдатель воркера')

        try:
            await self.__load_handle_cancel_script()
        except RedisError as error:
            logger.critical(f'Ошибка при регистрации скрипта в мониторинге воркера: {error}')
            return

        logger.debug(f'({self.__worker.settings.name}) Скрипты зарегистрированы')

        try:
            await self.__process_worker_messages()
        except _RetryShutdownException:
            logger.exception(f'({self.__worker.settings.name}) Redis не доступен или не отвечает при мониторинге воркера')

            if self.__resource_handler.resources is not None:
                logger.warning(
                    f'({self.__worker.settings.name}) Не удалось правильно обработать сообщение. '
                    f'Поток: {self.__resource_handler.resources.stream}. '
                    f'ID сообщения: {self.__resource_handler.resources.message_id}'
                )
        except Exception as error:
            logger.exception(f'({self.__worker.settings.name}) Мониторинг воркера неожиданно завершился: {error}')

        logger.debug(f'({self.__worker.settings.name}) Наблюдатель воркера завершил работ')
