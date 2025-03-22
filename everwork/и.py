import asyncio
import ctypes
import os
import sys
import threading
import time
from asyncio import Future
from typing import Any, Literal

import uvloop
from loguru import logger

from everwork.shemes import Settings, Event, ExecutorMode
from everwork.worker import BaseWorker


class TestWorker(BaseWorker):

    @staticmethod
    def settings() -> Settings:
        return Settings(
            name='testworker',
            mode=ExecutorMode(
                replicas=1
            )
        )

    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def __call__(self, a) -> list[Event] | None:
        if a == 100:
            time.sleep(100)

        await asyncio.sleep(a)

        return [
            Event(
                target='test2',
                kwargs={'a': a + 1}
            )
        ]


class WorkerThread:

    def __init__(self, worker: type[BaseWorker]):
        super().__init__()

        self.__worker = worker

        self.__main_loop = asyncio.get_event_loop()
        self.__thread_loop = uvloop.new_event_loop()

        self.__put_future: Future[dict[str, Any] | Literal['STOP']] = self.__thread_loop.create_future()
        self.__get_future: Future[list[Event] | None | Literal['ERROR']] = self.__main_loop.create_future()

        self.__killed = False

        self.__thread = threading.Thread(target=self.run, daemon=True)
        self.__thread.start()

    async def __async_run(self):
        worker = self.__worker()

        await worker.startup()

        while True:
            kwargs = await self.__put_future

            if kwargs == 'STOP':
                break

            self.__put_future = self.__thread_loop.create_future()

            try:
                result = await worker(**kwargs)
            except Exception as error:
                logger.exception(f'Ошибка при выполнении {self.__worker.settings().name}: {error}')
                result = 'ERROR'

            self.__main_loop.call_soon_threadsafe(self.__get_future.set_result, result)

            del kwargs, result

        await worker.shutdown()

    def run(self) -> None:
        try:
            with asyncio.Runner(loop_factory=lambda: self.__thread_loop) as runner:
                runner.run(self.__async_run())
        except SystemExit:
            pass

        self.__main_loop.call_soon_threadsafe(self.__get_future.set_result, "TIMEOUT")

    async def timeout_function(self, timeout: float):
        await asyncio.sleep(timeout)

        logger.warning(f'Worker {self.__worker.settings().name} завис, перезапускаю...')

        self.__killed = True
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(self.__thread.ident),
            ctypes.py_object(SystemExit)
        )

    async def run_task(self, kwargs: dict[str, Any], timeout: float) -> list[Event] | None | Literal['ERROR']:
        self.__thread_loop.call_soon_threadsafe(self.__put_future.set_result, kwargs)

        timeout_task = self.__thread_loop.create_task(self.timeout_function(timeout))

        while True:
            if self.__get_future.done():
                break

            await asyncio.sleep(0.1)

        result = self.__get_future.result()
        # result = await self.__get_future

        timeout_task.cancel()

        self.__get_future = self.__main_loop.create_future()

        if result == 'TIMEOUT':
            self.__main_loop = asyncio.get_event_loop()
            self.__thread_loop = uvloop.new_event_loop()

            self.__put_future: Future[dict[str, Any] | Literal['STOP']] = self.__thread_loop.create_future()
            self.__get_future: Future[list[Event] | None | Literal['ERROR']] = self.__main_loop.create_future()

            self.__killed = False

            self.__thread = threading.Thread(target=self.run, daemon=True)
            self.__thread.start()

        return result

    def stop(self) -> None:
        if self.__killed:
            return

        self.__thread_loop.call_soon_threadsafe(self.__put_future.set_result, 'STOP')

    def join(self):
        self.__thread.join()


async def main():
    # -Xgil=0
    print(os.listdir('/code/venv/bin'))
    # print(sys._is_gil_enabled())
    thread = WorkerThread(TestWorker)

    events = [
        Event(
            target='',
            kwargs={
                'a': 0
            }
        ),
        Event(
            target='',
            kwargs={
                'a': 0
            }
        ),
        Event(
            target='',
            kwargs={
                'a': 100
            }
        ),
        Event(
            target='',
            kwargs={
                'a': 0
            }
        )
    ]

    for event in events:
        r = await thread.run_task(event.kwargs, 2)
        print(r)

    thread.stop()
    thread.join()


if __name__ == '__main__':
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
        runner.run(main())
