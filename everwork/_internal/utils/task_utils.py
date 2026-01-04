import asyncio
from contextlib import suppress
from multiprocessing import connection
from typing import Any, Coroutine


class OperationCancelled(Exception):
    pass


async def wait_for_or_cancel[T](coroutine: Coroutine[Any, Any, T], event: asyncio.Event) -> T:
    main_task = asyncio.create_task(coroutine)
    event_task = asyncio.create_task(event.wait())

    tasks = {main_task, event_task}

    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        if event.is_set():
            main_task.cancel()

            with suppress(asyncio.CancelledError):
                await main_task

            raise OperationCancelled

        return await main_task
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))


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
