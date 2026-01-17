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


async def poll_connection(conn: connection.Connection, timeout: float | None = None) -> bool:
    if conn.poll(0):
        return True

    fd = conn.fileno()

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def _on_readable() -> None:
        if not future.done():
            future.set_result(True)

    loop.add_reader(fd, _on_readable)  # type: ignore

    try:
        if conn.poll(0):
            return True

        async with asyncio.timeout(timeout):
            await future
    except asyncio.TimeoutError:
        return False
    finally:
        loop.remove_reader(fd)

    return True
