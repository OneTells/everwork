import asyncio
from contextlib import suppress
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
