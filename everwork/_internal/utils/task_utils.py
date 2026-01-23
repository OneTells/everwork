import asyncio
from typing import Any, Coroutine


class OperationCancelled(Exception):
    pass


async def wait_for_or_cancel[T](
    coroutine: Coroutine[Any, Any, T],
    event: asyncio.Event,
    timeout: int | None = None
) -> T:
    main_task = asyncio.create_task(coroutine)
    event_task = asyncio.create_task(event.wait())

    tasks = {main_task, event_task}

    try:
        async with asyncio.timeout(timeout):
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        if event.is_set():
            raise OperationCancelled

        return await main_task
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
