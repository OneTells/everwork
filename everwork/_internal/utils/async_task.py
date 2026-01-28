import asyncio
from typing import Any, Coroutine


class OperationCancelled(Exception):
    pass


async def _wait_for_event_with_min_timeout(event: asyncio.Event, min_timeout: int) -> None:
    await event.wait()
    await asyncio.sleep(min_timeout)


async def wait_for_or_cancel[T](
    coroutine: Coroutine[Any, Any, T],
    event: asyncio.Event,
    min_timeout: int = 0,
    max_timeout: int | None = None,
) -> T:
    main_task = asyncio.create_task(coroutine)
    event_task = asyncio.create_task(
        _wait_for_event_with_min_timeout(event, min_timeout)
    )

    tasks = {main_task, event_task}

    try:
        async with asyncio.timeout(max_timeout):
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        if not main_task.done():
            raise OperationCancelled

        return await main_task
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
