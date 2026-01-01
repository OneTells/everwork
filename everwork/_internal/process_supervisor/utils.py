import asyncio
from multiprocessing import connection


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
