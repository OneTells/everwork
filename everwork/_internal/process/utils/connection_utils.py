import asyncio
from multiprocessing import connection


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
