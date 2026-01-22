import time
from multiprocessing import connection

from orjson import dumps

from everwork.schemas import WorkerSettings


class HeartbeatNotifier:

    def __init__(self, conn: connection.Connection) -> None:
        self._conn = conn

    def notify_started(self, worker_settings: WorkerSettings) -> None:
        data = {
            'worker_name': worker_settings.name,
            'end_time': time.time() + worker_settings.execution_timeout
        }

        self._conn.send_bytes(dumps(data))

    def notify_completed(self) -> None:
        self._conn.send_bytes(b'')

    def close(self) -> None:
        self._conn.close()
