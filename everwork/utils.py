import asyncio
import time
from typing import List

from redis.asyncio import Redis

from everwork.shemes import FloodControlSetting, Timeout


# class FloodControl:
#
#     def __init__(self, redis: Redis, worker_name: str, settings: List[FloodControlSetting]):
#         self.__redis = redis
#
#         self.__keys = []
#         self.__static_keys = []
#
#         for setting in settings:
#             self.__keys.append(f'{worker_name}:flood_control:{setting.n_times_per_period}:{setting.get_interval_ns()}')
#
#             self.__static_keys.append(f'{setting.get_interval_ns()}')
#             self.__static_keys.append(f'{setting.n_times_per_period}')
#
#         self.__script_sha: str | None = None
#
#     async def __register_script(self) -> None:
#         script = """
#             local max_sleep_time_ns = 0
#             local current_time_ns = tonumber(ARGV[1])
#
#             for i, key in ipairs(KEYS) do
#                 local interval_ns = tonumber(ARGV[2 * i])
#                 local max_requests = tonumber(ARGV[2 * i + 1])
#
#                 redis.call('ZREMRANGEBYSCORE', key, '-inf', current_time_ns - interval_ns)
#
#                 local request_count = redis.call('ZCARD', key)
#
#                 if request_count + 1 > max_requests then
#                     local last_entry = redis.call('ZRANGE', key, 0, 0)
#                     local sleep_time_ns = tonumber(last_entry[1]) + interval_ns - current_time_ns
#
#                     if sleep_time_ns > max_sleep_time_ns then
#                         max_sleep_time_ns = sleep_time_ns
#                     end
#                 end
#             end
#
#             local adjusted_time_ns = current_time_ns + max_sleep_time_ns
#
#             for i, key in ipairs(KEYS) do
#                 redis.call('ZADD', key, adjusted_time_ns, adjusted_time_ns)
#             end
#
#             return max_sleep_time_ns
#         """
#
#         self.__script_sha = await self.__redis.script_load(script)
#
#     async def sleep(self) -> None:
#         if self.__script_sha is None:
#             await self.__register_script()
#
#         max_sleep_time_ns = await self.__redis.evalsha(
#             self.__script_sha,
#             len(self.__keys), *self.__keys,
#             str(time.time_ns()), *self.__static_keys
#         )
#
#         if not max_sleep_time_ns:
#             return
#
#         await asyncio.sleep(int(max_sleep_time_ns) / 1e9)


async def waiting_worker_timeout(redis: Redis, worker_name: str, timeout: Timeout) -> None:
    last_work: float = await redis.get(f'{worker_name}:last_work')

    if last_work + timeout.active_lifetime < time.time():
        delay = timeout.inactive_timeout
    else:
        delay = timeout.active_timeout

    return await asyncio.sleep(delay)


async def set_last_work(redis: Redis, worker_name: str) -> None:
    await redis.set(f'{worker_name}:last_work', time.time())


async def get_is_worker_on(redis: Redis, worker_name: str) -> bool:
    value: int = await redis.get(f'{worker_name}:is_worker_on')
    return bool(value)


async def trigger_timeout(redis: Redis, worker_name: str, timeout: float) -> bool:
    last_work: float = await redis.get(f'{worker_name}:last_work')
    return last_work + timeout < time.time()


async def register_limit_args():
    pass


class LimitArgs:
    pass


class QueueEvents:
    pass
