from loguru import logger
from redis.asyncio import Redis

from everwork.process import ProcessState


async def register_move_by_value_script(redis: Redis) -> str:
    return await redis.script_load(
        """
        local value = ARGV[1]
        local source_list = KEYS[1]
        local destination_list = KEYS[2]
    
        local index = redis.call('LPOS', source_list, value)
    
        if index then
            redis.call('LREM', source_list, 0, value)
            redis.call('RPUSH', destination_list, value)
            return 1
        else
            return 0
        end
        """
    )


async def return_limit_args(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    value = await redis.evalsha(
        script_sha, 2, f'worker:{worker_name}:taken_limit_args', f'worker:{worker_name}:limit_args', raw_value
    )

    if bool(value):
        return None

    logger.warning(f'Невозможно вернуть {raw_value} в {worker_name}')


async def cancel_event(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    value = await redis.evalsha(
        script_sha,
        2, f'worker:{worker_name}:taken_events', f'worker:{worker_name}:events',
        raw_value
    )

    if bool(value):
        return None

    logger.warning(f'Невозможно отменить событие {raw_value} в {worker_name}')


async def remove_event(redis: Redis, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    await redis.lrem(f'worker:{worker_name}:taken_events', 1, raw_value)


async def return_event(redis: Redis, script_sha: str, worker_name: str, raw_value: str | None) -> None:
    if raw_value is None:
        return None

    value = await redis.evalsha(
        script_sha,
        2, f'worker:{worker_name}:taken_events', f'worker:{worker_name}:error_events',
        raw_value
    )

    if bool(value):
        return None

    logger.warning(f'Невозможно поместить событие {raw_value} в ошибки в {worker_name}')


async def set_process_state(redis: Redis, index: int, end_time: float | None) -> None:
    await redis.set(
        f'process:{index}:state',
        ProcessState(status='waiting' if end_time is None else 'running', end_time=end_time).model_dump_json()
    )
