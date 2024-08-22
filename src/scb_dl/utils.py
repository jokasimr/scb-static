import asyncio
import json
import os
import random
import sys
import time
from collections import deque
from functools import wraps

from gcloud.aio.storage import Storage


def throttle(
    *,
    interval_seconds,
    max_calls_in_interval,
    wait_time_seconds=None,
    min_time_between_calls=None,
):
    if wait_time_seconds is None:
        wait_time_seconds = interval_seconds / max_calls_in_interval / 10
    if min_time_between_calls is None:
        min_time_between_calls = interval_seconds / max_calls_in_interval / 2

    call_times = deque(maxlen=max_calls_in_interval)

    def decorator(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            while (
                len(call_times) == max_calls_in_interval
                and time.time() - call_times[0] <= interval_seconds
            ) or (
                call_times
                and time.time() - call_times[-1] <= min_time_between_calls
            ):
                await asyncio.sleep(wait_time_seconds * random.random())

            call_times.append(time.time())
            return await f(*args, **kwargs)

        return wrapper

    return decorator


def retry(*, wait_time=1, max_tries=10, timeout=60, exception_type=Exception):
    def decorator(f):
        @wraps(f)
        async def wrapper(*args, **kwargs):
            start = time.time()
            tries = 0
            latest_exception = None
            while True:
                try:
                    out = await f(*args, **kwargs)
                except exception_type as e:
                    print('retry', tries, e, file=sys.stderr)
                    latest_exception = e
                    await asyncio.sleep(wait_time)
                else:
                    return out
                if time.time() >= start + timeout:
                    raise RuntimeError(
                        'Timeout was reached, last exception was',
                        latest_exception,
                    )
                tries += 1
                if tries >= max_tries:
                    raise RuntimeError(
                        'Maximum number of retries was reached, '
                        'last exception was',
                        latest_exception,
                    )

        return wrapper

    return decorator


async def save_to_local(to_path, docs):
    async for url, doc in docs:
        path = to_path(url)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(doc, f)


async def save_to_gc(bucket_name, to_path, docs):
    async with Storage() as client:
        async with asyncio.TaskGroup() as tg:
            async for url, doc in docs:
                tg.create_task(
                    retry()(client.upload)(
                        bucket_name,
                        to_path(url),
                        json.dumps(doc),
                    )
                )


def read_info_local(directory, table_path):
    with open(os.path.join(directory, table_path.strip('/')) + '.json') as f:
        return json.load(f)


async def read_info_gc(bucket, table_path):
    async with Storage() as client:
        data = await retry()(client.download)(
            bucket, table_path.strip('/') + '.json'
        )
        return json.loads(data)
