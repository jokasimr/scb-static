import argparse
import asyncio
import os
import time
from functools import partial

import aiohttp

from .utils import retry, save_to_gc, save_to_local, throttle

domain = "https://api.scb.se"
root = f"{domain}/OV0104/v1/doris/sv/ssd"


async def metadata(get, urls):
    async def _get(url):
        data = await get(url)
        return url, data

    for coro in asyncio.as_completed(list(map(_get, urls))):
        url, data = await coro
        yield (url, data)
        if isinstance(data, list):
            async for item in metadata(
                get, (f'{url.removesuffix("/")}/{d["id"]}' for d in data)
            ):
                yield item


def to_local_path(base, url):
    return os.path.join(base, url.removeprefix(domain).strip("/") + '.json')


def to_gc_path(url):
    return url.removeprefix(domain).strip("/") + '.json'


async def _main(save, start_from):
    n_downloaded = 0

    async def download_metadata():
        async with aiohttp.ClientSession() as session:

            @retry(wait_time=10, max_tries=5, timeout=float('inf'))
            @throttle(interval_seconds=10, max_calls_in_interval=9)
            async def get(url):
                res = await session.get(url)
                if res.status != 200:
                    print('x', end='', flush=True)
                    print()
                    print(url)
                    raise RuntimeError(
                        'Request failed', res.status, await res.text()
                    )
                print('.', end='', flush=True)
                return await res.json()

            async for item in metadata(get, ['/'.join((root, start_from))]):
                nonlocal n_downloaded
                n_downloaded += 1
                yield item

    start = time.time()
    await save(download_metadata())
    end = time.time()

    print()
    print(
        'Total time:',
        f'{end - start:0.2f}',
        'No. downloaded:',
        f'{n_downloaded:0.2f}',
        'dl/s:',
        f'{n_downloaded / (end - start):0.2f}',
    )


def main():
    parser = argparse.ArgumentParser(
        prog='scb-download-meta',
        description=(
            'Downloads meta data from the SCB api '
            'and stores it locally or in google cloud storage'
        ),
    )
    parser.add_argument('--bucket-name', default='api-scb-se')
    parser.add_argument('--dir-name', default='api.scb.se')
    parser.add_argument('--remote', action='store_true', default=False)
    parser.add_argument(
        '--start-from', type=lambda v: v.strip('/'), default=''
    )
    args = parser.parse_args()
    asyncio.run(
        _main(
            partial(save_to_gc, args.bucket_name, to_gc_path)
            if args.remote
            else partial(save_to_local, partial(to_local_path, args.dir_name)),
            args.start_from,
        )
    )
