import argparse
import ast
import asyncio
import json
import os
import shutil
import string
import subprocess
import sys
import tempfile
from datetime import datetime
from functools import partial
from itertools import islice, product

import aiohttp
import pyarrow as pa
import pyarrow.dataset as pds
from tqdm import tqdm

from .mcpp import maximize_constrained_partial_product
from .utils import retry, throttle

MAX_CELLS = 107_762


def batched(iterable, n):
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def url_from_table_path(table_path):
    base_url = 'https://api.scb.se'
    return base_url.strip('/') + '/' + table_path.strip('/')


def parse_value(lookup, info, column, value):
    if value == '..':
        return None
    if column['type'] == 'c':
        return ast.literal_eval(value)
    if column['type'] == 't':
        try:
            return int(value)
        except Exception:
            pass
    return lookup[column['code']][value]


def parse_name(name):
    name = name.lower()
    for c, r in zip('åäö', 'aao'):
        name = name.replace(c, r)
    name = name.replace(' ', '_')
    return ''.join(
        c for c in name if c in (string.digits + string.ascii_letters + '_')
    )


async def _get_data(get, info, set_variables):
    query = {
        'query': [
            *(
                {
                    'code': var['code'],
                    'selection': {
                        'filter': 'item',
                        'values': [set_variables[var['code']]],
                    },
                }
                for var in info['variables']
                if var['code'] in set_variables
            ),
            *(
                {
                    'code': var['code'],
                    'selection': {'filter': 'all', 'values': ['*']},
                }
                for var in info['variables']
                if (
                    var['code'] != 'ContentsCode'
                    and var['code'] not in set_variables
                )
            ),
        ],
        'response': {'format': 'json'},
    }
    data = await get(query)
    _lookup = {
        var['code']: dict(zip(var['values'], var['valueTexts']))
        for var in info['variables']
    }
    columns = [
        [
            parse_value(_lookup, info, column, (row['key'] + row['values'])[i])
            for row in data['data']
        ]
        for i, column in enumerate(data['columns'])
    ]
    columns += [
        [row['key'][i] for row in data['data']]
        for i, column in enumerate(data['columns'])
        if column['type'] != 'c'
    ]
    names = [parse_name(column['text']) for column in data['columns']]
    names = [
        name if name not in names[:i] else f'{name}_varde'
        for i, name in enumerate(names)
    ]
    names += [
        parse_name(column['text']) + '__code'
        for column in data['columns']
        if column['type'] != 'c'
    ]
    return pa.table(columns, names=names)


async def get_data(get, url, info, max_download_time):
    key_field_lengths = {
        var["code"]: len(var["values"])
        for var in info["variables"]
        if var["code"] != "ContentsCode"
    }
    value_fields = next(
        len(var["values"])
        for var in info["variables"]
        if var["code"] == "ContentsCode"
    )
    table_size = value_fields
    table_rows = 1
    for length in key_field_lengths.values():
        table_size *= length
        table_rows *= length

    dimensions_to_iterate_over = ()

    if table_size > MAX_CELLS:
        dimensions_to_iterate_over = maximize_constrained_partial_product(
            tuple(key_field_lengths.values()), MAX_CELLS // value_fields
        )

    _key_codes = list(key_field_lengths.keys())
    codes_to_iterate_over = [_key_codes[d] for d in dimensions_to_iterate_over]
    values_in_each_chunk = product(
        *(
            next(var for var in info["variables"] if var["code"] == code)[
                "values"
            ]
            for code in codes_to_iterate_over
        )
    )

    async def get_chunk(values):
        async with aiohttp.ClientSession() as session:
            return await _get_data(
                partial(get, session, url),
                info,
                dict(zip(codes_to_iterate_over, values)),
            )

    # optimal_download_time =
    # table_size / (max_size_per_request * requests_per_second)
    optimal_download_time = table_size / (MAX_CELLS * 1)
    print('optimal download time [s]:', optimal_download_time)
    if optimal_download_time > max_download_time:
        yield None
        return

    has_yielded_schema = False

    with tqdm(total=table_rows) as pbar:
        for tasks in batched(map(get_chunk, values_in_each_chunk), n=90):
            new = None
            for chunk in asyncio.as_completed(tasks):
                chunk = await chunk
                new = (
                    pa.concat_tables(
                        (new, chunk), promote_options="permissive"
                    )
                    if new is not None
                    else chunk
                )
                pbar.update(len(chunk))
            if not has_yielded_schema:
                yield new.schema
                has_yielded_schema = True
            for b in new.to_batches():
                yield b


def syncify(async_chunk_iterator):
    loop = asyncio.new_event_loop()
    while True:
        task = asyncio.ensure_future(anext(async_chunk_iterator), loop=loop)
        try:
            yield loop.run_until_complete(task)
        except StopAsyncIteration:
            break
    loop.close()


def _main(start_from, sync_metadata, max_download_time_seconds):
    upload_tasks = []

    def go_through_tasks_remove_done(tasks, final=False):
        new_tasks = []
        for dirname, proc in tasks:
            if final:
                proc.wait()
            elif proc.poll() is None:
                new_tasks.append((dirname, proc))
                continue
            shutil.rmtree(dirname)
        return new_tasks

    @retry(wait_time=10, max_tries=10, timeout=float('inf'))
    @throttle(interval_seconds=10, max_calls_in_interval=9)
    async def get(session, url, query):
        res = await session.post(url, json=query)
        if res.status != 200:
            print(res.status, await res.text(), query, file=sys.stderr)
        return await res.json()

    for name, info in list_tables(start_from, sync_metadata):
        print(datetime.now().isoformat(), name)
        try:
            data = syncify(
                get_data(
                    get,
                    url_from_table_path(name),
                    info,
                    max_download_time_seconds,
                )
            )
            schema = next(data)
            if schema is None:
                # The download was canceled because it would take too much time
                print(name, "canceled because download time exceeds the limit")
                continue
            dirname = tempfile.mkdtemp()
            filename = '_'.join(name.strip('/').split('/')[-2:])
            pds.write_dataset(
                data,
                dirname,
                schema=schema,
                basename_template=f'{filename}-{{i}}.parquet',
                format='parquet',
            )
            upload_tasks.append(
                (
                    dirname,
                    subprocess.Popen(
                        ['/usr/bin/rclone', 'copy', dirname, 'r2:scb-tables']
                    ),
                )
            )
            upload_tasks = go_through_tasks_remove_done(upload_tasks)
        except Exception as e:
            print(f"Failed to collect table {name}", e)

    go_through_tasks_remove_done(upload_tasks, final=True)


def list_tables(matching, sync_metadata):
    meta_dir = './api-scb-se'
    if sync_metadata:
        print('Syncing table metadata... ', end='')
        subprocess.run(['/usr/bin/rclone', 'sync', 'r2:scb-meta/', meta_dir])
        print('done.')
    for dirpath, _, filenames in os.walk(meta_dir):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            if matching in path:
                with open(path) as f:
                    info = json.load(f)
                    if isinstance(info, list):
                        # Not a table
                        continue
                    yield path.removeprefix(meta_dir).removesuffix(
                        '.json'
                    ), info


def main():
    parser = argparse.ArgumentParser(
        prog='scb-download',
        description=(
            'Downloads table from the SCB api '
            'and stores it locally or in google cloud storage'
        ),
    )
    parser.add_argument(
        '--max-optimal-download-time-seconds', type=float, default=float('inf')
    )
    parser.add_argument('--remote', action='store_true', default=False)
    parser.add_argument(
        '--start-from', type=lambda v: v.strip('/'), default=''
    )
    parser.add_argument(
        '--no-sync-metadata', action='store_true', default=False
    )
    args = parser.parse_args()
    _main(
        args.start_from,
        not args.no_sync_metadata,
        max_download_time_seconds=args.max_optimal_download_time_seconds,
    )
