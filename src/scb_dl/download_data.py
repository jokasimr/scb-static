import argparse
import ast
import asyncio
import string
import sys
from itertools import islice, product

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
from asyncstdlib.functools import reduce
from tqdm import tqdm

from .mcpp import maximize_constrained_partial_product
from .utils import read_info_gc, read_info_local, retry, throttle


def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


domain = 'https://api.scb.se'


def url_from_table_path(table_path):
    return domain.strip('/') + '/' + table_path.strip('/')


def parse_value(info, column, value):
    if value == '..':
        return None

    if column['type'] == 'c':
        return ast.literal_eval(value)

    if column['type'] == 't':
        try:
            return int(value)
        except Exception:
            pass

    var = next(
        var for var in info['variables'] if var['code'] == column['code']
    )
    index = var['values'].index(value)
    assert index >= 0
    return var['valueTexts'][index]


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
    columns = [
        [
            parse_value(info, column, (row['key'] + row['values'])[i])
            for row in data['data']
        ]
        for i, column in enumerate(data['columns'])
    ]

    names = [
        ''.join(
            ch
            for ch in column['text'].replace(' ', '_')
            if ch in (string.digits + string.ascii_letters + '_')
        )
        for i, column in enumerate(data['columns'])
    ]
    return pa.table(columns, names=names)


async def get_data(get, info):
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

    if table_size > 100_000:
        dimensions_to_iterate_over = maximize_constrained_partial_product(
            tuple(key_field_lengths.values()), 100_000 // value_fields
        )

    codes_to_iterate_over = [
        info['variables'][d]['code'] for d in dimensions_to_iterate_over
    ]
    values_in_each_chunk = product(
        *(info['variables'][d]['values'] for d in dimensions_to_iterate_over)
    )

    async def get_chunk(values):
        return await _get_data(
            get,
            info,
            dict(zip(codes_to_iterate_over, values)),
        )

    with tqdm(total=table_rows) as pbar:

        async def merge(table, tasks):
            old = table
            for chunk in asyncio.as_completed(tasks):
                chunk = await chunk
                new = pa.concat_tables((old, chunk)) if old else chunk
                pbar.update(len(new) - (len(old) if old else 0))
            return new

        return await reduce(
            merge,
            batched(map(get_chunk, values_in_each_chunk), n=5),
            None,
        )


async def _main(url, info, path):
    async with aiohttp.ClientSession() as session:

        @retry(wait_time=10, max_tries=5, timeout=float('inf'))
        @throttle(interval_seconds=10, max_calls_in_interval=10)
        async def get(query):
            res = await session.post(url, json=query)
            if res.status != 200:
                print(res.status, await res.text(), file=sys.stderr)
            return await res.json()

        table = await get_data(get, info)
    pq.write_table(table, path)


def main():
    parser = argparse.ArgumentParser(
        prog='scb-download',
        description=(
            'Downloads table from the SCB api '
            'and stores it locally or in google cloud storage',
        ),
    )
    parser.add_argument('--bucket-name', default='api-scb-se')
    parser.add_argument('--dir-name', default='api.scb.se')
    parser.add_argument('--remote', action='store_true', default=False)
    parser.add_argument('--table', type=lambda v: v.strip('/'), default='')
    parser.add_argument('--path', default='example.parquet')
    args = parser.parse_args()
    asyncio.run(
        _main(
            url_from_table_path(args.table),
            read_info_gc(args.bucket_name, args.table)
            if args.remote
            else read_info_local(args.dir_name, args.table),
            args.path,
        )
    )
