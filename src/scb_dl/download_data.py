import argparse
import ast
import asyncio
import string
import sys
import tempfile
from functools import partial
from itertools import islice, product

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
from asyncstdlib.functools import reduce
from gcloud.aio.storage import Storage
from tqdm import tqdm

from .mcpp import maximize_constrained_partial_product
from .utils import read_info_gc, retry, throttle

domain = 'https://api.scb.se'


def batched(iterable, n):
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def url_from_table_path(table_path):
    return domain.strip('/') + '/' + table_path.strip('/')


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
    names += [
        parse_name(column['text']) + '__code'
        for column in data['columns']
        if column['type'] != 'c'
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
        return await _get_data(
            get,
            info,
            dict(zip(codes_to_iterate_over, values)),
        )

    # optimal_download_time =
    # table_size / (max_size_per_request * requests_per_second)
    print('optimal download time [s]:', table_size / (100_000 * 1))

    with tqdm(total=table_rows) as pbar:

        async def merge(table, tasks):
            new = table
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
            return new

        return await reduce(
            merge,
            batched(map(get_chunk, values_in_each_chunk), n=5),
            None,
        )


async def _main(repo, table_prefix):
    async with aiohttp.ClientSession() as session:

        @retry(wait_time=10, max_tries=5, timeout=float('inf'))
        @throttle(interval_seconds=10, max_calls_in_interval=9)
        async def get(url, query):
            res = await session.post(url, json=query)
            if res.status != 200:
                print(res.status, await res.text(), query, file=sys.stderr)
            return await res.json()

        async for name, info in list_tables(
            repo.removeprefix('gs://'), table_prefix
        ):
            print(name)
            table = await get_data(
                partial(get, url_from_table_path(name)), info
            )

            with tempfile.NamedTemporaryFile() as f:
                pq.write_table(table, f.name)

                async with Storage() as client:
                    await client.upload_from_filename(
                        'scb-v1',
                        '_'.join(name.strip('/').split('/')[-2:]) + '.parquet',
                        f.name,
                    )


async def list_tables(bucket, prefix):
    async with Storage() as client:
        params = dict(prefix=prefix)
        while True:
            response = await client.list_objects(bucket, params=params)
            for item in response['items']:
                table = item['name'].removesuffix('.json')
                info = await read_info_gc(bucket, table)
                if isinstance(info, list):
                    # Not a table
                    continue
                yield table, info

            if (pagetoken := response.get('nextPageToken')) is None:
                break
            else:
                params['pageToken'] = pagetoken


def main():
    parser = argparse.ArgumentParser(
        prog='scb-download',
        description=(
            'Downloads table from the SCB api '
            'and stores it locally or in google cloud storage',
        ),
    )
    parser.add_argument('--info-dir', default='gs://api-scb-se')
    parser.add_argument('--remote', action='store_true', default=False)
    parser.add_argument('--table', type=lambda v: v.strip('/'), default='')
    args = parser.parse_args()
    asyncio.run(
        _main(
            args.info_dir,
            args.table,
        )
    )
