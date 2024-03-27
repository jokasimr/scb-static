import os
import sys
import ast
import time
import urllib
import string
import json
from itertools import product
from collections import deque
import requests

from mcpp import maximize_constrained_partial_product


import pyarrow as pa
import pyarrow.parquet as pq


class Throttler:
    def __init__(self, limit, time_limit, wait_time=0.1):
        self.queue = deque(maxlen=limit)
        self.wait_time = wait_time
        self.time_limit = time_limit

    def schedule(self, f, *args, **kwargs):
        if len(self.queue) == self.queue.maxlen:
            while time.time() - self.queue[0] <= self.time_limit:
                print('waiting..')
                time.sleep(self.wait_time)

        out = f(*args, **kwargs)
        self.queue.append(time.time())
        return out



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


throttler = Throttler(10, 10)


def _get_data(info, set_variables):

    query = {
        'query': [
            *(
                {"code": var["code"],
                 "selection": {
                     "filter": "item",
                     "values": [set_variables[var["code"]]]
                 }
                }
                for var in info["variables"]
                if var["code"] in set_variables
            ),
            *(
                {"code": var["code"],
                 "selection": {
                     "filter": "all",
                     "values": ["*"]
                 }
                }
                for var in info["variables"]
                if var["code"] != "ContentsCode" and var["code"] not in set_variables
            )
        ],
        "response": {"format": "json"}
    }

    res = throttler.schedule(
        requests.post,
        info['url'],
        json=query
    )

    print(res.status_code)
    if res.status_code != 200:
        print(res.text)
        exit(1)

    data = res.json()

    columns = [
        [
            parse_value(info, column, (row['key'] + row['values'])[i])
            for row in data['data']
        ]
        for i, column in enumerate(data['columns'])
    ]

    names = [
        ''.join(ch for ch in column['text'].replace(' ', '_') if ch in (string.digits + string.ascii_letters + '_'))
        for i, column in enumerate(data['columns'])
    ]
    return pa.table(columns, names=names)


def get_data(table_path):
    with open(table_path) as f:
        info = json.load(f)

    key_field_lengths = {
        var['code']: len(var['values']) 
        for var in info['variables']
        if var['code'] != 'ContentsCode'
    }
    value_fields = next(
        len(var['values'])
        for var in info['variables'] 
        if var['code'] == 'ContentsCode'
    )
    table_size = value_fields
    for l in key_field_lengths.values():
        table_size *= l

    dimensions_to_iterate_over = ()

    print("Table size:", table_size)

    if table_size > 100_000:
        dimensions_to_iterate_over = maximize_constrained_partial_product(
            tuple(key_field_lengths.values()),
            100_000 // value_fields
        )

    codes_to_iterate_over = [
        info['variables'][d]['code']
        for d in dimensions_to_iterate_over
    ]
    table = None

    for values in product(*(info['variables'][d]["values"] for d in dimensions_to_iterate_over)):

        set_values = {
            k: v
            for k, v in zip(codes_to_iterate_over, values)
        }

        chunk = _get_data(info, set_values) 
        table = (
            pa.concat_tables((
                table,
                chunk,
            )) if table is not None else chunk
        )

        # TODO: remove
        pq.write_table(table, 'example.parquet')
    return table


table = get_data(sys.argv[1])
pq.write_table(table, 'example.parquet')
