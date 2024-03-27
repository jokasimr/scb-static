import os
import time
import urllib
import json
from collections import deque
import requests

root = 'https://api.scb.se/OV0104/v1/doris/sv/ssd'
base = 'api.scb.se'


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


def to_path(url):
    return os.path.join(base, *url.lstrip(root).split('/'), 'meta.json')


def get_all_metadata(throttler, urls):
    for url in urls:
        if os.path.exists(to_path(url)):
            with open(to_path(url)) as f:
                data = json.load(f)
            print(url, 'cached')
        else:
            response = throttler.schedule(requests.get, url)
            print(url, response.status_code)
            data = response.json()
        yield (url, data)
        if isinstance(data, list):
            suburls = (
                f'{url}/{el["id"]}'
                for el in data
            )
            yield from get_all_metadata(throttler, suburls)


def save_metadata():
    throttler = Throttler(10, 10, wait_time=0.5)
    for url, data in get_all_metadata(throttler, [root]):
        os.makedirs(os.path.dirname(to_path(url)), exist_ok=True)
        with open(to_path(url), 'w') as f:
            json.dump(data, f)


def save_metadata_flat():
    dirname = 'api.scb.se-flat'

    def to_path(url, data):
        if isinstance(data, list):
            return os.path.join(dirname, '_'.join(url.lstrip('root').split('/')[-1:]) + '.json')
        return os.path.join(dirname, '_'.join(url.lstrip('root').split('/')[-2:]) + '.json')

    throttler = Throttler(10, 10, wait_time=0.5)
    os.makedirs(dirname)
    for url, data in get_all_metadata(throttler, [root]):
        path = to_path(url, data)
        with open(path, 'w') as f:
            if not isinstance(data, list):
                data['url'] = url
            json.dump(data, f)


save_metadata_flat()
