"""Bounded cache of grouped view results shared by all country selections."""
from collections import OrderedDict
from copy import deepcopy
import threading
import time

from .metrics import Metrics, make_window
from .snapshot import all_series, all_event_types


class QueryCache:
    def __init__(self, store, ttl=15, capacity=8):
        self.store, self.ttl, self.capacity = store, ttl, capacity
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def resolve(self, name, days, country, fresh=False):
        country = country.upper()
        win = make_window(days)
        params = self.store.get_state('parameter_version', 0)
        key = (name, days, params, win.start)
        with self.lock:
            entry = self.cache.get(key)
            if fresh or entry is None or time.monotonic()-entry[0] >= self.ttl:
                with self.store.read_snapshot() as reader:
                    metrics = Metrics(reader)
                    payload = getattr(metrics, name.replace('-', '_'))(days=days)
                    record = {'common': payload,
                              'series': all_series(metrics, days, 'attitude' if name == 'overview' else name)}
                    if name == 'attitude':
                        record['event_types'] = all_event_types(metrics, days)
                entry = (time.monotonic(), record)
                self.cache[key] = entry
                while len(self.cache) > self.capacity:
                    self.cache.popitem(last=False)
            self.cache.move_to_end(key)
            record = entry[1]
            result = deepcopy(record['common'])
            result['cache_age_seconds'] = round(time.monotonic()-entry[0], 2)
            result['selected'] = next((r for r in result['countries'] if r['code'] == country), None)
            if name != 'overview' or country:
                result['series'] = deepcopy(record['series'].get(country, []))
            if name == 'attitude':
                result['event_types'] = deepcopy(record['event_types'].get(country, []))
            latest = result['coverage']['last_file_ts']
            from .store import utcnow
            result['coverage']['stale'] = latest is None or int(utcnow().timestamp())-latest > 2700
            return result
