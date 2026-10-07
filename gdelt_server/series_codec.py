"""Lossless columnar trend transport; expand only the requested country/window."""
from datetime import datetime, timezone


def pack_series(value):
    if isinstance(value, dict):
        return {k: pack_series(v) for k, v in value.items()}
    if not isinstance(value, list):
        return value
    if not value or not all(isinstance(p, dict) and 'bucket' in p and 'timestamp' in p for p in value):
        return [pack_series(v) for v in value]
    fields = list(dict.fromkeys(k for p in value for k in p))
    derived = all(type(p['bucket']) is int and p['timestamp'] == datetime.fromtimestamp(p['bucket'], timezone.utc).isoformat() for p in value)
    if derived:
        fields.remove('timestamp')
    masks = [sum(1 << i for i, k in enumerate(fields) if k in p) for p in value]
    result = {'_series': 'columns-v1', 'fields': fields,
              'rows': [[p.get(k) for k in fields] for p in value]}
    if derived:
        result['utc_timestamp'] = True
    if any(m != (1 << len(fields))-1 for m in masks):
        result['masks'] = masks
    return result


def iter_series(value):
    if isinstance(value, list):
        yield from value
        return
    if not isinstance(value, dict) or value.get('_series') != 'columns-v1':
        raise ValueError('趋势编码不合法')
    fields, rows = value.get('fields'), value.get('rows')
    if (not isinstance(fields, list) or not 1 <= len(fields) <= 64
            or any(not isinstance(k, str) for k in fields) or len(set(fields)) != len(fields)
            or 'bucket' not in fields or not isinstance(rows, list) or len(rows) > 10000):
        raise ValueError('趋势列结构不合法')
    derived = value.get('utc_timestamp', False)
    if type(derived) is not bool or (derived and 'timestamp' in fields):
        raise ValueError('趋势日期编码不合法')
    masks = value.get('masks')
    if masks is not None and (not isinstance(masks, list) or len(masks) != len(rows)):
        raise ValueError('趋势字段掩码不合法')
    for i, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != len(fields):
            raise ValueError('趋势列长度不合法')
        mask = masks[i] if masks is not None else (1 << len(fields))-1
        if type(mask) is not int or not 0 <= mask < 1 << len(fields):
            raise ValueError('趋势字段掩码不合法')
        point = {k: row[j] for j, k in enumerate(fields) if mask & (1 << j)}
        if derived:
            bucket = point.get('bucket')
            if type(bucket) is not int or not 0 <= bucket <= 253402300799:
                raise ValueError('趋势时间不合法')
            point['timestamp'] = datetime.fromtimestamp(bucket, timezone.utc).isoformat()
        yield point


def series_length(value):
    if isinstance(value, list):
        return len(value)
    # Full structure/row validation happens when iter_series is consumed.
    if isinstance(value, dict) and isinstance(value.get('rows'), list):
        return len(value['rows'])
    raise ValueError('趋势结构不合法')


def expand_series(value):
    return list(iter_series(value))
