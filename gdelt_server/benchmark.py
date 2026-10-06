"""Small isolated comparison; never opens the production database."""
from pathlib import Path
import math
import re
import tempfile

import httpx

from .ingest import Ingestor, BASE
from .store import Store, SLOT, SPECS


def _aggregates(store):
    with store.connect() as db:
        result = {}
        for table, (metrics, dims) in SPECS.items():
            keys = ('bucket', *dims)
            result[table] = {tuple(row[:len(keys)]): tuple(row[len(keys):]) for row in db.execute(
                f"SELECT {','.join((*keys, *metrics))} FROM {table} WHERE granularity='day'")}
        return result


def _equivalent(left, right):
    return left.keys() == right.keys() and all(
        left[table].keys() == right[table].keys() and all(
            math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-8)
            for key in left[table] for a, b in zip(left[table][key], right[table][key]))
        for table in left)


def real_benchmark(settings, slots=4):
    if not 1 <= slots <= 12:
        raise ValueError('对照测试仅支持1至12个十五分钟时段')
    with httpx.Client(timeout=settings.request_timeout, follow_redirects=True) as client:
        response = client.get(BASE+'/lastupdate.txt')
        response.raise_for_status()
        from datetime import datetime, timezone
        timestamps = []
        for kind in ('export.CSV', 'gkg.csv'):
            match = re.search(r'(\d{14})\.'+re.escape(kind)+r'\.zip', response.text)
            if not match:
                raise ValueError('官方索引未包含完整Events/GKG批次')
            timestamps.append(int(datetime.strptime(match[1], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc).timestamp()))
    end = min(timestamps)+SLOT
    results, totals = {}, []
    with tempfile.TemporaryDirectory(prefix='gdelt-benchmark-') as temp:
        for name, workers, parsers in (('serial', 1, 1),
                                      ('parallel', settings.download_workers, settings.parser_workers)):
            folder = Path(temp)/name
            config = settings.model_copy(update={'data_dir': folder, 'download_workers': workers, 'parser_workers': parsers})
            store = Store(folder/'gdelt.db'); store.initialize()
            store.enqueue(end-slots*SLOT, end)
            ingest = Ingestor(store, config)
            try:
                result = ingest.run_once(schedule=False, limit=slots*2)
                if result['processed'] != slots*2 or result['failed']:
                    raise RuntimeError(f'{name}有批次失败，无法有效对照：{store.stats()["errors"]}')
                result.update(download_workers=workers, parser_workers=parsers,
                              raw_files_remaining=len(list(ingest.temp_dir.glob('*.zip'))))
                results[name] = result
                totals.append(_aggregates(store))
            finally:
                ingest.close()
    equivalent = _equivalent(*totals)
    if not equivalent:
        raise RuntimeError('串行与并行的聚合结果不一致')
    return {'ok': True, 'slots': slots, 'results': results, 'aggregates_equal': equivalent,
            'speedup': round(results['serial']['seconds']/max(results['parallel']['seconds'], .001), 2),
            'note': '临时数据库与原始文件退出后清理；含网络和解析进程首次启动，少量样本不代表长期速度。'}
