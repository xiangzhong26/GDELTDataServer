from concurrent.futures import ThreadPoolExecutor
import threading
import time

import httpx
import pytest

from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor, _parse_file
from gdelt_server.parser import parse_events, parse_gkg
from gdelt_server.store import SLOT
from conftest import event_row, gkg_row, zipped


def run_mock(store, tmp_path, recent_ts, workers, parsers, handler):
    store.enqueue(recent_ts, recent_ts + 4 * SLOT)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1,
                                    download_workers=workers, parser_workers=parsers), client)
    try:
        result = ingest.run_once(schedule=False, limit=8)
        assert result['processed'] == 8 and result['failed'] == 0
        assert not list(ingest.temp_dir.glob('*.zip'))
        return result
    finally:
        ingest.close()
        client.close()


def aggregate_dump(store):
    with store.connect() as db:
        return {table: sorted(tuple(row) for row in db.execute(f'SELECT * FROM {table}'))
                for table in ('agg_relation', 'agg_geo', 'agg_gkg')}


def test_parallel_download_is_bounded_and_overlaps(store, tmp_path, recent_ts):
    lock = threading.Lock()
    active = peak = 0
    gate = threading.Barrier(4)
    def handler(request):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            gate.wait(timeout=10)  # Serial execution cannot pass this test.
            return httpx.Response(200, content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()]))
        finally:
            with lock:
                active -= 1
    run_mock(store, tmp_path, recent_ts, 4, 1, handler)
    assert peak == 4


def test_process_parsing_matches_serial_and_duplicate_run_is_noop(tmp_path, recent_ts):
    from gdelt_server.store import Store
    outputs = []
    for workers, parsers in ((1, 1), (4, 2)):
        folder = tmp_path / f'{workers}-{parsers}'
        store = Store(folder / 'gdelt.db'); store.initialize()
        def handler(request):
            rows = [gkg_row(str(i)) for i in range(20)] if '.gkg.' in str(request.url) else [event_row(str(i)) for i in range(20)]
            return httpx.Response(200, content=zipped(rows))
        run_mock(store, folder, recent_ts, workers, parsers, handler)
        outputs.append(aggregate_dump(store))
        assert store.enqueue(recent_ts, recent_ts + 4 * SLOT) == 0
        assert store.pending(8) == []
    assert outputs[0] == outputs[1]


def test_parallel_pause_drains_inflight_and_preserves_queue(store, tmp_path, recent_ts):
    store.enqueue(recent_ts, recent_ts + 4 * SLOT)
    entered = threading.Barrier(5)
    release = threading.Event()
    def handler(request):
        entered.wait(timeout=10)
        assert release.wait(10)
        return httpx.Response(200, content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()]))
    client = httpx.Client(transport=httpx.MockTransport(handler))
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1, download_workers=4, parser_workers=2), client)
    try:
        with ThreadPoolExecutor(1) as runner:
            task = runner.submit(ingest.run_once, schedule=False, limit=8)
            entered.wait(timeout=10)
            assert len(ingest.concurrency_status()['active_files']) == 4
            ingest.cancel.set(); release.set()
            result = task.result(timeout=15)
        assert result['processed'] == result['failed'] == 0
        assert len(store.pending(8)) == 8
        assert not ingest.concurrency_status()['active_files']
        assert not list(ingest.temp_dir.glob('*.zip'))
        client.close()
        ingest.client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200,
            content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()]))))
        assert ingest.run_once(schedule=False, limit=8)['processed'] == 8
        assert store.stats()['ledger'] == {'done': 8}
    finally:
        release.set()
        ingest.close(); ingest.client.close()


@pytest.mark.parametrize('parser,row', [(parse_events, event_row), (parse_gkg, gkg_row)])
def test_parser_cancellation_is_cooperative(tmp_path, recent_ts, parser, row):
    path = tmp_path / 'file.zip'; path.write_bytes(zipped([row()]))
    cancel = threading.Event(); cancel.set()
    with pytest.raises(InterruptedError):
        parser(path, recent_ts, cancel=cancel)


@pytest.mark.parametrize('config', [{'download_workers': 0}, {'download_workers': 65}, {'parser_workers': 0}, {'parser_workers': 17}])
def test_concurrency_limits(config):
    with pytest.raises(ValueError):
        Settings(**config)


def test_child_parser_cancel_event_and_resume(store, tmp_path, recent_ts):
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1))
    path = tmp_path/'sample.zip'; path.write_bytes(zipped([gkg_row()]))
    try:
        ingest.ensure_parser_pool()
        ingest.parser_cancel.set()
        future = ingest.parser_pool.submit(_parse_file, 'gkg', path, recent_ts, 1024)
        with pytest.raises(InterruptedError):
            future.result(timeout=15)
        ingest.parser_cancel.clear()
        assert ingest.parser_pool.submit(_parse_file, 'gkg', path, recent_ts, 1024).result(timeout=15).rows == 1
    finally:
        ingest.close()


def test_parallel_failure_isolated_and_retry_does_not_duplicate(store, tmp_path, recent_ts):
    store.enqueue(recent_ts, recent_ts + 4*SLOT)
    failure_lock = threading.Lock()
    fail_once = True
    def handler(request):
        nonlocal fail_once
        with failure_lock:
            if '.gkg.' in str(request.url) and fail_once:
                fail_once = False
                return httpx.Response(503)
        return httpx.Response(200, content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()]))
    client = httpx.Client(transport=httpx.MockTransport(handler))
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1), client)
    try:
        result = ingest.run_once(schedule=False, limit=8)
        assert result['processed'] == 7 and result['failed'] == 1
        store.retry_failed()
        assert ingest.run_once(schedule=False)['processed'] == 1
        assert store.stats()['ledger'] == {'done': 8}
        with store.connect() as db:
            assert db.execute("SELECT SUM(n_events) FROM agg_geo WHERE granularity='day'").fetchone()[0] == 4
            assert db.execute("SELECT SUM(total_docs) FROM agg_gkg WHERE granularity='day'").fetchone()[0] == 4
        assert not list(ingest.temp_dir.glob('*.zip'))
    finally:
        ingest.close(); client.close()


def test_cancelled_network_error_keeps_batch_pending(store, tmp_path, recent_ts):
    store.enqueue(recent_ts, recent_ts+SLOT)
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1, parser_workers=1))
    def handler(request):
        ingest.cancel.set()
        raise httpx.ReadTimeout('cancelled during network wait')
    ingest.client.close()
    ingest.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        result = ingest.run_once(schedule=False)
        assert result['processed'] == result['failed'] == 0
        assert store.stats()['ledger'] == {'pending': 2}
        assert not list(ingest.temp_dir.glob('*.zip'))
    finally:
        ingest.close()


def test_benchmark_compares_country_dimensions_not_just_global_sum():
    from gdelt_server.benchmark import _equivalent
    assert not _equivalent({'geo': {(1, 'US'): (10,)}}, {'geo': {(1, 'FR'): (10,)}})
    assert _equivalent({'geo': {(1, 'US'): (10,)}}, {'geo': {(1, 'US'): (10.00000000001,)}})


def test_database_commits_are_serialized(store, tmp_path, recent_ts, monkeypatch):
    lock = threading.Lock()
    active = peak = 0
    apply = store.apply
    def observed_apply(*args, **kwargs):
        nonlocal active, peak
        with lock:
            active += 1; peak = max(peak, active)
        try:
            time.sleep(.02)
            return apply(*args, **kwargs)
        finally:
            with lock:
                active -= 1
    monkeypatch.setattr(store, 'apply', observed_apply)
    run_mock(store, tmp_path, recent_ts, 4, 1, lambda request: httpx.Response(200,
        content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()])))
    assert peak == 1
    assert store.get_state('snapshot_dirty') is True


def test_crashed_parser_pool_recreated_on_retry(store, tmp_path, recent_ts):
    import os
    from concurrent.futures.process import BrokenProcessPool
    store.enqueue(recent_ts, recent_ts+SLOT)
    client = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200,
        content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()]))))
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1), client)
    try:
        ingest.ensure_parser_pool()
        with pytest.raises(BrokenProcessPool):
            ingest.parser_pool.submit(os._exit, 7).result(timeout=15)
        result = ingest.run_once(schedule=False)
        assert result['processed'] == 0 and result['failed'] == 2
        assert ingest.parser_pool is None
        store.retry_failed()
        assert ingest.run_once(schedule=False)['processed'] == 2
        assert not list(ingest.temp_dir.glob('*.zip'))
    finally:
        ingest.close(); client.close()


def test_sixteen_download_tasks_and_stage_timings(store, tmp_path, recent_ts):
    gate = threading.Barrier(16)
    store.enqueue(recent_ts, recent_ts+8*SLOT)
    def handler(request):
        gate.wait(timeout=15)
        return httpx.Response(200, content=zipped([gkg_row()] if '.gkg.' in str(request.url) else [event_row()]))
    client = httpx.Client(transport=httpx.MockTransport(handler))
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1), client)
    try:
        assert ingest.settings.download_workers == 16 and ingest.settings.parser_workers == 4
        result = ingest.run_once(schedule=False, limit=16)
        assert result['processed'] == 16 and result['failed'] == 0
        for stage in ('download', 'parse_compute', 'parse_pipeline', 'commit'):
            # Published timings round to milliseconds; a fast mock stage can round to zero.
            assert result['stage_seconds'][stage] >= 0
        assert result['stage_seconds']['parse_pipeline'] > 0
        assert result['stage_seconds']['parse_queue_transfer'] >= 0
        assert result['scheduling_seconds'] >= 0
        assert not list(ingest.temp_dir.glob('*.zip'))
    finally:
        ingest.close(); client.close()


def test_old_configuration_uses_new_defaults_but_explicit_limits_are_respected(tmp_path):
    import json
    config = tmp_path/'config.json'
    config.write_text(json.dumps({'data_dir': './data'}))
    assert Settings.load(config).download_workers == 16
    config.write_text(json.dumps({'download_workers': 4, 'parser_workers': 2}))
    assert Settings.load(config).download_workers == 4
    assert Settings.load(config).parser_workers == 2
