from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import threading
import time

import httpx
import pytest

from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor
from gdelt_server.parser import parse_events, parse_gkg
from gdelt_server.service import Service
from gdelt_server.store import SLOT
from conftest import event_row, gkg_row, zipped


def test_live_download_bytes_and_completed_cycle(store, tmp_path, recent_ts):
    entered, release = threading.Event(), threading.Event()
    payload = zipped([event_row()])
    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield payload[:10]
            entered.set()
            assert release.wait(10)
            yield payload[10:]
    client = httpx.Client(transport=httpx.MockTransport(lambda request:
        httpx.Response(200, headers={'Content-Length':str(len(payload))}, stream=Stream())))
    store.enqueue(recent_ts, recent_ts+SLOT)
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1,
        download_workers=1, parser_workers=1), client)
    try:
        with ThreadPoolExecutor(1) as runner:
            future = runner.submit(ingest.run_once, schedule=False, limit=1)
            try:
                assert entered.wait(10)
                live = ingest.concurrency_status()
                row = live['active_files'][0]
                assert row['stage'] == 'download'
                assert row['downloaded_bytes'] == 10 and row['total_bytes'] == len(payload)
                assert row['filename'].endswith('.export.CSV.zip')
                assert row['elapsed_seconds'] >= row['inactive_seconds'] >= 0
                assert live['cycle']['total'] == 1 and live['cycle']['completed'] == 0
            finally:
                release.set()
            assert future.result(timeout=10)['processed'] == 1
        live = ingest.concurrency_status()
        assert live['active_files'] == []
        assert live['cycle']['succeeded'] == live['cycle']['completed'] == 1
        assert live['recent_files'][0]['outcome'] == 'done'
        assert live['seconds_since_completion'] >= 0
    finally:
        release.set()
        ingest.close(); client.close()


def test_trickling_download_has_total_deadline_and_is_retryable(store, tmp_path, recent_ts, monkeypatch):
    import gdelt_server.ingest as module
    offset = 0
    clock = time.monotonic
    monkeypatch.setattr(module, 'time', SimpleNamespace(monotonic=lambda:clock()+offset, time=time.time))
    payload = zipped([event_row()])
    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            nonlocal offset
            yield payload[:10]
            offset += 6  # Data keeps arriving, so a read-inactivity timeout would not fire.
            yield payload[10:]
    client = httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(200, stream=Stream())))
    store.enqueue(recent_ts, recent_ts+SLOT)
    ingest = Ingestor(store, Settings(data_dir=tmp_path, min_free_gb=.1,
        download_workers=1, parser_workers=1, request_timeout=5), client)
    try:
        result = ingest.run_once(schedule=False, limit=1)
        assert result['processed'] == 0 and result['failed'] == 1
        live = ingest.concurrency_status()
        assert live['cycle']['failed'] == 1
        assert '总时限' in live['recent_files'][0]['error']
        assert store.stats()['ledger'] == {'pending':1, 'failed':1}
        assert not list(ingest.temp_dir.glob('*.zip'))
    finally:
        ingest.close(); client.close()


@pytest.mark.parametrize('parser,row', [(parse_events,event_row), (parse_gkg,gkg_row)])
def test_parser_reports_rows_without_changing_aggregates(tmp_path, recent_ts, parser, row):
    path = tmp_path/'sample.zip'
    path.write_bytes(zipped([row(str(i)) for i in range(300)]))
    progress = []
    observed = parser(path, recent_ts, progress=progress.append)
    expected = parser(path, recent_ts)
    assert observed == expected
    assert progress[0] == 1 and progress[-1] == 300


def test_history_progress_is_scoped_and_survives_pause_restart(tmp_path, recent_ts):
    settings = Settings(data_dir=tmp_path, min_free_gb=.1)
    service = Service(settings)
    job = {'action':'backfill', 'hours':1, 'start_ts':recent_ts, 'end_ts':recent_ts+4*SLOT, 'paused':True}
    try:
        service.store.enqueue(recent_ts, recent_ts+4*SLOT)
        service.store.apply('events', recent_ts, {}, 1)
        service.store.apply('events', recent_ts-SLOT, {}, 1)  # Outside this task.
        service.store.mark_failed('gkg', recent_ts, 'network')
        service.paused_backfill = job
        service.store.set_state('paused_backfill', job)
        progress = service.status()['progress']
        assert progress == {'start_ts':recent_ts,'end_ts':recent_ts+4*SLOT,'total':8,'done':1,
            'pending':6,'failed':1,'unseeded':0,'percent':12.5,'paused':True}
    finally:
        service.ingestor.close()
    restarted = Service(settings)
    try:
        assert restarted.status()['progress'] == progress
    finally:
        restarted.ingestor.close()


def test_snapshot_phase_is_visible_and_restored_on_failure(tmp_path, monkeypatch):
    service = Service(Settings(data_dir=tmp_path))
    def fail():
        assert service.status()['phase'] == 'exporting'
        raise RuntimeError('disk unavailable')
    monkeypatch.setattr(service.ingestor, 'check_disk', fail)
    try:
        with pytest.raises(RuntimeError):
            service.export_locked()
        assert service.ingestor.phase == 'idle'
    finally:
        service.ingestor.close()


def test_child_parser_progress_reaches_parent(tmp_path, store, recent_ts):
    from gdelt_server.ingest import _parse_file
    path = tmp_path/'gkg.zip'
    path.write_bytes(zipped([gkg_row(str(i)) for i in range(300)]))
    ingest = Ingestor(store, Settings(data_dir=tmp_path, parser_workers=2))
    now = time.monotonic()
    ingest.active[('gkg',recent_ts)] = {'kind':'gkg','file_ts':recent_ts,'stage':'parse_pipeline',
        'started':now,'stage_started':now,'last_activity':now,'parsed_rows':0}
    try:
        ingest.ensure_parser_pool()
        parsed = ingest.parser_pool.submit(_parse_file, 'gkg', path, recent_ts, 1024).result(timeout=15)
        deadline = time.monotonic()+5
        while time.monotonic() < deadline:
            row = ingest.concurrency_status()['active_files'][0]
            if row['stage'] == 'parse_transfer':
                break
            time.sleep(.01)
        assert row['stage'] == 'parse_transfer' and row['parsed_rows'] == parsed.rows == 300
        # Delayed telemetry must not revert the stage after the parent begins writing.
        ingest.file_progress('gkg', recent_ts, stage='commit')
        ingest.parser_progress.put(('gkg',recent_ts,'parsing',1,time.monotonic()))
        time.sleep(.02)
        assert ingest.concurrency_status()['active_files'][0]['stage'] == 'commit'
    finally:
        ingest.active.clear()
        ingest.close()
