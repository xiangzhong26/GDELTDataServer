from datetime import datetime, timezone

from gdelt_server.config import Settings
from gdelt_server.metrics import Metrics
from gdelt_server.service import Service
from gdelt_server.snapshot import build_snapshot
from gdelt_server.store import DAY, SLOT, bucket_of, utcnow


def test_short_window_momentum_uses_whole_day_counts(store, monkeypatch):
    now = datetime(2026, 10, 6, 12, tzinfo=timezone.utc)
    monkeypatch.setattr('gdelt_server.metrics.utcnow', lambda: now)
    today = int(now.timestamp()) // DAY * DAY
    first = today - 7 * DAY
    # Each fully collected day has 240 events: 120 before and after noon.
    # A noon rolling window clips the first half-day, but momentum must remain neutral.
    for day in range(first, today, DAY):
        for offset in (0, 14 * 3600):
            ts = day + offset
            store.apply('events', ts, {'agg_geo': {
                ('hour', ts, 'US', '19'): {'n_events': 120, 'sum_sources': 240, 'sum_w': 120}
            }}, 120)
    with store.connect(write=True) as db:
        db.executemany("INSERT OR IGNORE INTO ingest_file(kind,file_ts,status) VALUES ('events',?,'done')",
                       [(ts,) for ts in range(first, today, SLOT)])
    selected = Metrics(store).country_risk(7)['selected']
    assert selected['momentum_available']
    assert selected['momentum'] == 50


def test_pruned_daily_history_can_be_reimported_without_double_count(store):
    today = bucket_of(int(utcnow().timestamp()), 'day')
    ts = today - 731 * DAY
    tables = {'agg_geo': {('hour', ts, 'US', '19'): {'n_events': 10}}}
    store.apply('events', ts, tables, 10)
    version = store.get_state('data_version')
    store.prune(60, 730)
    assert store.get_state('data_version') > version
    assert store.enqueue(ts, ts + SLOT) == 2
    assert store.apply('events', ts, tables, 10)
    assert not store.apply('events', ts, tables, 10)
    with store.connect() as db:
        assert db.execute("SELECT n_events FROM agg_geo WHERE granularity='day'").fetchone()[0] == 10


def test_retry_seeds_previously_unqueued_holes(tmp_path):
    service = Service(Settings(data_dir=tmp_path))
    now = int(utcnow().timestamp()) // SLOT * SLOT
    try:
        service.store.apply('events', now - 4 * SLOT, {}, 1)
        service.store.apply('gkg', now - SLOT, {}, 1)
        service.request('retry')
        service.ingestor.repair_gaps()
        queued = {(r['kind'], r['file_ts']) for r in service.store.pending(100)}
        assert ('events', now - 2 * SLOT) in queued
        assert ('gkg', now - 4 * SLOT) in queued
        assert ('events', now - 4 * SLOT) not in queued
    finally:
        service.ingestor.close()


def test_snapshot_keeps_one_database_version_during_concurrent_write(store, monkeypatch):
    original = Metrics.overview
    before = store.get_state('data_version', 0)
    def write_after_first_read(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        store.apply('events', int(utcnow().timestamp()) // SLOT * SLOT - SLOT, {}, 1)
        return result
    monkeypatch.setattr(Metrics, 'overview', write_after_first_read)
    snapshot = build_snapshot(store, [7])
    assert store.get_state('data_version') == before + 1
    assert snapshot['data_version'] == before
    assert {r['common']['data_version'] for r in snapshot['views']['7'].values()} == {before}


def test_monitor_retries_old_failures_outside_initial_window(tmp_path):
    service = Service(Settings(data_dir=tmp_path, initial_hours=1))
    now = int(utcnow().timestamp()) // SLOT * SLOT
    try:
        service.store.mark_failed('events', now - 10 * DAY, RuntimeError('timeout'))
        service.store.retry_failed()
        service.monitor(True)
        batch = service.store.pending(8, **service.work_options())
        assert any(r['file_ts'] == now - 10 * DAY for r in batch)
        service.request('backfill', hours=1)
        batch = service.store.pending(8, **service.work_options(service.job))
        assert any(r['file_ts'] == now - 10 * DAY for r in batch)
    finally:
        service.ingestor.close()


def test_snapshot_uses_one_clock_across_midnight(store, monkeypatch):
    before = datetime(2026, 10, 5, 23, 59, 59, tzinfo=timezone.utc)
    after = datetime(2026, 10, 6, 0, 0, 1, tzinfo=timezone.utc)
    monkeypatch.setattr('gdelt_server.store.utcnow', lambda: before)
    monkeypatch.setattr('gdelt_server.metrics.utcnow', lambda: after)
    monkeypatch.setattr('gdelt_server.snapshot.utcnow', lambda: after)
    snapshot = build_snapshot(store, [30])
    assert snapshot['created_at'] == before.isoformat()
    views = snapshot['views']['30'].values()
    assert {v['common']['window']['end'] for v in views} == {before.isoformat(timespec='seconds')}
