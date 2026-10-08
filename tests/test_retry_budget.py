import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from gdelt_server.app import create_app
from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor
from gdelt_server.metrics import Metrics
from gdelt_server.service import Service
from gdelt_server.store import Store, SLOT, utcnow


def wait_until(predicate):
    deadline = time.monotonic()+8
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(.01)


def exhaust(store, kind, ts):
    for _ in range(5):
        store.mark_failed(kind, ts, RuntimeError('network timeout'))


def test_five_failures_are_dormant_until_explicit_new_round(store, recent_ts):
    for attempt in range(1, 6):
        store.mark_failed('events', recent_ts, RuntimeError('timeout'))
        row = store.stats()['errors'][0]
        assert row['attempts'] == row['retry_attempts'] == attempt
        assert row['status'] == ('exhausted' if attempt == 5 else 'failed')
    assert not store.pending(10, now=10**10)
    assert not store.has_unfinished()
    assert store.retry_failed() == 0
    store.enqueue(recent_ts, recent_ts+SLOT)
    assert not any(r['kind']=='events' for r in store.pending(10, now=10**10))
    restarted = Store(store.path); restarted.initialize()
    assert restarted.stats()['ledger']['exhausted'] == 1
    assert restarted.reopen_failed() == 1
    assert restarted.pending(10, failed_only=True)[0]['retry_attempts'] == 0
    exhaust(restarted, 'events', recent_ts)
    row = restarted.stats()['errors'][0]
    assert row['attempts'] == 10 and row['retry_attempts'] == 5
    assert row['status'] == 'exhausted'


def test_legacy_database_migration_preserves_success_and_failure_history(tmp_path):
    path = tmp_path/'old.db'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE ingest_file(kind TEXT,file_ts INTEGER,status TEXT,attempts INTEGER DEFAULT 0,next_retry INTEGER DEFAULT 0,row_count INTEGER DEFAULT 0,skipped_rows INTEGER DEFAULT 0,error TEXT,done_at INTEGER,PRIMARY KEY(kind,file_ts))')
        db.executemany('INSERT INTO ingest_file(kind,file_ts,status,attempts,error) VALUES (?,?,?,?,?)',
                       [('events',0,'failed',12,'bad'),('gkg',0,'failed',2,'timeout'),('events',900,'done',0,None)])
    store = Store(path); store.initialize()
    assert store.stats()['ledger'] == {'done':1,'failed':1,'exhausted':1}
    rows = {r['kind']:r for r in store.stats()['errors']}
    assert rows['events']['attempts'] == 12 and rows['events']['retry_attempts'] == 5
    assert rows['gkg']['retry_attempts'] == 2
    store.reopen_failed(); store.mark_failed('events',0,RuntimeError('still missing'))
    store.initialize()  # Restart does not derive the current round from lifetime failures again.
    assert next(r for r in store.stats()['errors'] if r['kind']=='events')['retry_attempts'] == 1


def test_repair_success_does_not_duplicate_aggregates_and_gap_stays_missing(store, recent_ts):
    ts = recent_ts//3600*3600
    exhaust(store, 'events', ts)
    before = Metrics(store).store.coverage_buckets('events', ts, ts+3600, 3600)[ts]
    assert before['done'] == 0 and not before['complete']
    store.reopen_failed()
    tables = {'agg_geo':{('hour',ts,'US','19'):{'n_events':10}}}
    assert store.apply('events', ts, tables, 10)
    assert not store.apply('events', ts, tables, 10)
    assert store.reopen_failed() == 0
    with store.connect() as db:
        assert db.execute("SELECT n_events FROM agg_geo WHERE granularity='day'").fetchone()[0] == 10


def test_backfill_finishes_with_reported_gaps_not_fake_success(tmp_path, monkeypatch):
    settings = Settings(data_dir=tmp_path, min_free_gb=.1)
    svc = Service(settings)
    horizon = int(utcnow().timestamp())//SLOT*SLOT
    fail_ts = horizon-SLOT
    calls = []
    def process(self, kind, ts):
        if kind == 'events' and ts == fail_ts:
            calls.append(ts)
            raise RuntimeError('permanent missing file')
        return self.store.apply(kind, ts, {}, 1)
    original = svc.store.mark_failed
    def fail_without_wait(kind, ts, error):
        original(kind, ts, error)
        svc.store.retry_failed()  # Expedite the test; budget must still cap attempts.
    monkeypatch.setattr(svc.store, 'mark_failed', fail_without_wait)
    monkeypatch.setattr(Ingestor, 'process', process)
    svc.request('backfill', hours=1); svc.start()
    try:
        wait_until(lambda: svc.job is None)
        assert len(calls) == 5
        p = svc.status()['progress']
        assert p['work_finished'] and p['done'] == 7 and p['exhausted'] == 1
        assert p['percent'] == 87.5  # Success coverage is not fabricated as 100%.
        assert svc.request('sync')['accepted']
        wait_until(lambda: svc.job is None)
        assert len(calls) == 5
    finally:
        svc.close()


def test_repair_only_failed_files_leaves_paused_history_pending(tmp_path, monkeypatch):
    svc = Service(Settings(data_dir=tmp_path, min_free_gb=.1))
    now = int(utcnow().timestamp())//SLOT*SLOT
    svc.store.enqueue(now-2*SLOT, now)
    exhaust(svc.store, 'events', now-SLOT)
    svc.paused_backfill = {'action':'backfill','start_ts':now-2*SLOT,'end_ts':now,'paused':True}
    monkeypatch.setattr(Ingestor,'process',lambda self,k,t:self.store.apply(k,t,{},1))
    assert svc.request('repair')['accepted']
    assert not svc.request('repair')['accepted']  # Double-click cannot renew attempts in flight.
    svc.start()
    try:
        wait_until(lambda: svc.job is None)
        assert svc.store.stats()['ledger'] == {'done':1,'pending':3}
        assert svc.paused_backfill and not svc.enabled
        assert svc.last_job['reopened_files'] == 1
    finally:
        svc.close()


def test_repair_reset_is_deferred_and_persisted_atomically_for_restart(tmp_path):
    settings = Settings(data_dir=tmp_path)
    svc = Service(settings)
    ts = int(utcnow().timestamp())//SLOT*SLOT-SLOT
    exhaust(svc.store,'events',ts)
    svc.ingestor.busy.acquire()
    try:
        assert svc.request('repair')['accepted']
        assert svc.store.stats()['errors'][0]['status'] == 'exhausted'
    finally:
        svc.ingestor.busy.release()
    svc.store.reopen_failed(svc.job)
    svc.store.mark_failed('events',ts,RuntimeError('temporary'))
    svc.ingestor.close()
    restarted = Service(settings)
    try:
        assert restarted.job['seeded']
        assert restarted.store.stats()['errors'][0]['retry_attempts'] == 1
        restarted.pause_all()
        assert restarted.store.get_state('active_repair') is None
        assert restarted.store.stats()['errors'][0]['retry_attempts'] == 1
    finally:
        restarted.ingestor.close()


def test_repair_endpoint_authorization_and_config_limit(tmp_path):
    token = 'management-'+'x'*32
    with TestClient(create_app(Settings(data_dir=tmp_path,api_token=token))) as client:
        assert client.post('/api/admin/repair').status_code == 401
        assert client.post('/api/admin/repair',headers={'Authorization':'Bearer '+token}).json()['accepted']
    for value in (0,21,True):
        with pytest.raises(ValueError): Settings(max_file_attempts=value)


def test_monitor_keeps_processing_new_files_while_repair_waits(tmp_path, monkeypatch):
    svc = Service(Settings(data_dir=tmp_path, min_free_gb=.1))
    now = int(utcnow().timestamp())//SLOT*SLOT
    old = now-10*SLOT
    exhaust(svc.store,'events',old)
    svc.store.enqueue(now-SLOT,now)
    svc.store.set_state('scheduled_until',now)
    def process(self,kind,ts):
        if ts == old: raise RuntimeError('temporary')
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor,'process',process)
    svc.monitor(True); svc.request('repair'); svc.start()
    try:
        wait_until(lambda:svc.store.stats()['ledger'].get('done')==2)
        assert svc.job['action'] == 'repair'
        assert svc.store.stats()['errors'][0]['retry_attempts'] == 1
        svc.pause_all()
        wait_until(lambda:svc.job is None)
        assert svc.store.stats()['errors'][0]['retry_attempts'] == 1
    finally:
        svc.close()


def test_pause_before_repair_starts_never_reopens_budget(tmp_path):
    svc = Service(Settings(data_dir=tmp_path,min_free_gb=.1))
    now = int(utcnow().timestamp())//SLOT*SLOT-SLOT
    exhaust(svc.store,'events',now)
    svc.request('repair'); svc.pause_all(); svc.start()
    try:
        wait_until(lambda:svc.job is None)
        assert svc.store.stats()['errors'][0]['status'] == 'exhausted'
        assert svc.store.get_state('active_repair') is None
    finally:
        svc.close()
