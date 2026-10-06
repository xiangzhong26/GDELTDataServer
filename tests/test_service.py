import time
from pathlib import Path
from fastapi.testclient import TestClient
import pytest

from gdelt_server.app import create_app
from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor
from gdelt_server.store import SLOT,utcnow
from gdelt_server.parser import parse_gkg,parse_events
from conftest import event_row,gkg_row,zipped


def wait_until(predicate,timeout=20):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if predicate():return
        time.sleep(.03)
    raise AssertionError('后台任务未按时结束')


def test_manual_backfill_continues_across_batches_and_restart(tmp_path,monkeypatch):
    processed=[]
    def fake_process(self,kind,ts):
        processed.append((kind,ts))
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor,'process',fake_process)
    settings=Settings(data_dir=tmp_path,initial_hours=1,batch_files=3,poll_seconds=60,snapshot_days=[7],min_free_gb=.1)
    app=create_app(settings)
    with TestClient(app) as client:
        assert client.post('/api/admin/backfill',json={'hours':1}).json()['accepted']
        wait_until(lambda:client.get('/api/gdelt/status').json()['job'] is None)
        status=client.get('/api/gdelt/status').json()
        assert status['storage']['ledger']['done']==8
        assert len(processed)==8
        assert status['snapshot'] is not None
        assert not status['monitor_enabled']
    with TestClient(create_app(settings)) as client:
        assert not client.get('/api/gdelt/status').json()['monitor_enabled']
        assert client.post('/api/admin/backfill',json={'hours':1}).json()['accepted']
        wait_until(lambda:client.get('/api/gdelt/status').json()['job'] is None)
        assert len(processed)==8


def test_monitor_state_and_stopping_without_resetting_data(tmp_path,monkeypatch):
    monkeypatch.setattr(Ingestor,'process',lambda self,k,t:self.store.apply(k,t,{},1))
    settings=Settings(data_dir=tmp_path,initial_hours=1,batch_files=8,poll_seconds=60,snapshot_days=[7],min_free_gb=.1)
    with TestClient(create_app(settings)) as client:
        assert client.post('/api/admin/monitor',json={'enabled':True}).status_code==200
        wait_until(lambda:client.get('/api/gdelt/status').json()['storage']['ledger'].get('done',0)==8)
        client.post('/api/admin/monitor',json={'enabled':False})
        assert client.get('/api/gdelt/status').json()['storage']['ledger']['done']==8
    with TestClient(create_app(settings)) as client:
        assert not client.get('/api/gdelt/status').json()['monitor_enabled']


def test_invalid_numeric_parser_data_never_publishes(tmp_path):
    ts=int(utcnow().timestamp())//SLOT*SLOT-SLOT
    path=tmp_path/'bad.zip'
    row=event_row(tone='NaN');path.write_bytes(zipped([row]))
    with pytest.raises(ValueError):parse_events(path,ts)
    path.write_bytes(zipped([gkg_row(tone='not-a-number')]))
    with pytest.raises(ValueError):parse_gkg(path,ts)


def test_pending_expiration_keeps_completed_history_ledger(store):
    from gdelt_server.store import DAY,bucket_of
    old=bucket_of(int(utcnow().timestamp()),'day')-731*DAY
    store.enqueue(old,old+SLOT)
    store.apply('events',old,{},1)
    store.prune()
    assert store.stats()['ledger']=={'done':1,'expired':1}
    assert store.pending(20)==[]


def test_date_backfill_exact_utc_start_and_retention_survives_restart(tmp_path, monkeypatch):
    from datetime import date, datetime, timezone
    from gdelt_server.service import Service
    from gdelt_server.store import DAY
    settings = Settings(data_dir=tmp_path, day_retention_days=730, min_free_gb=.1)
    service = Service(settings)
    try:
        assert service.request("backfill", start_date=date(2020, 1, 1))["accepted"]
        assert service.job["start_ts"] == int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
        assert service.settings.day_retention_days == 3650
        assert settings.day_retention_days == 730
        spans = []
        monkeypatch.setattr(service.store, "enqueue", lambda a,b: spans.append((a,b)) or 0)
        service.ingestor.backfill(service.job["hours"], service.job["start_ts"])
        assert spans[0][0] == service.job["start_ts"]
        monkeypatch.undo()
        old = int(utcnow().timestamp()) // DAY * DAY - 1000 * DAY
        service.store.enqueue(old, old+SLOT)
        service.store.apply('events', old, {'agg_geo': {('hour', old, 'US', '01'): {'n_events': 2}}}, 2)
        service.store.prune(service.settings.hour_retention_days, service.settings.day_retention_days)
        assert any(r['file_ts'] == old for r in service.store.pending(10))
        with service.store.connect() as db:
            assert db.execute("SELECT SUM(n_events) FROM agg_geo WHERE granularity='day'").fetchone()[0] == 2
            assert db.execute("SELECT COUNT(*) FROM agg_geo WHERE granularity='hour'").fetchone()[0] == 0
    finally:
        service.ingestor.close()
    restored = Service(settings)
    try:
        assert restored.settings.day_retention_days == 3650
        assert restored.job['start_date'] == '2020-01-01'
    finally:
        restored.ingestor.close()


def test_invalid_date_backfill_does_not_change_retention(tmp_path):
    from datetime import date, timedelta
    from gdelt_server.service import Service
    service = Service(Settings(data_dir=tmp_path))
    try:
        for start in (date(2015, 2, 18), utcnow().date()+timedelta(days=1),
                      utcnow().date()-timedelta(days=3651)):
            with pytest.raises(ValueError):
                service.request('backfill', start_date=start)
        assert service.store.get_state('backfill_retention_days') is None
        assert service.job is None
    finally:
        service.ingestor.close()


def test_backfill_request_validation_and_persisted_job_resume(tmp_path, monkeypatch):
    from gdelt_server.service import Service
    settings = Settings(data_dir=tmp_path, batch_files=8, snapshot_days=[7], min_free_gb=.1)
    service = Service(settings)
    assert service.request('backfill', hours=1)['accepted']
    service.ingestor.close()
    processed = []
    def process(self, kind, ts):
        processed.append((kind,ts))
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor, 'process', process)
    with TestClient(create_app(settings)) as client:
        wait_until(lambda: client.get('/api/gdelt/status').json()['job'] is None)
        assert len(processed) == 8
        assert not client.get('/api/gdelt/status').json()['monitor_enabled']
        for body in ({'hours':1, 'start_date':'2020-01-01'}, {'start_date':'invalid'}):
            assert client.post('/api/admin/backfill',json=body).status_code == 422
    restored = Service(settings)
    try:
        assert restored.job is None
    finally:
        restored.ingestor.close()


def test_shutdown_during_backfill_preserves_job_for_resume(tmp_path, monkeypatch):
    import threading
    from gdelt_server.service import Service
    entered, release = threading.Event(), threading.Event()
    def process(self,kind,ts):
        entered.set()
        assert release.wait(10)
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor,'process',process)
    settings=Settings(data_dir=tmp_path,batch_files=8,snapshot_days=[7],min_free_gb=.1)
    service=Service(settings)
    service.request('backfill',hours=1)
    service.start()
    try:
        assert entered.wait(10)
        service.shutdown.set()
        service.ingestor.cancel.set()
    finally:
        release.set()
        service.close()
    assert service.store.get_state('active_backfill')['seeded']
    with TestClient(create_app(settings)) as client:
        wait_until(lambda:client.get('/api/gdelt/status').json()['job'] is None)
        assert client.get('/api/gdelt/status').json()['storage']['ledger']['done']==8
