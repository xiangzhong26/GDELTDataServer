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
