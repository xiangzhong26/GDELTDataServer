from datetime import timedelta
from unittest.mock import patch
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from gdelt_server.app import create_app
from gdelt_server.config import Settings
from gdelt_server.diagnostics import diagnose
from gdelt_server.ingest import Ingestor
from gdelt_server.metrics import Metrics
from gdelt_server.query import QueryCache
from gdelt_server.service import Service
from gdelt_server.store import SLOT, utcnow
from test_metrics_snapshot import populate
from test_service import wait_until


@pytest.mark.parametrize('name,country', [('country-risk','US'),('enterprise-risk','US'),('attitude','USA')])
def test_cached_country_results_match_calculation(store,tmp_path,recent_ts,name,country):
    populate(store,tmp_path,recent_ts)
    cache=QueryCache(store)
    expected=getattr(Metrics(store),name.replace('-','_'))(30,country)
    actual=cache.resolve(name,30,country)
    for field in ('countries','selected','series','coverage'):
        assert actual[field]==expected[field]
    if name=='attitude': assert actual['event_types']==expected['event_types']
    with patch.object(Metrics,name.replace('-','_'),side_effect=AssertionError('country switch must reuse grouped results')):
        assert cache.resolve(name,30,'FR' if name!='attitude' else 'FRA')['selected'] == next((r for r in actual['countries'] if r['code'] in ('FR','FRA')),None)


def test_overview_country_selection_changes_trend(store,tmp_path,recent_ts):
    populate(store,tmp_path,recent_ts)
    cache=QueryCache(store)
    total=cache.resolve('overview',30,'')
    usa=cache.resolve('overview',30,'USA')
    absent=cache.resolve('overview',30,'RUS')
    assert total['summary']['event_count']==usa['summary']['event_count']
    assert usa['selected']['code']=='USA'
    assert absent['selected'] is None and absent['series']==[]
    assert usa['series']==Metrics(store).attitude(30,'USA')['series']


def test_cache_expiry_refresh_parameter_change_and_bound(store,tmp_path,recent_ts):
    populate(store,tmp_path,recent_ts)
    cache=QueryCache(store,capacity=2)
    with patch.object(Metrics,'attitude',wraps=Metrics(store).attitude) as compute:
        cache.resolve('attitude',30,'USA')
        cache.resolve('attitude',30,'FRA')
        assert compute.call_count==1
        cache.resolve('attitude',30,'USA',fresh=True)
        assert compute.call_count==2
        store.set_state('parameter_version',1)
        cache.resolve('attitude',30,'USA')
        assert compute.call_count==3
        cache.ttl=0
        cache.resolve('attitude',30,'USA')
        assert compute.call_count==4
    assert len(cache.cache)<=2


def test_manual_retry_and_backfill_wait_for_delayed_failure(tmp_path,monkeypatch):
    settings=Settings(data_dir=tmp_path,batch_files=8,snapshot_days=[7],min_free_gb=.1)
    service=Service(settings)
    ts=int(utcnow().timestamp())//SLOT*SLOT-SLOT
    service.store.mark_failed('events',ts,RuntimeError('temporary timeout'))
    attempts=service.store.stats()['errors'][0]['attempts']
    assert not service.store.pending(1)
    assert service.store.has_unfinished()
    service.request('retry')
    assert service.store.pending(1)[0]['attempts']==attempts
    monkeypatch.setattr(Ingestor,'process',lambda self,k,t:self.store.apply(k,t,{},1))
    service.start()
    try:
        wait_until(lambda:service.job is None)
        assert service.store.stats()['ledger']=={'done':2}  # Missing companion is repaired too.
        assert not service.enabled
    finally:
        service.close()


def test_backfill_keeps_job_while_retry_is_not_due(tmp_path,monkeypatch):
    settings=Settings(data_dir=tmp_path,batch_files=8,snapshot_days=[7],min_free_gb=.1)
    entered=[]
    def process(self,k,t):
        if not entered:
            entered.append((k,t));raise RuntimeError('temporary timeout')
        return self.store.apply(k,t,{},1)
    monkeypatch.setattr(Ingestor,'process',process)
    with TestClient(create_app(settings)) as client:
        client.post('/api/admin/backfill',json={'hours':1})
        svc=client.app.state.service
        wait_until(lambda:svc.store.stats()['ledger'].get('done')==7)
        wait_until(lambda:not svc.ingestor.busy.locked())
        assert svc.job is not None
        assert client.post('/api/admin/retry').json()['accepted']
        wait_until(lambda:svc.job is None)
        assert svc.store.stats()['ledger']=={'done':8}


def test_sqlite_failure_returns_actionable_503(tmp_path,monkeypatch):
    with TestClient(create_app(Settings(data_dir=tmp_path,min_free_gb=.1))) as client:
        monkeypatch.setattr(client.app.state.service.queries,'resolve',lambda *a,**k: (_ for _ in ()).throw(sqlite3.OperationalError('disk I/O error')))
        response=client.get('/api/gdelt/country-risk')
        assert response.status_code==503
        assert '临时目录' in response.json()['detail']


def test_recording_disk_error_cannot_kill_worker(tmp_path,monkeypatch):
    service=Service(Settings(data_dir=tmp_path))
    try:
        monkeypatch.setattr(service.store,'set_state',lambda *a: (_ for _ in ()).throw(sqlite3.OperationalError('disk I/O error')))
        service.record_error(sqlite3.OperationalError('disk I/O error'))
        assert service.runtime_error=='disk I/O error'
        assert service.next_delay()==10
    finally:service.ingestor.close()


def test_doctor_checks_existing_db_without_creating_missing_db(store,tmp_path):
    settings=Settings(data_dir=tmp_path)
    assert diagnose(settings)['ok']
    missing=tmp_path/'absent'
    result=diagnose(Settings(data_dir=missing))
    assert not result['ok']
    assert not missing.exists()


def test_systemd_has_writable_temporary_directory():
    from pathlib import Path
    unit=(Path(__file__).parents[1]/'deploy/gdelt-data-server.service').read_text()
    assert 'ProtectSystem=strict' in unit and 'PrivateTmp=true' in unit


def test_query_read_snapshot_is_consistent_while_ingest_writes(store):
    store.set_state('data_version', 1)
    with store.read_snapshot() as reader:
        assert reader.get_state('data_version') == 1
        store.set_state('data_version', 2)
        assert reader.get_state('data_version') == 1
        with pytest.raises(RuntimeError):
            reader.set_state('data_version', 3)
    assert store.get_state('data_version') == 2


def test_expanded_retention_requeues_only_fully_pruned_files(store):
    from gdelt_server.store import DAY, bucket_of
    old = bucket_of(int(utcnow().timestamp()), 'day')-731*DAY
    store.enqueue(old, old+SLOT)
    store.apply('events', old, {}, 1)
    store.prune()
    assert store.stats()['ledger'] == {'expired': 2}
    assert store.enqueue(old, old+SLOT) == 2
    assert store.apply('events', old, {}, 1)
    assert not store.apply('events', old, {}, 1)


def test_large_history_queue_does_not_starve_retry_or_recent_files(store):
    from gdelt_server.store import DAY
    now=int(utcnow().timestamp())//SLOT*SLOT
    store.enqueue(now-100*DAY,now-99*DAY)
    store.enqueue(now-SLOT,now)
    old=now-101*DAY
    store.mark_failed('events',old,RuntimeError('timeout'))
    store.retry_failed()
    batch=store.pending(8,now=now)
    assert len(batch)==8
    assert any(r['status']=='failed' for r in batch)
    assert any(r['file_ts']==now-SLOT for r in batch)
    assert any(r['file_ts']<now-DAY and r['status']=='pending' for r in batch)
    assert len({(r['kind'],r['file_ts']) for r in batch})==8
