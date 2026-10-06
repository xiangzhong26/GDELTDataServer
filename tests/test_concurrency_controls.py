import threading

import httpx
import pytest
from fastapi.testclient import TestClient

from gdelt_server.app import create_app
from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor
from gdelt_server.service import Service
from gdelt_server.store import SLOT, utcnow
from test_service import wait_until
from conftest import zipped, event_row


def test_web_settings_persist_and_override_local_configuration(tmp_path):
    settings = Settings(data_dir=tmp_path, snapshot_days=[7])
    with TestClient(create_app(settings)) as client:
        assert client.put('/api/admin/concurrency', json={'download_workers':32,'parser_workers':12}).json()['accepted']
        wait_until(lambda:client.app.state.service.pending_concurrency is None)
        assert client.get('/api/gdelt/status').json()['concurrency']['parser_workers'] == 12
        assert not client.app.state.service.enabled
    with TestClient(create_app(settings)) as client:
        effective = client.get('/api/gdelt/status').json()['concurrency']
        assert effective['download_workers'] == 32 and effective['parser_workers'] == 12
        assert not client.app.state.service.enabled


@pytest.mark.parametrize('body', [
    {'download_workers':65,'parser_workers':4}, {'download_workers':1,'parser_workers':17},
    {'download_workers':0,'parser_workers':1}, {'download_workers':16,'parser_workers':0},
    {'download_workers':True,'parser_workers':4}, {'download_workers':4.5,'parser_workers':4},
    {'download_workers':4,'parser_workers':4,'extra':1},
])
def test_invalid_web_limits_are_rejected(tmp_path, body):
    with TestClient(create_app(Settings(data_dir=tmp_path))) as client:
        assert client.put('/api/admin/concurrency', json=body).status_code == 422
        assert client.app.state.service.store.get_state('concurrency_settings') is None


def test_control_endpoints_require_auth(tmp_path):
    with TestClient(create_app(Settings(data_dir=tmp_path,api_token='t'*32))) as client:
        assert client.put('/api/admin/concurrency',json={'download_workers':4,'parser_workers':2}).status_code == 401
        assert client.post('/api/admin/pause').status_code == 401


def test_running_change_drains_old_tasks_and_keeps_backfill(tmp_path, monkeypatch):
    entered = threading.Barrier(3)
    release = threading.Event()
    seen = []
    def process(self, kind, ts):
        workers = self.settings.download_workers
        if workers == 2:
            entered.wait(timeout=10)
            assert release.wait(10)
        seen.append((kind,ts,workers))
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor,'process',process)
    settings=Settings(data_dir=tmp_path,download_workers=2,parser_workers=1,snapshot_days=[7],min_free_gb=.1)
    with TestClient(create_app(settings)) as client:
        client.post('/api/admin/backfill',json={'hours':1})
        try:
            entered.wait(timeout=10)
            assert client.put('/api/admin/concurrency',json={'download_workers':8,'parser_workers':4}).json()['accepted']
            assert client.app.state.service.job is not None
        finally:
            release.set()
        wait_until(lambda:client.app.state.service.job is None)
        assert [r[2] for r in seen].count(2) == 2
        assert [r[2] for r in seen].count(8) == 6
        assert len({r[:2] for r in seen}) == 8
        assert client.app.state.service.store.stats()['ledger'] == {'done':8}


def test_pause_all_during_configuration_then_resume(tmp_path, monkeypatch):
    entered = threading.Barrier(3)
    release = threading.Event()
    def process(self, kind, ts):
        if not release.is_set():
            entered.wait(timeout=10)
            assert release.wait(10)
        if self.cancel.is_set():
            raise InterruptedError('paused')
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor,'process',process)
    settings=Settings(data_dir=tmp_path,download_workers=2,parser_workers=1,snapshot_days=[7],min_free_gb=.1)
    with TestClient(create_app(settings)) as client:
        client.post('/api/admin/backfill',json={'hours':1})
        try:
            entered.wait(timeout=10)
            client.post('/api/admin/monitor',json={'enabled':True})
            client.put('/api/admin/concurrency',json={'download_workers':8,'parser_workers':8})
            assert client.post('/api/admin/pause').json()['accepted']
            assert client.app.state.service.ingestor.cancel.is_set()
        finally:
            release.set()
        service=client.app.state.service
        wait_until(lambda:service.job is None and service.pending_concurrency is None)
        assert not service.enabled and service.paused_backfill is not None
        assert service.settings.parser_workers == 8
        assert service.store.stats()['ledger'].get('done',0) == 0
        assert client.post('/api/admin/backfill/resume').json()['accepted']
        wait_until(lambda:service.job is None)
        assert service.store.stats()['ledger'] == {'done':8}


def test_newer_setting_wins_during_pool_change_and_pause_stays_available(tmp_path, monkeypatch):
    service=Service(Settings(data_dir=tmp_path))
    original=service.ingestor.set_concurrency
    def change(**desired):
        service.configure_concurrency(2,2)
        service.pause_all()  # Would deadlock if changing a pool held the control lock.
        original(**desired)
    monkeypatch.setattr(service.ingestor,'set_concurrency',change)
    try:
        service.configure_concurrency(32,8)
        service.apply_pending_concurrency()
        assert service.pending_concurrency == {'download_workers':2,'parser_workers':2}
        assert service.ingestor.reconfigure.is_set() and service.ingestor.cancel.is_set()
        monkeypatch.setattr(service.ingestor,'set_concurrency',original)
        service.apply_pending_concurrency()
        assert service.settings.download_workers == 2 and service.settings.parser_workers == 2
        assert service.pending_concurrency is None and service.ingestor.cancel.is_set()
    finally:
        service.ingestor.close()


def test_batch_size_does_not_cap_web_concurrency(store,tmp_path,recent_ts):
    ingest=Ingestor(store,Settings(data_dir=tmp_path,batch_files=1,download_workers=4,parser_workers=1,min_free_gb=.1))
    store.enqueue(recent_ts,recent_ts+4*SLOT)
    ingest.client.close()
    def handler(request):
        assert request.extensions['timeout']['read'] == 5
        assert request.extensions['timeout']['connect'] == 5
        return httpx.Response(200,content=zipped([event_row()]))
    ingest.client=httpx.Client(transport=httpx.MockTransport(handler))
    try:
        assert ingest.run_once(schedule=False)['queued'] == 4
    finally:
        ingest.close()
