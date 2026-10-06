from datetime import timedelta
import threading

from fastapi.testclient import TestClient

from gdelt_server.app import create_app
from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor
from gdelt_server.service import Service
from gdelt_server.store import DAY, SLOT, bucket_of, utcnow
from test_service import wait_until


def test_date_backfill_anchors_to_earliest_done_and_runs_backwards(tmp_path):
    service = Service(Settings(data_dir=tmp_path))
    now = int(utcnow().timestamp())//SLOT*SLOT
    earliest = bucket_of(now, 'day')-3*DAY+12*SLOT
    service.store.apply('events', earliest, {}, 1)
    service.store.apply('gkg', earliest+SLOT, {}, 1)
    try:
        target = (utcnow()-timedelta(days=5)).date()
        assert service.request('backfill', start_date=target)['accepted']
        job = service.job
        assert job['end_ts'] == earliest+SLOT
        service.ingestor.backfill(job['hours'], job['start_ts'], job['end_ts'])
        options = service.work_options(job)
        batch = service.store.pending(8, **options)
        assert batch[0]['file_ts'] == earliest
        assert batch[0]['kind'] == 'gkg'  # Boundary's missing companion is not lost.
        assert all(job['start_ts'] <= r['file_ts'] < job['end_ts'] for r in batch)
        assert [r['file_ts'] for r in batch] == sorted([r['file_ts'] for r in batch], reverse=True)
    finally:
        service.ingestor.close()


def test_pause_persists_and_resume_skips_already_committed_batches(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    completed = []
    def process(self, kind, ts):
        if not completed:
            entered.set()
            assert release.wait(10)
            if self.cancel.is_set():
                raise InterruptedError('paused')
        completed.append((kind, ts))
        return self.store.apply(kind, ts, {}, 1)
    monkeypatch.setattr(Ingestor, 'process', process)
    settings = Settings(data_dir=tmp_path, batch_files=8, snapshot_days=[7], min_free_gb=.1)
    with TestClient(create_app(settings)) as client:
        now = int(utcnow().timestamp())//SLOT*SLOT
        client.app.state.service.store.apply('events',now-SLOT,{},1)
        assert client.post('/api/admin/backfill', json={'hours':1}).json()['accepted']
        try:
            assert entered.wait(10)
            assert client.post('/api/admin/backfill/pause').json()['accepted']
        finally:
            release.set()
        service = client.app.state.service
        wait_until(lambda:service.job is None)
        assert service.paused_backfill is not None
        assert completed == []
    with TestClient(create_app(settings)) as client:
        service = client.app.state.service
        assert service.job is None and service.paused_backfill is not None
        assert client.post('/api/admin/backfill/resume').json()['accepted']
        wait_until(lambda:service.job is None)
        assert service.store.stats()['ledger']['done'] == 8
        assert len(set(completed)) == len(completed) == 7


def test_paused_history_is_excluded_from_monitoring_but_new_data_is_allowed(tmp_path):
    service = Service(Settings(data_dir=tmp_path))
    now = int(utcnow().timestamp())//SLOT*SLOT
    old = now-10*DAY
    service.paused_backfill = {'action':'backfill','start_ts':old,'end_ts':old+DAY,'hours':240,'seeded':True}
    service.store.set_state('paused_backfill', service.paused_backfill)
    service.store.enqueue(old,old+SLOT)
    service.store.enqueue(now-SLOT,now)
    try:
        service.monitor(True)
        batch = service.store.pending(8, **service.work_options())
        assert len(batch)==2 and all(r['file_ts']==now-SLOT for r in batch)
        assert service.paused_backfill is not None
        service.store.apply('events',now-SLOT,{},1)
        service.store.apply('gkg',now-SLOT,{},1)
        service.monitor(False)
        assert not service.store.has_unfinished(**service.work_options())
        assert service.store.has_unfinished()
    finally:
        service.ingestor.close()


def test_stopping_monitor_does_not_cancel_independent_backfill(tmp_path):
    service = Service(Settings(data_dir=tmp_path))
    try:
        service.request('backfill',hours=1)
        service.monitor(False)
        assert service.job is not None
        assert not service.ingestor.cancel.is_set()
    finally:
        service.ingestor.close()


def test_incremental_cursor_survives_restart_and_seeds_every_elapsed_slot(store,tmp_path):
    settings = Settings(data_dir=tmp_path,initial_hours=1)
    now = int(utcnow().timestamp())//SLOT*SLOT
    ingest = Ingestor(store,settings)
    try:
        assert ingest.schedule(now)==8
        assert store.get_state('scheduled_until')==now
        assert store.get_state('incremental_start')==now-3600
    finally:ingest.close()
    restarted = Ingestor(store,settings)
    try:
        assert restarted.schedule(now+2*3600)==16
        assert store.get_state('scheduled_until')==now+2*3600
        assert restarted.schedule(now+2*3600)==0
    finally:restarted.close()


def test_first_monitor_uses_slower_sources_tail_when_no_cursor(store,tmp_path):
    now=int(utcnow().timestamp())//SLOT*SLOT
    store.apply('events',now-2*3600,{},1)
    store.apply('gkg',now-3600,{},1)
    ingest=Ingestor(store,Settings(data_dir=tmp_path))
    try:
        ingest.schedule(now)
        assert store.get_state('incremental_start') <= now-2*3600
        with store.connect() as db:
            earliest=db.execute('SELECT MIN(file_ts) FROM ingest_file').fetchone()[0]
        assert earliest==now-2*3600
        assert store.get_state('scheduled_until')==now
    finally:ingest.close()


def test_backfill_finishes_its_range_without_waiting_for_unrelated_failures(tmp_path,monkeypatch):
    processed=[]
    def process(self,kind,ts):
        processed.append((kind,ts))
        return self.store.apply(kind,ts,{},1)
    monkeypatch.setattr(Ingestor,'process',process)
    settings=Settings(data_dir=tmp_path,batch_files=256,snapshot_days=[7],min_free_gb=.1)
    now=int(utcnow().timestamp())//SLOT*SLOT
    with TestClient(create_app(settings)) as client:
        svc=client.app.state.service
        svc.store.apply('events',now-SLOT,{},1)
        svc.store.mark_failed('gkg',now-100*DAY,RuntimeError('unrelated missing file'))
        target=(utcnow()-timedelta(days=1)).date()
        assert client.post('/api/admin/backfill',json={'start_date':target.isoformat()}).json()['accepted']
        wait_until(lambda:svc.job is None)
        assert processed and all(bucket_of(now,'day')-DAY <= ts < now for _,ts in processed)
        assert svc.store.stats()['ledger'].get('failed')==1
