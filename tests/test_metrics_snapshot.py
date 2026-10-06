from dataclasses import replace
import gzip
import hashlib
import json
import math

from fastapi.testclient import TestClient
import pytest

from gdelt_server.app import create_app
from gdelt_server.config import Settings
from gdelt_server.metrics import Metrics,Params,make_window,validate_params
from gdelt_server.parser import parse_events,parse_gkg
from gdelt_server.receiver import create_receiver
from gdelt_server.snapshot import build_snapshot,SnapshotFiles,snapshot_view,encode_json
from gdelt_server.store import DAY,SLOT,bucket_of,utcnow
from conftest import zipped,event_row,gkg_row


def populate(store,tmp_path,recent_ts):
    p=tmp_path/'events.zip';p.write_bytes(zipped([event_row(str(i)) for i in range(100)]))
    parsed=parse_events(p,recent_ts);store.apply('events',recent_ts,parsed.tables,parsed.rows)
    p=tmp_path/'gkg.zip';p.write_bytes(zipped([gkg_row(str(i)) for i in range(100)]+[gkg_row(str(i+100),countries=('FR',),themes='',org='',tone='2') for i in range(100)]))
    parsed=parse_gkg(p,recent_ts);store.apply('gkg',recent_ts,parsed.tables,parsed.rows)


def test_default_formulas_and_rank_reference(store,tmp_path,recent_ts):
    populate(store,tmp_path,recent_ts);m=Metrics(store)
    assert m.attitude(30)['selected']['attitude_score']==-92
    cr=m.country_risk(30)['selected']
    expected=.35*(100*(1-math.exp(-1/.12)))+.15*25+.1*50
    assert cr['risk_score']==round(expected,1)
    assert not cr['momentum_available']
    er=m.enterprise_risk(30)
    # Security + economy + negativity score 100; other tied dimensions score 50.
    assert er['selected']['risk_score']==74.5
    assert er['percentile_reference_count']==2
    tiny={('hour',bucket_of(recent_ts,'hour'),'ZZ'):{'total_docs':1,'security_docs':1}}
    store.apply('gkg',recent_ts+SLOT,{'agg_gkg':tiny},1)
    assert m.enterprise_risk(30)['selected']['risk_score']==74.5
    assert len(m.enterprise_risk(30)['countries'])==2


def test_missing_collection_null_and_complete_zero_distinct(store):
    today=bucket_of(int(utcnow().timestamp()),'day')
    # A fully collected day can legitimately have no counted country events.
    with store.connect(write=True) as db:
        db.executemany("INSERT INTO ingest_file(kind,file_ts,status) VALUES ('events',?,'done')",[(d,) for d in range(today-2*DAY,today-DAY,SLOT)])
    series=Metrics(store).attitude(30)['series']
    complete=next(p for p in series if p['bucket']==today-2*DAY)
    missing=next(p for p in series if p['bucket']==today-DAY)
    assert complete['event_count']==0 and complete['complete']
    assert missing['event_count'] is None and not missing['complete']
    assert len(series)==30


def test_momentum_uses_calendar_and_coverage(store):
    today=bucket_of(int(utcnow().timestamp()),'day');m=Metrics(store)
    stale={today-10*DAY:100,today-9*DAY:200}
    assert m.momentum_value(stale,today,Params()) is None
    with store.connect(write=True) as db:
        db.executemany("INSERT INTO ingest_file(kind,file_ts,status) VALUES ('events',?,'done')",[(d,) for d in range(today-3*DAY,today,SLOT)])
    counts={today-3*DAY:100,today-2*DAY:100,today-DAY:200}
    assert m.momentum_value(counts,today,Params())==75
    # No events in the most recent collected day should be a real zero.
    counts.pop(today-DAY)
    assert m.momentum_value(counts,today,Params())==25


@pytest.mark.parametrize('change',[{'w_cr_security':2},{'w_goldstein':-1},{'scale_security':0},
                                  {'ev_docs_coef':float('nan')},{'roots_security':('99',)},
                                  {'min_docs':0},{'momentum_recent_days':31}])
def test_invalid_parameters_rejected(change):
    with pytest.raises(ValueError):validate_params(replace(Params(),**change))


def test_snapshot_country_details_equal_direct_queries(store,tmp_path,recent_ts):
    populate(store,tmp_path,recent_ts);snapshot=build_snapshot(store,[7,30]);m=Metrics(store)
    for name,method,country in [('attitude',m.attitude,'USA'),('country-risk',m.country_risk,'US'),('enterprise-risk',m.enterprise_risk,'US')]:
        for days in (7,30):
            actual=snapshot_view(snapshot,name,days,country);expected=method(days,country)
            assert actual['countries']==expected['countries']
            assert actual['series']==expected['series']
            assert actual['selected']==expected['selected']
            if name=='attitude':assert actual['event_types']==expected['event_types']
    raw=encode_json(snapshot)
    assert b'example.org' not in raw and b'news source' not in raw
    files=SnapshotFiles(tmp_path/'snapshots')
    for _ in range(3):files.publish(build_snapshot(store,[7]))
    assert len(list(files.folder.glob('*.json.gz')))==2
    assert not list(files.folder.glob('*.tmp'))
    assert files.read()['schema_version']==1


def test_cloud_receive_checksum_atomicity_and_offline_reads(store,tmp_path,recent_ts):
    populate(store,tmp_path,recent_ts)
    snapshot=build_snapshot(store,[7,30]);payload=gzip.compress(encode_json(snapshot))
    settings=Settings(data_dir=tmp_path/'cloud',api_token='t'*32)
    app=create_receiver(settings);headers={'Authorization':'Bearer '+'t'*32,'X-SHA256':hashlib.sha256(payload).hexdigest()}
    with TestClient(app) as client:
        assert client.get('/api/gdelt/country-risk').status_code==401
        assert client.put('/api/snapshots',content=payload,headers={**headers,'X-SHA256':'bad'}).status_code==400
        assert client.put('/api/snapshots',content=payload,headers=headers).status_code==200
        assert client.put('/api/snapshots',content=payload,headers=headers).json()['duplicate']
        result=client.get('/api/gdelt/country-risk?days=30&country=US',headers=headers).json()
        assert result['selected']['code']=='US'
        # An incomplete replacement cannot erase the last valid snapshot.
        broken=gzip.compress(encode_json({'schema_version':1}))
        assert client.put('/api/snapshots',content=broken,headers={**headers,'X-SHA256':hashlib.sha256(broken).hexdigest()}).status_code==400
        assert client.get('/api/gdelt/country-risk',headers=headers).json()['snapshot_id']==snapshot['snapshot_id']
        older={**snapshot,'snapshot_id':'old','created_at':'2020-01-01T00:00:00+00:00'}
        old_payload=gzip.compress(encode_json(older))
        assert client.put('/api/snapshots',content=old_payload,headers={**headers,'X-SHA256':hashlib.sha256(old_payload).hexdigest()}).status_code==409
    # Restart receiver with no connection to the data server: last complete results survive.
    with TestClient(create_receiver(settings)) as client:
        assert client.get('/api/gdelt/enterprise-risk',headers=headers).json()['selected']['code']=='US'


def test_api_default_off_auth_parameters_and_no_raw(tmp_path):
    settings=Settings(data_dir=tmp_path,api_token='x'*32,snapshot_days=[7])
    with TestClient(create_app(settings)) as client:
        assert client.get('/health').json()['ok']
        assert client.get('/api/gdelt/status').status_code==401
        headers={'Authorization':'Bearer '+'x'*32}
        status=client.get('/api/gdelt/status',headers=headers).json()
        assert not status['monitor_enabled'] and not status['busy']
        assert status['storage']['raw_data_retained'] is False
        assert client.get('/api/gdelt/details',headers=headers).status_code==410
        assert client.put('/api/admin/params',headers=headers,json={'w_cr_security':2}).status_code==400
        assert client.put('/api/admin/params',headers=headers,json={'save':'bad'}).status_code==400
        response=client.put('/api/admin/params',headers=headers,json={'tone_negative_divisor':4})
        assert response.status_code==200
        assert client.get('/api/gdelt/status',headers=headers).json()['parameter_version']==1
        assert client.get('/api/gdelt/metrics',headers=headers).json()['params']['tone_negative_divisor']==4
        assert client.post('/api/admin/backfill',headers=headers,json={'hours':20000}).status_code==400


def test_network_bind_requires_token():
    with pytest.raises(ValueError):Settings(host='0.0.0.0')


def test_single_instance_lock(tmp_path):
    from gdelt_server.instance import InstanceLock
    a=InstanceLock(tmp_path/'lock');b=InstanceLock(tmp_path/'lock')
    a.acquire()
    try:
        with pytest.raises(RuntimeError):b.acquire()
    finally:a.release()
    b.acquire();b.release()
