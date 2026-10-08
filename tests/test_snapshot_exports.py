import gzip
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient
import pytest

from gdelt_server.app import create_app
from gdelt_server.cli import configure_snapshot_access
from gdelt_server.config import Settings
from gdelt_server.snapshot import SnapshotFiles, build_snapshot, encode_json, validate_snapshot


def test_read_token_scope_etag_and_pinned_download(tmp_path):
    settings = Settings(data_dir=tmp_path, api_token='a'*32, snapshot_read_token='r'*32)
    admin = {'Authorization':'Bearer '+'a'*32}
    readonly = {'Authorization':'Bearer '+'r'*32}
    with TestClient(create_app(settings)) as client:
        files = client.app.state.service.snapshots
        a = files.publish(build_snapshot(client.app.state.service.store, [7]))
        assert client.get('/api/snapshots/latest').status_code == 401
        latest = client.get('/api/snapshots/latest', headers=readonly)
        assert latest.json()['schema_version'] == 1
        assert latest.json()['time_ranges'] == [7]
        assert set(latest.json()['coverage_by_range']['7']) == {'events','gkg'}
        assert client.get('/api/snapshots/latest', headers={**readonly,'If-None-Match':latest.headers['etag']}).status_code == 304
        files.publish(build_snapshot(client.app.state.service.store, [7]))
        pinned = client.get(a['download_path'], headers=readonly)
        assert pinned.status_code == 200
        assert pinned.headers['x-snapshot-id'] == a['snapshot_id']
        assert hashlib.sha256(pinned.content).hexdigest() == a['sha256']
        assert gzip.decompress(pinned.content)
        assert client.get('/api/snapshots/latest', headers={**readonly,'If-None-Match':latest.headers['etag']}).status_code == 200
        assert client.get('/api/snapshots/download', headers=admin).status_code == 200
        assert client.get('/api/gdelt/status', headers=readonly).status_code == 401
        for path in ('monitor','backfill','export','pause','sync'):
            assert client.post('/api/admin/'+path, headers=readonly, json={'enabled':True}).status_code == 401
        assert client.put('/api/admin/params', headers=readonly, json={}).status_code == 401
        assert client.put('/api/admin/concurrency', headers=readonly, json={}).status_code == 401
        assert client.get('/api/snapshots/versions/bad%20id/download', headers=readonly).status_code == 400
        files.publish(build_snapshot(client.app.state.service.store, [7]))
        assert client.get(a['download_path'], headers=readonly).status_code == 404
        assert files.read()['snapshot_id'] != a['snapshot_id']


def test_open_download_survives_concurrent_publication_and_cleanup(store, tmp_path):
    files = SnapshotFiles(tmp_path/'snapshots')
    a = files.publish(build_snapshot(store,[7]))
    manifest, stream = files.open_download(a['snapshot_id'])
    try:
        with ThreadPoolExecutor(1) as worker:
            for _ in range(3):
                worker.submit(files.publish,build_snapshot(store,[7])).result()
        payload = stream.read()
        assert hashlib.sha256(payload).hexdigest() == manifest['sha256']
        assert json.loads(gzip.decompress(payload))['snapshot_id'] == a['snapshot_id']
    finally:
        stream.close()
    files.publish(build_snapshot(store,[7]))
    assert len(list(files.folder.glob('*.json.gz'))) == 2
    assert len(list(files.folder.glob('version-*.json'))) == 2
    assert not list(files.folder.glob('*.tmp'))


def test_legacy_snapshot_upgrades_without_changing_payload(store, tmp_path):
    folder = tmp_path/'legacy'; folder.mkdir()
    snapshot = build_snapshot(store,[7,30])
    payload = gzip.compress(encode_json(snapshot))
    name = 'snapshot-legacy.json.gz'
    (folder/name).write_bytes(payload)
    old = {'filename':name,'sha256':hashlib.sha256(payload).hexdigest(),'bytes':len(payload),
           **{k:snapshot[k] for k in ('snapshot_id','created_at','data_version','parameter_version')}}
    (folder/'latest.json').write_bytes(encode_json(old))
    files = SnapshotFiles(folder)
    assert files.manifest()['time_ranges'] == [7,30]
    assert files.manifest()['sha256'] == old['sha256']
    manifest, stream = files.open_download(snapshot['snapshot_id'])
    with stream:
        assert stream.read() == payload
    assert files.read() == snapshot


def test_same_id_is_immutable_and_duplicate_is_idempotent(store, tmp_path):
    files = SnapshotFiles(tmp_path/'snapshots')
    snapshot = build_snapshot(store,[7])
    original = files.publish(snapshot)
    assert files.publish(snapshot) == original
    changed = {**snapshot,'parameter_version':snapshot['parameter_version']+1}
    with pytest.raises(ValueError, match='相同快照ID'):
        files.publish(changed)
    from gdelt_server.series_codec import pack_series
    assert files.read() == pack_series(snapshot)


@pytest.mark.parametrize('limit', ['MAX_UNPACKED', 'MAX_COMPRESSED'])
def test_publisher_checks_receiver_limits_without_replacing_previous(store, tmp_path, monkeypatch, limit):
    import gdelt_server.snapshot as module
    files = SnapshotFiles(tmp_path/'snapshots')
    snapshot = build_snapshot(store, [7])
    previous = files.publish(snapshot)
    monkeypatch.setattr(module, limit, 1)
    with pytest.raises(ValueError, match='超过'):
        files.publish(build_snapshot(store, [7]))
    assert files.manifest() == previous


@pytest.mark.parametrize('change', [
    {'snapshot_id':'../private'}, {'snapshot_id':''}, {'data_version':True},
    {'parameter_version':-1}, {'created_at':'2026-01-01'}, {'schema_version':999},
])
def test_snapshot_contract_validation(store, change):
    with pytest.raises(ValueError):
        validate_snapshot({**build_snapshot(store,[7]),**change})


def test_configure_sync_preserves_settings_and_existing_credentials(tmp_path):
    path = tmp_path/'config.local.json'
    values = {'data_dir':'./keep-data','download_workers':32,'parser_workers':8,'api_token':'a'*32}
    path.write_text(json.dumps(values))
    first = configure_snapshot_access(path)
    saved = json.loads(path.read_text())
    assert all(saved[k] == v for k,v in values.items())
    assert first['api_token'] == 'a'*32 and len(first['snapshot_read_token']) >= 24
    assert first == configure_snapshot_access(path)
    assert Settings.load(path).snapshot_read_token == first['snapshot_read_token']
    assert not list(tmp_path.glob('*.tmp'))


@pytest.mark.parametrize('values', [
    {'snapshot_read_token':'short'}, {'api_token':'a'*32,'snapshot_read_token':'a'*32},
    {'snapshot_read_token':'r'*32},
])
def test_readonly_key_must_be_separate_and_admin_protected(values):
    with pytest.raises(ValueError):
        Settings(**values)


def test_environment_read_token_is_supported(tmp_path, monkeypatch):
    path = tmp_path/'config.json'
    path.write_text(json.dumps({'api_token':'a'*32}))
    monkeypatch.setenv('GDELT_SNAPSHOT_READ_TOKEN', 'r'*32)
    assert Settings.load(path).snapshot_read_token == 'r'*32


def test_overview_country_trend_matches_local_page_queries(store, tmp_path, recent_ts):
    from test_metrics_snapshot import populate
    from gdelt_server.query import QueryCache
    from gdelt_server.snapshot import snapshot_view
    populate(store,tmp_path,recent_ts)
    snapshot = build_snapshot(store,[7,30])
    queries = QueryCache(store)
    for days in (7,30):
        for country in ('USA','FRA',''):
            expected = queries.resolve('overview',days,country)
            actual = snapshot_view(snapshot,'overview',days,country)
            assert actual['selected'] == expected['selected']
            assert actual['series'] == expected['series']
            assert actual['countries'] == expected['countries']


def test_failed_manifest_switch_keeps_previous_and_allows_retry(store, tmp_path, monkeypatch):
    import gdelt_server.snapshot as module
    from pathlib import Path
    files = SnapshotFiles(tmp_path/'snapshots')
    old = files.publish(build_snapshot(store,[7]))
    incoming = build_snapshot(store,[7])
    replace = module.os.replace
    def fail_switch(source, target):
        if Path(target).name == 'latest.json':
            raise OSError('simulated manifest write failure')
        replace(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(module.os,'replace',fail_switch)
        with pytest.raises(OSError):
            files.publish(incoming)
    assert files.manifest() == old
    assert files.read()['snapshot_id'] == old['snapshot_id']
    assert len(list(files.folder.glob('*.json.gz'))) == 1
    assert not list(files.folder.glob('*.tmp'))
    assert files.publish(incoming)['snapshot_id'] == incoming['snapshot_id']


def test_configure_sync_refuses_environment_credential_conflict(tmp_path, monkeypatch):
    from gdelt_server.cli import main
    import sys
    path = tmp_path/'config.json'; path.write_text('{}')
    monkeypatch.setenv('GDELT_API_TOKEN','existing-secret-in-env')
    monkeypatch.setattr(sys,'argv',['gdelt-server','--config',str(path),'configure-sync'])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    assert path.read_text() == '{}'


def test_snapshot_exports_extra_daily_history_for_short_windows(store,monkeypatch):
    import gdelt_server.snapshot as module
    calls=[]
    def series(metrics,days,name, hourly=False):
        calls.append((days,name))
        return {'USA':[{'bucket':0,'timestamp':'1970-01-01T00:00:00Z','complete':True,'event_count':2}]}
    monkeypatch.setattr(module,'all_series',series)
    snapshot=build_snapshot(store,[7,365])
    assert (455,'attitude') in calls and (455,'country-risk') in calls and (455,'enterprise-risk') in calls
    assert len(snapshot['trend_history']['overview_all'])==455
    assert all('attitude_score' in p for p in snapshot['trend_history']['overview_all'])
    assert snapshot['schema_version']==1
    assert (9, 'country-risk') in calls
    assert set(snapshot['trend_history_hour']) == {'attitude','country-risk','enterprise-risk','overview_all'}


def test_three_year_window_and_realtime_are_published_with_history(store,monkeypatch):
    import gdelt_server.snapshot as module
    from gdelt_server.metrics import make_window
    calls=[]
    def series(metrics,days,name, hourly=False):
        calls.append((days,name))
        return {}
    monkeypatch.setattr(module,'all_series',series)
    snapshot=build_snapshot(store,[1,1095])
    assert set(snapshot['views'])=={'1','1095'}
    assert snapshot['views']['1095']['overview']['common']['window']['days']==1095
    assert snapshot['views']['1']['overview']['common']['window']['granularity']=='hour'
    assert (1185,'attitude') in calls
    assert make_window(1185).days==1185


def test_existing_service_upgrades_windows_without_shortening_backfill_retention(tmp_path):
    from gdelt_server.service import Service
    settings=Settings(data_dir=tmp_path,snapshot_days=[7],day_retention_days=3650)
    service=Service(settings)
    assert service.settings.snapshot_days==[1,7,1095]
    assert service.settings.day_retention_days==3650
    assert settings.snapshot_days==[7]  # caller's settings are not mutated


def test_historical_scores_use_shared_formulas_per_bucket(store,tmp_path,recent_ts):
    from test_metrics_snapshot import populate
    from gdelt_server.metrics import Metrics
    from gdelt_server.snapshot import all_series
    populate(store,tmp_path,recent_ts)
    m=Metrics(store)
    # All fixture evidence lives in one bucket; the ranking and this bucket share statistics.
    for name,method,code in [('country-risk',m.country_risk,'US'),('enterprise-risk',m.enterprise_risk,'US')]:
        points=all_series(m,7,name)[code]
        valid=[p for p in points if p['risk_score'] is not None]
        assert valid
        assert valid[-1]['risk_score']==method(7,code)['selected']['risk_score']
        assert all(0<=p['risk_score']<=100 for p in valid)
        assert all(p['risk_score'] is None for p in points if not p['collected_files'])
    overview=m.overview(7)
    points=[p for p in overview['series'] if p['attitude_score'] is not None]
    assert points[-1]['attitude_score']==overview['summary']['attitude_score']


def test_enterprise_history_does_not_mix_percentile_reference_dates():
    from gdelt_server.metrics import Params,enterprise_score_components
    fields=('security','political','economic','infrastructure','social','health')
    def row(negative):
        return {'total_docs':100,'sum_tone':-negative*100,**{f+'_docs':0 for f in fields}}
    a=enterprise_score_components({'US':row(1),'RS':row(2)},Params())
    b=enterprise_score_components({'US':row(2),'RS':row(1)},Params())
    assert a['US'][0]['negativity']==0
    assert b['US'][0]['negativity']==100


def test_hour_history_uses_hourly_coverage_and_precedes_seven_day_view(store,tmp_path,recent_ts):
    from test_metrics_snapshot import populate
    populate(store,tmp_path,recent_ts)
    snapshot=build_snapshot(store,[1,7,30])
    hour=snapshot['trend_history_hour']
    for name,code in [('attitude','USA'),('country-risk','US'),('enterprise-risk','US')]:
        points=hour[name][code]
        assert len(points)==216
        assert points[1]['bucket']-points[0]['bucket']==3600
        assert snapshot['views']['7'][name]['series_by_country'][code][0]['bucket']-points[0]['bucket']==48*3600
        assert all(p['risk_score'] is None for p in points if not p['collected_files']) if name.endswith('risk') else True
    global_points=hour['overview_all']
    assert len(global_points)==216
    assert all(p['attitude_score'] is None and p['event_count'] is None for p in global_points if not p['collected_files'])
