from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import patch
import sqlite3

import httpx
import pytest

from gdelt_server.config import Settings
from gdelt_server.ingest import Ingestor
from gdelt_server.parser import parse_events, parse_gkg, gkg_categories, is_china_business
from gdelt_server.store import DAY,HOUR,SLOT,bucket_of,utcnow
from conftest import zipped,event_row,gkg_row


def count(store,table,gran):
    with store.connect() as db:
        col = "total_docs" if table == "agg_gkg" else "n_events"
        return db.execute(f"SELECT SUM({col}) FROM {table} WHERE granularity=?",(gran,)).fetchone()[0] or 0


@pytest.mark.parametrize("layout",[58,61])
def test_event_columns_and_transaction_idempotency(store,tmp_path,recent_ts,layout):
    path=tmp_path/"sample.zip";path.write_bytes(zipped([event_row(layout=layout)]))
    parsed=parse_events(path,recent_ts)
    assert parsed.rows==1
    assert store.apply("events",recent_ts,parsed.tables,parsed.rows)
    assert not store.apply("events",recent_ts,parsed.tables,parsed.rows)
    assert count(store,"agg_geo","hour")==count(store,"agg_geo","day")==1
    with store.connect() as db:
        assert db.execute("SELECT sum_gold,sum_tone,sum_quad_w,sum_w FROM agg_relation WHERE granularity='hour'").fetchone()[:] == (-1,-.2,-1.5,1.5)
        tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert not any(t.startswith("raw_") for t in tables)


def test_gkg_country_dedup_and_no_original_rows(store,tmp_path,recent_ts):
    path=tmp_path/"gkg.zip";path.write_bytes(zipped([gkg_row(countries=("US","US","FR","CH"))]))
    parsed=parse_gkg(path,recent_ts)
    assert len(parsed.tables["agg_gkg"])==2
    store.apply("gkg",recent_ts,parsed.tables,parsed.rows)
    assert count(store,"agg_gkg","day")==2
    assert is_china_business("Huawei;Tencent")
    assert not is_china_business("Bydgoszcz;aztec")
    assert "security" in gkg_categories("TERRORISM;KILLING;WOUNDED")
    with store.connect() as db:
        assert db.execute("SELECT china_business_docs FROM agg_gkg WHERE granularity='day' AND country='US'").fetchone()[0]==1


def test_duplicate_race_only_one_commit(store,tmp_path,recent_ts):
    path=tmp_path/"gkg.zip";path.write_bytes(zipped([gkg_row()]))
    parsed=parse_gkg(path,recent_ts)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:store.apply("gkg",recent_ts,parsed.tables,1),range(2)))
    assert sorted(results)==[False,True]
    assert count(store,"agg_gkg","day")==1


def test_mid_file_failure_rolls_back_and_retries(store,recent_ts):
    tables={"agg_geo":{("hour",bucket_of(recent_ts,"hour"),"US","19"):{"n_events":30}},
            "agg_gkg":{("hour",bucket_of(recent_ts,"hour"),"US"):{"sum_tone":float('nan')}}}
    with pytest.raises(ValueError):store.apply("events",recent_ts,tables,30)
    assert count(store,"agg_geo","hour")==0
    assert store.get_state("data_version",0)==0
    with store.connect() as db:assert db.execute("SELECT COUNT(*) FROM ingest_file").fetchone()[0]==0


def test_retention_never_overwrites_daily_and_preserves_ledger(store):
    now=int(utcnow().timestamp());day=bucket_of(now-61*DAY,"day")
    for h in range(24):
        ts=day+h*HOUR
        store.apply("gkg",ts,{"agg_gkg":{("hour",ts,"US"):{"total_docs":1}}},1)
    store.prune(60,730)
    assert count(store,"agg_gkg","hour")==0
    assert count(store,"agg_gkg","day")==24
    assert not store.apply("gkg",day,{"agg_gkg":{("hour",day,"US"):{"total_docs":1}}},1)
    assert count(store,"agg_gkg","day")==24


def test_hour_cleanup_aligns_natural_day(store):
    now=int(utcnow().timestamp());cut=bucket_of(now,"day")-60*DAY
    store.apply("gkg",cut,{"agg_gkg":{("hour",cut,"US"):{"total_docs":1}}},1)
    store.prune(60,730)
    assert count(store,"agg_gkg","hour")==1


def test_schedule_catches_hourly_and_restart_gaps(store,tmp_path):
    settings=Settings(data_dir=tmp_path,initial_hours=1,min_free_gb=.1)
    ing=Ingestor(store,settings)
    try:
        now=int(utcnow().timestamp())//SLOT*SLOT
        assert ing.schedule(now)==8
        assert ing.schedule(now)==0
        assert ing.schedule(now+3600)==8
        assert ing.schedule(now+DAY*3)==192
        assert store.get_state("scheduled_until")==now+3600+DAY
        for _ in range(3):ing.schedule(now+DAY*3)
        with store.connect() as db:
            assert db.execute("SELECT COUNT(*) FROM ingest_file").fetchone()[0]==(1+72)*8
    finally:ing.close()


def test_backfill_all_72_hours_not_tail_cap(store,tmp_path):
    ing=Ingestor(store,Settings(data_dir=tmp_path,min_free_gb=.1))
    try:
        assert ing.backfill(72)==576
        assert ing.backfill(72)==0
        rows=store.pending(600)
        assert len(rows)==576
        assert max(r['file_ts'] for r in rows)-min(r['file_ts'] for r in rows)==72*3600-SLOT
        for row in rows[:32]:store.apply(row['kind'],row['file_ts'],{},0)
        assert len(store.pending(600))==544
    finally:ing.close()


@pytest.mark.parametrize("mode",["valid","http_error","invalid_zip","oversize"])
def test_temporary_download_cleanup(store,tmp_path,recent_ts,mode):
    payload=zipped([gkg_row()])
    if mode=="invalid_zip":payload=b"broken"
    if mode=="oversize":payload=b'x'*(1024*1024+1)
    client=httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(503 if mode=='http_error' else 200,content=payload)))
    settings=Settings(data_dir=tmp_path,min_free_gb=.1,max_download_mb=1)
    ing=Ingestor(store,settings,client)
    try:
        if mode=="valid":assert ing.process("gkg",recent_ts)
        else:
            with pytest.raises(Exception):ing.process("gkg",recent_ts)
            assert store.get_state('data_version',0)==0
        assert list((tmp_path/'tmp').glob('*.zip'))==[]
    finally:client.close()


def test_retry_after_404_not_marked_done(store,tmp_path,recent_ts):
    store.enqueue(recent_ts,recent_ts+SLOT)
    client=httpx.Client(transport=httpx.MockTransport(lambda request:httpx.Response(404)))
    ing=Ingestor(store,Settings(data_dir=tmp_path,min_free_gb=.1),client)
    result=ing.run_once(schedule=False)
    assert result['failed']==2 and result['processed']==0
    assert store.stats()['ledger']=={'failed':2}
    assert store.pending(10)==[]
    with store.connect(write=True) as db:db.execute('UPDATE ingest_file SET next_retry=0')
    assert len(store.pending(10))==2
    client.close()


def test_low_disk_pauses_before_network(store,tmp_path,recent_ts,monkeypatch):
    client=httpx.Client(transport=httpx.MockTransport(lambda request:pytest.fail('must not download')))
    ing=Ingestor(store,Settings(data_dir=tmp_path,min_free_gb=.1),client)
    monkeypatch.setattr('gdelt_server.ingest.shutil.disk_usage',lambda _:type('Usage',(),{'free':1})())
    with pytest.raises(RuntimeError):ing.process('gkg',recent_ts)
    assert list((tmp_path/'tmp').glob('*.zip'))==[]
    client.close()


def test_layout_and_zip_size_failure(tmp_path,recent_ts):
    path=tmp_path/'bad.zip';path.write_bytes(zipped([['x']*60]))
    with pytest.raises(ValueError,match='布局'):parse_events(path,recent_ts)
    path.write_bytes(zipped([['x'*(2*1024*1024)]*61]))
    with pytest.raises(ValueError,match='解压'):parse_events(path,recent_ts,max_uncompressed_mb=1)
