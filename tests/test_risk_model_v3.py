"""Behavioral safeguards for the model: direction, severity, exposure, migration."""
import json
import sqlite3
from dataclasses import replace

import pytest

from gdelt_server.metrics import Metrics, Params, validate_params, enterprise_score_components
from gdelt_server.parser import parse_events, parse_gkg
from gdelt_server.store import GKG_METRICS, Store
from gdelt_server.snapshot import build_snapshot, all_series
from conftest import event_row, gkg_row, zipped


def ingest_events(store, path, ts, *, root='19', gold='-10', tone='0', count=100, actor1='USA'):
    rows = [event_row(str(i),root=root,tone=tone,actor1=actor1) for i in range(count)]
    for row in rows:
        row[30] = gold
    path.write_bytes(zipped(rows))
    parsed = parse_events(path, ts)
    store.apply('events',ts,parsed.tables,parsed.rows)


def test_severity_and_military_posture_count(store,tmp_path,recent_ts):
    path = tmp_path/'event.zip'
    ingest_events(store,path,recent_ts,root='19',gold='-10')
    war = Metrics(store).country_risk(7)['selected']
    # Different country, same volume and tone: lower severity produces lower risk.
    rows = [event_row(str(i),root='15',geo='FR',tone='0') for i in range(100)]
    for r in rows:r[30]='-3'
    path.write_bytes(zipped(rows));parsed=parse_events(path,recent_ts+900)
    store.apply('events',recent_ts+900,parsed.tables,parsed.rows)
    military=next(r for r in Metrics(store).country_risk(7)['countries'] if r['code']=='FR')
    assert 0 < military['security'] < war['security']
    assert military['risk_score'] < war['risk_score']


def test_coerce_does_not_double_count_or_outrank_war(store,tmp_path,recent_ts):
    path=tmp_path/'event.zip'
    rows=[event_row(str(i),root='17',geo='FR',tone='0') for i in range(100)]
    for r in rows:r[30]='-6'
    path.write_bytes(zipped(rows));p=parse_events(path,recent_ts)
    store.apply('events',recent_ts,p.tables,p.rows)
    ingest_events(store,path,recent_ts+900)
    result=Metrics(store).country_risk(7)
    coerce=next(r for r in result['countries'] if r['code']=='FR')
    assert coerce['political']==0
    assert coerce['risk_score'] < result['selected']['risk_score']


def test_momentum_never_changes_risk_score():
    from gdelt_server.metrics import country_score_components
    a=dict(n_events=100,sum_sources=100,sum_w=100,sum_tone=0,w_sec=10,w_soc=0,w_pol=0)
    assert country_score_components(a,Params(),0)[6]==country_score_components(a,Params(),100)[6]


def test_positive_theme_news_does_not_create_enterprise_risk(store,tmp_path,recent_ts):
    path=tmp_path/'gkg.zip'
    path.write_bytes(zipped([gkg_row(str(i),themes='ECON_TRADE;ENERGY;MEDICAL;',tone='5') for i in range(100)]))
    p=parse_gkg(path,recent_ts);store.apply('gkg',recent_ts,p.tables,p.rows)
    row=Metrics(store).enterprise_risk(7)['selected']
    assert row['risk_score']==0
    assert row['joint_coverage']==100 and row['joint_method']=='measured'


def test_negative_and_theme_must_be_in_same_article(store,tmp_path,recent_ts):
    path=tmp_path/'gkg.zip'
    rows=[gkg_row(str(i),themes='ECON_TRADE;',tone='2') for i in range(50)]
    rows += [gkg_row(str(i+50),themes='',tone='-8') for i in range(50)]
    path.write_bytes(zipped(rows));p=parse_gkg(path,recent_ts)
    store.apply('gkg',recent_ts,p.tables,p.rows)
    row=Metrics(store).enterprise_risk(7)['selected']
    assert row['economic']==0 and row['negativity']>0


def test_unknown_gkg_tone_is_skipped_not_measured_neutral(tmp_path,recent_ts):
    path=tmp_path/'gkg.zip'
    path.write_bytes(zipped([gkg_row('missing',tone=''),gkg_row('valid',tone='-2')]))
    parsed=parse_gkg(path,recent_ts)
    assert parsed.rows==2 and parsed.skipped==1
    assert sum(a['modeled_docs'] for a in parsed.tables['agg_gkg'].values())==1


def test_other_countries_do_not_change_enterprise_score():
    def row(tone):
        return {'total_docs':100,'sum_tone':tone*100,
                **{f+'_docs':30 for f in Metrics.ER_FIELDS}}
    own=row(-2)
    one=enterprise_score_components({'US':own},Params())['US']
    many=enterprise_score_components({'US':own,'FR':row(-10),'RS':row(2)},Params())['US']
    assert one==many
    assert enterprise_score_components({'US':row(-2.01)},Params())['US'][1]-one[1]<1


def test_legacy_data_labeled_and_mixed_mass_not_double_counted(store,tmp_path,recent_ts):
    old=dict.fromkeys(GKG_METRICS,0)
    old.update(total_docs=100,economic_docs=100,sum_tone=-200)
    from gdelt_server.store import bucket_of
    store.apply('gkg',recent_ts,{'agg_gkg':{('hour',bucket_of(recent_ts,'hour'),'US'):old}},100)
    legacy=Metrics(store).enterprise_risk(7)['selected']
    assert legacy['joint_method']=='legacy-estimate' and legacy['joint_coverage']==0
    path=tmp_path/'gkg.zip';path.write_bytes(zipped([gkg_row(str(i),themes='ECON_TRADE;',tone='-2') for i in range(100)]))
    p=parse_gkg(path,recent_ts+900);store.apply('gkg',recent_ts+900,p.tables,p.rows)
    mixed=Metrics(store).enterprise_risk(7)['selected']
    assert mixed['joint_coverage']==50 and mixed['joint_method']=='legacy-estimate'
    # The density is unchanged; only additional sample support can increase score.
    from gdelt_server.metrics import enterprise_density
    with store.connect() as db:a=dict(db.execute("SELECT * FROM agg_gkg WHERE granularity='day'").fetchone())
    assert enterprise_density(a,'economic')==pytest.approx(.2)


def test_migrate_old_schema_params_once(tmp_path):
    path=tmp_path/'legacy.db'
    with sqlite3.connect(path) as db:
        base=GKG_METRICS[:10]
        db.execute('CREATE TABLE agg_gkg(granularity TEXT,bucket INTEGER,country TEXT,'+
                   ','.join(f'{m} REAL NOT NULL DEFAULT 0' for m in base)+
                   ',PRIMARY KEY(granularity,bucket,country)) WITHOUT ROWID')
        db.execute('CREATE TABLE gdelt_state(key TEXT PRIMARY KEY,value TEXT)')
        old={'w_cr_momentum':.1,'roots_political':['10','11','12','13','16','17']}
        db.executemany('INSERT INTO gdelt_state VALUES (?,?)',[
            ('metric_params',json.dumps(old)),('parameter_version','7')])
    store=Store(path);store.initialize()
    assert Params.load(store).w_cr_momentum==0
    assert store.get_state('parameter_version')==8 and store.get_state('snapshot_dirty')
    assert store.get_state('model_migration_backup')['metric_params']==old
    Params.load(store).save(store);store.initialize()
    assert store.get_state('parameter_version')==8
    with store.connect() as db:assert set(GKG_METRICS)<=set(r['name'] for r in db.execute('PRAGMA table_info(agg_gkg)'))


def test_small_sample_direction_shrinks_and_full_history_one_version(store,tmp_path,recent_ts):
    # Put both batches in one UTC hour; a multi-hour ranking need not equal one point.
    recent_ts = recent_ts - recent_ts % 3600
    path=tmp_path/'event.zip'
    ingest_events(store,path,recent_ts,count=2,actor1='FRA')
    ingest_events(store,path,recent_ts+900,count=100)
    rows=Metrics(store).attitude(7)['countries']
    assert abs(next(r for r in rows if r['code']=='FRA')['attitude_score']) < abs(next(r for r in rows if r['code']=='USA')['attitude_score'])
    snapshot=build_snapshot(store,[7])
    assert snapshot['model_version']=='media-risk-v3'
    assert all(v['common']['model_version']==snapshot['model_version'] for v in snapshot['views']['7'].values())
    for view,method,code in [('attitude',Metrics(store).attitude,'USA'),('country-risk',Metrics(store).country_risk,'US')]:
        points=[p for p in all_series(Metrics(store),7,view)[code] if p.get('risk_score',p.get('attitude_score')) is not None]
        score='risk_score' if view=='country-risk' else 'attitude_score'
        assert points[-1][score]==method(7,code)['selected'][score]


@pytest.mark.parametrize('change',[{'roots_political':('17',)},{'w_cr_momentum':.1,'w_cr_media':0},
                                  {'sample_prior':0},{'risk_curve_power':0},{'risk_curve_power':2}])
def test_v3_rejects_invalid_model_changes(change):
    with pytest.raises(ValueError):validate_params(replace(Params(),**change))
