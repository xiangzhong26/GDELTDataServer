from conftest import event_row,zipped
from gdelt_server.parser import parse_events


def test_known_official_unclassified_placeholder_is_counted_as_skip(tmp_path,recent_ts):
    row=event_row('2')
    row[26]=row[27]='---'
    row[28]='--'
    row[30]=''
    path=tmp_path/'sample.zip';path.write_bytes(zipped([event_row('1'),row]))
    parsed=parse_events(path,recent_ts)
    assert parsed.rows==2 and parsed.skipped==1
    assert sum(v['n_events'] for v in parsed.tables['agg_geo'].values())==1
