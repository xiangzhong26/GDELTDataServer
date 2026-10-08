import pytest
from conftest import zipped, event_row, gkg_row
from gdelt_server.parser import parse_events, parse_gkg


def test_missing_official_score_input_skips_only_the_record(tmp_path, recent_ts):
    row = event_row('2', root='12'); row[26] = '1213'; row[29] = '3'; row[30] = ''
    path = tmp_path / 'events.zip'; path.write_bytes(zipped([event_row(), row, event_row('3')]))
    parsed = parse_events(path, recent_ts)
    assert parsed.rows == 3 and parsed.skipped == 1
    assert sum(r['n_events'] for r in parsed.tables['agg_geo'].values()) == 2


@pytest.mark.parametrize('count', ['nan', 'inf', '1.5', 'broken'])
def test_nonempty_invalid_counts_cannot_silently_become_zero(tmp_path, recent_ts, count):
    row = event_row(); row[31] = count
    path = tmp_path / 'events.zip'; path.write_bytes(zipped([row]))
    with pytest.raises(ValueError):
        parse_events(path, recent_ts)


def test_official_xml_newlines_preserve_document_count(tmp_path, recent_ts):
    first = gkg_row('20260917130000-1033')
    first[26] = '<PAGE_LINKS>https://example.org/?body=feedback'
    path = tmp_path / 'gkg.zip'
    path.write_bytes(zipped([first, [], ['---'], [], ['&HomeUrl=x</PAGE_LINKS><PAGE_TITLE>Title</PAGE_TITLE>'], gkg_row('20260917130000-1034')]))
    parsed = parse_gkg(path, recent_ts)
    assert parsed.rows == 2 and parsed.skipped == 0
    assert sum(r['total_docs'] for r in parsed.tables['agg_gkg'].values()) == 2


@pytest.mark.parametrize('tail', [['20260917130000-1034'], ['bad', 'column']])
def test_short_core_record_is_not_an_xml_continuation(tmp_path, recent_ts, tail):
    row = gkg_row('20260917130000-1033'); row[26] = '<PAGE_LINKS>x'
    path = tmp_path / 'gkg.zip'; path.write_bytes(zipped([row, tail]))
    with pytest.raises(ValueError, match='列布局'):
        parse_gkg(path, recent_ts)


def test_recent_ingest_history_survives_restart(store, recent_ts):
    from gdelt_server.store import Store
    store.apply('events', recent_ts, {}, 10, 2)
    rows = Store(store.path).recent_files()
    assert len(rows) == 1 and rows[0]['row_count'] == 10 and rows[0]['skipped_rows'] == 2


def test_invalid_nonempty_score_is_not_hidden_by_another_missing_input(tmp_path, recent_ts):
    row = event_row(); row[30] = 'broken'; row[34] = ''
    path = tmp_path/'events.zip'; path.write_bytes(zipped([row]))
    with pytest.raises(ValueError):
        parse_events(path, recent_ts)
