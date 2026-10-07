from datetime import datetime, timezone
import json
import pytest
from gdelt_server.series_codec import pack_series, expand_series


def test_lossless_missing_fields_dates_and_nulls():
    points = [{'bucket': 0, 'timestamp': '1970-01-01T00:00:00+00:00',
               'event_count': 0, 'risk_score': None, 'complete': False},
              {'bucket': 86400, 'timestamp': '1970-01-02T00:00:00+00:00',
               'event_count': None, 'risk_score': 63.1, 'evidence': 0}]
    assert expand_series(pack_series(points)) == points
    # Preserve timestamps that cannot be derived exactly, including legacy Z form.
    points[0]['timestamp'] = '1970-01-01T00:00:00Z'
    assert expand_series(pack_series(points)) == points


def test_dense_three_year_point_size_is_reduced():
    points = [{'bucket': i*86400, 'timestamp': datetime.fromtimestamp(i*86400, timezone.utc).isoformat(),
               'risk_score': 62.1, 'evidence': 100, 'momentum_available': False,
               'complete': True, 'collected_files': 96, 'expected_files': 96,
               'event_count': 1431, 'conflict': 51, 'security': 42, 'avg_tone': -4.98}
              for i in range(1185)]
    packed = pack_series(points)
    assert expand_series(packed) == points
    assert len(json.dumps(packed)) < len(json.dumps(points)) * .35


@pytest.mark.parametrize('edit', [lambda p:p['rows'][0].pop(),
    lambda p:p.update(fields=['bucket','bucket']), lambda p:p.update(masks=[True])])
def test_malformed_rows_rejected(edit):
    packed = pack_series([{'bucket':0,'timestamp':'1970-01-01T00:00:00+00:00','complete':False}])
    edit(packed)
    with pytest.raises(ValueError): expand_series(packed)
