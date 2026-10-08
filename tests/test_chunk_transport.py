import gzip
import hashlib
import json
import random
from gdelt_server.chunk_transport import compress_chunks
from gdelt_server.snapshot import SnapshotFiles, build_snapshot
from gdelt_server.config import Settings
from gdelt_server.app import create_app
from fastapi.testclient import TestClient


def test_content_chunks_reuse_stable_history_and_reconstruct_exact_json():
    rng = random.Random(42)
    rows = [[i, *[rng.randrange(1000000) for _ in range(10)]] for i in range(50000)]
    raw = json.dumps({'version': 1, 'rows': rows}, separators=(',', ':')).encode()
    first, index = compress_chunks(raw)
    newer, second = compress_chunks(raw.replace(b'"version":1', b'"version":200', 1))
    assert gzip.decompress(first) == raw
    assert gzip.decompress(newer) == raw.replace(b'"version":1', b'"version":200', 1)
    hashes = {p['sha256'] for p in index}
    assert sum(p['bytes'] for p in second if p['sha256'] in hashes) > len(newer) * .5
    for part in second:
        assert hashlib.sha256(newer[part['offset']:part['offset'] + part['bytes']]).hexdigest() == part['sha256']


def test_authenticated_chunk_download_and_activation_receipt(tmp_path):
    settings = Settings(data_dir=tmp_path, api_token='a'*32, snapshot_read_token='r'*32)
    headers = {'Authorization': 'Bearer '+'r'*32}
    with TestClient(create_app(settings)) as client:
        service = client.app.state.service
        manifest = service.snapshots.publish(build_snapshot(service.store, [7]))
        blocks = []
        for part in manifest['chunks']:
            path = f"/api/snapshots/versions/{manifest['snapshot_id']}/chunks/{part['sha256']}"
            assert client.get(path).status_code == 401
            response = client.get(path, headers=headers)
            assert response.status_code == 200
            blocks.append(response.content)
        assert hashlib.sha256(b''.join(blocks)).hexdigest() == manifest['sha256']
        body = {k: manifest[k] for k in ('snapshot_id', 'sha256')}
        assert client.post('/api/snapshots/receipt', headers=headers, json={**body, 'sha256': '0'*64}).status_code == 409
        assert service.store.get_state('consumer_receipt') is None
        assert client.post('/api/snapshots/receipt', headers=headers, json=body).status_code == 200
        assert service.status()['consumer_receipt']['snapshot_id'] == manifest['snapshot_id']
        checked = client.get('/api/snapshots/latest', headers=headers)
        assert checked.headers['X-GDELT-Monitor'] == '0'
