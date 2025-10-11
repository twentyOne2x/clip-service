import importlib
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


PAYLOAD = {
    'sourceUrl': 'https://example.com/video.mp4',
    'parentTitle': 'Demo',
    'clipLabel': 'Segment',
    'channel': 'Demo Channel',
    'start': 10,
    'end': 20,
    'contextMode': 'seconds',
    'padBefore': 5,
    'padAfter': 5,
}


def reload_service(token: str | None):
    if token is None:
        os.environ.pop('CLIP_SERVICE_AUTH_TOKEN', None)
        os.environ.pop('CLIP_SERVICE_TOKEN', None)
    else:
        os.environ['CLIP_SERVICE_AUTH_TOKEN'] = token
    module = importlib.import_module('main')
    module = importlib.reload(module)
    module.store = module.ClipJobStore()
    return module


def test_post_rejects_missing_bearer_token():
    module = reload_service('secret-token')
    client = TestClient(module.app)

    response = client.post('/clips', json=PAYLOAD)
    assert response.status_code == 401
    assert response.json()['detail'] == 'Missing bearer token'


def test_post_and_get_require_matching_token():
    module = reload_service('my-token')
    client = TestClient(module.app)
    headers = {'Authorization': 'Bearer my-token'}

    response = client.post('/clips', json=PAYLOAD, headers=headers)
    assert response.status_code == 200
    body = response.json()
    clip_id = body['clipId']
    assert body['status'] == 'queued'

    unauthorized = client.get(f'/clips/{clip_id}')
    assert unauthorized.status_code == 401

    authorized = client.get(f'/clips/{clip_id}', headers=headers)
    assert authorized.status_code == 200
    assert authorized.json()['clipId'] == clip_id


@pytest.fixture(autouse=True)
def cleanup_env():
    yield
    os.environ.pop('CLIP_SERVICE_AUTH_TOKEN', None)
    os.environ.pop('CLIP_SERVICE_TOKEN', None)
    importlib.invalidate_caches()
