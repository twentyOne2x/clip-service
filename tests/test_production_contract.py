import asyncio
import hashlib
import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import ValidationError

import main
from persistence import MediaRecord, MemoryRepository, new_job_record


AUTH = 's' * 32
TENANT_A = f"ten_{'a' * 64}"
TENANT_B = f"ten_{'b' * 64}"
USER_A = f"usr_{'a' * 64}"


def identity_headers(tenant=TENANT_A, user=USER_A):
    return {
        'x-icmfyi-internal-secret': AUTH,
        'x-icmfyi-tenant-id': tenant,
        'x-icmfyi-user-id': user,
    }


def configure_test_runtime(monkeypatch, tmp_path: Path, *, production: bool):
    repository = MemoryRepository()
    monkeypatch.setattr(main, 'repository', repository)
    monkeypatch.setattr(main, 'AUTH_TOKEN', AUTH)
    monkeypatch.setattr(main.config, 'production', production)
    monkeypatch.setattr(main.config, 'dry_run', True)
    monkeypatch.setattr(main.config, 'worker_enabled', False)
    monkeypatch.setattr(main.config, 'output_directory', tmp_path / 'output')
    monkeypatch.setattr(main.config, 'media_roots', [tmp_path / 'media'])
    monkeypatch.setattr(main.config, 'verify_media_sha256', True)
    (tmp_path / 'media').mkdir()
    return repository


def test_render_parameters_are_finite_and_bounded():
    base = {
        'sourceUrl': 'https://example.com/video.mp4',
        'start': 0,
        'end': 10,
        'contextMode': 'seconds',
        'padBefore': 0,
        'padAfter': 0,
    }
    main.ClipRequest.parse_obj(base)

    for changes in (
        {'end': 601},
        {'padBefore': 31},
        {'start': float('nan')},
        {'renderProfile': 'caller-controlled-codec'},
    ):
        try:
            main.ClipRequest.parse_obj({**base, **changes})
        except ValidationError:
            pass
        else:
            raise AssertionError(f'unbounded render request unexpectedly passed: {changes}')


def test_production_headers_media_access_and_idempotency_are_tenant_scoped(monkeypatch, tmp_path):
    repository = configure_test_runtime(monkeypatch, tmp_path, production=True)
    source = tmp_path / 'media' / 'source.mp4'
    source.write_bytes(b'canonical-video-bytes')
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    client = TestClient(main.app)

    missing_secret = client.post('/clips', json={
        'mediaId': 'media-1', 'start': 0, 'end': 1,
        'contextMode': 'seconds', 'padBefore': 0, 'padAfter': 0,
    })
    assert missing_secret.status_code == 401

    missing_user = client.post('/clips', headers={
        'x-icmfyi-internal-secret': AUTH,
        'x-icmfyi-tenant-id': TENANT_A,
    }, json={
        'mediaId': 'media-1', 'start': 0, 'end': 1,
        'contextMode': 'seconds', 'padBefore': 0, 'padAfter': 0,
    })
    assert missing_user.status_code == 400

    swapped_scopes = client.post('/clips', headers=identity_headers(
        tenant=f"usr_{'c' * 64}",
        user=f"ten_{'d' * 64}",
    ), json={
        'mediaId': 'media-1', 'start': 0, 'end': 1,
        'contextMode': 'seconds', 'padBefore': 0, 'padAfter': 0,
    })
    assert swapped_scopes.status_code == 400

    repository.register_media(
        MediaRecord('media-1', str(source.resolve()), source_sha, True),
        [TENANT_A],
    )
    registration_attempt = client.post('/internal/media', headers=identity_headers(), json={
        'mediaId': 'media-1',
        'sourcePath': str(source),
        'sourceSha256': source_sha,
        'videoCapable': True,
    })
    assert registration_attempt.status_code == 404

    payload = {
        'mediaId': 'media-1', 'start': 0, 'end': 1,
        'contextMode': 'seconds', 'padBefore': 0, 'padAfter': 0,
    }
    created = client.post(
        '/clips', headers={**identity_headers(), 'Idempotency-Key': 'clip-1'}, json=payload,
    )
    assert created.status_code == 200
    clip_id = created.json()['clipId']

    assert client.get(f'/clips/{clip_id}', headers=identity_headers(TENANT_B)).status_code == 404
    assert client.get(f'/clips/{clip_id}/file', headers=identity_headers(TENANT_B)).status_code == 404

    conflict = client.post(
        '/clips',
        headers={**identity_headers(), 'Idempotency-Key': 'clip-1'},
        json={**payload, 'end': 2},
    )
    assert conflict.status_code == 409

    arbitrary_url = client.post('/clips', headers=identity_headers(), json={
        'sourceUrl': 'https://example.com/video.mp4', 'start': 0, 'end': 1,
        'contextMode': 'seconds', 'padBefore': 0, 'padAfter': 0,
    })
    assert arbitrary_url.status_code == 400


def test_legacy_batch_ids_cannot_cross_tenants(monkeypatch, tmp_path):
    configure_test_runtime(monkeypatch, tmp_path, production=False)
    client = TestClient(main.app)
    payload = {
        'clips': [{
            'key': 'one',
            'sourceUrl': 'https://example.com/video.mp4',
            'start': 0,
            'end': 1,
            'contextMode': 'seconds',
            'padBefore': 0,
            'padAfter': 0,
        }],
    }
    created = client.post('/clips/batch', headers=identity_headers(), json=payload)
    assert created.status_code == 200
    batch_id = created.json()['batchId']

    assert client.get(f'/clips/batch/{batch_id}', headers=identity_headers(TENANT_B)).status_code == 404
    assert client.get(f'/clips/batch/{batch_id}/file', headers=identity_headers(TENANT_B)).status_code == 404
    assert client.patch(
        f'/clips/batch/{batch_id}',
        headers=identity_headers(TENANT_B),
        json={'clipKey': 'one'},
    ).status_code == 404


def test_every_batch_endpoint_fails_closed_in_production(monkeypatch, tmp_path):
    configure_test_runtime(monkeypatch, tmp_path, production=True)
    client = TestClient(main.app)

    assert client.post(
        '/clips/batch',
        headers=identity_headers(),
        json={'clips': []},
    ).status_code == 503
    assert client.get(
        '/clips/batch/nonexistent',
        headers=identity_headers(),
    ).status_code == 503
    assert client.get(
        '/clips/batch/nonexistent/file',
        headers=identity_headers(),
    ).status_code == 503
    assert client.patch(
        '/clips/batch/nonexistent',
        headers=identity_headers(),
        json={'clipKey': 'one'},
    ).status_code == 503


def test_real_ffmpeg_render_is_validated_and_content_addressed(monkeypatch, tmp_path):
    configure_test_runtime(monkeypatch, tmp_path, production=False)
    monkeypatch.setattr(main.config, 'dry_run', False)
    source = tmp_path / 'media' / 'source.mp4'
    subprocess.run(
        [
            'ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
            '-f', 'lavfi', '-i', 'testsrc=size=640x360:rate=30',
            '-f', 'lavfi', '-i', 'sine=frequency=1000:sample_rate=48000',
            '-t', '2', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', str(source),
        ],
        check=True,
    )
    request = main.ClipRequest.parse_obj({
        'mediaId': 'media-1', 'start': 0.25, 'end': 1.25,
        'contextMode': 'seconds', 'padBefore': 0, 'padAfter': 0,
        'preferVideo': True,
    })
    job = main.Job(id='render-job', payload=request)

    asyncio.run(main.run_ffmpeg_clip(job, source_override=source, raise_errors=True))

    assert job.status == 'ready'
    assert job.output_path is not None and job.output_path.is_file()
    assert job.artifact_sha256 == hashlib.sha256(job.output_path.read_bytes()).hexdigest()
    assert job.output_path.name == f'{job.artifact_sha256}.mp4'
    assert job.validation['video_codec'] == 'h264'
    assert job.validation['audio_codec'] == 'aac'


def test_file_delivery_honors_single_ranges_and_tenant_scope(monkeypatch, tmp_path):
    repository = configure_test_runtime(monkeypatch, tmp_path, production=False)
    artifact_hash = 'a' * 64
    artifact = tmp_path / 'output' / 'artifacts' / 'job-range' / f'{artifact_hash}.mp4'
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b'0123456789')
    payload = {
        'sourceUrl': 'https://example.com/video.mp4',
        'start': 0,
        'end': 1,
        'contextMode': 'seconds',
        'padBefore': 0,
        'padAfter': 0,
        'renderProfile': 'hq-1080p-v1',
    }
    repository.create_or_get(new_job_record(
        id='job-range',
        tenant_id=TENANT_A,
        requested_by_user_id=USER_A,
        media_id=None,
        request_hash='1' * 64,
        idempotency_key=None,
        payload=payload,
        render_profile='hq-1080p-v1',
        status='queued',
    ))
    assert repository.claim('job-range', 'worker-a', 30) is not None
    repository.mark_ready(
        'job-range',
        'worker-a',
        stream_url='/clips/job-range/file',
        download_url='/clips/job-range/file?download=1',
        output_path=str(artifact),
        artifact_sha256=artifact_hash,
        artifact_bytes=10,
        validation={},
    )
    client = TestClient(main.app)

    ranged = client.get(
        '/clips/job-range/file',
        headers={**identity_headers(), 'Range': 'bytes=2-5'},
    )
    assert ranged.status_code == 206
    assert ranged.content == b'2345'
    assert ranged.headers['content-range'] == 'bytes 2-5/10'
    assert ranged.headers['accept-ranges'] == 'bytes'
    assert ranged.headers['etag'] == f'"{artifact_hash}"'

    suffix = client.get(
        '/clips/job-range/file',
        headers={**identity_headers(), 'Range': 'bytes=-3'},
    )
    assert suffix.status_code == 206
    assert suffix.content == b'789'

    invalid = client.get(
        '/clips/job-range/file',
        headers={**identity_headers(), 'Range': 'bytes=0-1,4-5'},
    )
    assert invalid.status_code == 416
    assert invalid.headers['content-range'] == 'bytes */10'

    cross_tenant = client.get(
        '/clips/job-range/file',
        headers={**identity_headers(TENANT_B), 'Range': 'bytes=0-1'},
    )
    assert cross_tenant.status_code == 404


def test_retention_cleanup_expires_metadata_and_only_unlinks_inside_output_root(
    monkeypatch,
    tmp_path,
):
    repository = configure_test_runtime(monkeypatch, tmp_path, production=False)
    monkeypatch.setattr(main.config, 'artifact_retention_seconds', 3600)
    output_root = tmp_path / 'output'
    artifact_hash = 'b' * 64
    artifact = output_root / 'artifacts' / 'job-retain' / f'{artifact_hash}.mp4'
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b'artifact')
    repository.create_or_get(new_job_record(
        id='job-retain',
        tenant_id=TENANT_A,
        requested_by_user_id=USER_A,
        media_id=None,
        request_hash='2' * 64,
        idempotency_key=None,
        payload={
            'sourceUrl': 'https://example.com/video.mp4',
            'start': 0,
            'end': 1,
            'contextMode': 'seconds',
            'padBefore': 0,
            'padAfter': 0,
            'renderProfile': 'hq-1080p-v1',
        },
        render_profile='hq-1080p-v1',
        status='queued',
    ))
    assert repository.claim('job-retain', 'worker-a', 30) is not None
    ready = repository.mark_ready(
        'job-retain',
        'worker-a',
        stream_url='/clips/job-retain/file',
        download_url='/clips/job-retain/file?download=1',
        output_path=str(artifact),
        artifact_sha256=artifact_hash,
        artifact_bytes=artifact.stat().st_size,
        validation={},
    )
    repository._jobs['job-retain'] = replace(  # pylint: disable=protected-access
        ready,
        updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )

    assert asyncio.run(main.run_retention_cleanup_once()) == (1, 1)
    assert not artifact.exists()
    expired = repository.get_for_tenant('job-retain', TENANT_A)
    assert expired is not None and expired.status == 'expired'
    client = TestClient(main.app)
    status_response = client.get('/clips/job-retain', headers=identity_headers())
    assert status_response.status_code == 200
    assert status_response.json()['status'] == 'expired'
    assert client.get(
        '/clips/job-retain/file',
        headers=identity_headers(),
    ).status_code == 404

    outside = tmp_path / 'outside.mp4'
    outside.write_bytes(b'do-not-delete')
    assert main.delete_expired_artifact(str(outside)) is False
    assert outside.read_bytes() == b'do-not-delete'
