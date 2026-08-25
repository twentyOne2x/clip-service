from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from persistence import (
    IdempotencyConflict,
    JobRecord,
    LeaseLost,
    MediaConflict,
    MediaNotFound,
    MediaRecord,
    MemoryRepository,
)


def record(**overrides):
    values = {
        'id': 'job-1',
        'tenant_id': 'tenant-a',
        'requested_by_user_id': 'user-a',
        'media_id': 'media-1',
        'request_hash': '1' * 64,
        'idempotency_key': 'request-1',
        'payload': {'mediaId': 'media-1'},
        'render_profile': 'hq-1080p-v1',
        'status': 'queued',
    }
    values.update(overrides)
    return JobRecord(**values)


def test_job_record_timestamp_defaults_are_real_utc_datetimes():
    created = record()

    assert isinstance(created.available_at, datetime)
    assert isinstance(created.created_at, datetime)
    assert isinstance(created.updated_at, datetime)
    assert created.available_at.tzinfo == timezone.utc


def test_canonical_media_is_immutable_and_tenant_scoped():
    repository = MemoryRepository()
    media = MediaRecord('media-1', '/media/source.mp4', 'a' * 64, True)
    repository.register_media(media, ['tenant-a'])

    assert repository.resolve_media('tenant-a', 'media-1') == media
    with pytest.raises(MediaNotFound):
        repository.resolve_media('tenant-b', 'media-1')
    with pytest.raises(MediaConflict):
        repository.register_media(
            MediaRecord('media-1', '/media/replaced.mp4', 'b' * 64, True),
            ['tenant-a'],
        )


def test_idempotency_conflict_is_user_scoped_while_request_dedupe_is_tenant_scoped():
    repository = MemoryRepository()
    first, created = repository.create_or_get(record())
    assert created is True

    duplicate, created = repository.create_or_get(record(id='job-2'))
    assert created is False
    assert duplicate.id == first.id

    with pytest.raises(IdempotencyConflict):
        repository.create_or_get(record(id='job-3', request_hash='2' * 64))

    other_user, created = repository.create_or_get(
        record(
            id='job-4',
            requested_by_user_id='user-b',
            request_hash='3' * 64,
        )
    )
    assert created is True
    assert other_user.id == 'job-4'

    alias, created = repository.create_or_get(
        record(id='job-5', idempotency_key='request-alias')
    )
    assert created is False
    assert alias.id == first.id
    with pytest.raises(IdempotencyConflict):
        repository.create_or_get(
            record(id='job-6', request_hash='4' * 64, idempotency_key='request-alias')
        )


def test_expired_lease_cannot_publish_and_last_attempt_becomes_terminal():
    repository = MemoryRepository()
    repository.create_or_get(record(max_attempts=1))
    claimed = repository.claim('job-1', 'worker-a', 30)
    assert claimed is not None
    repository._jobs['job-1'] = replace(  # pylint: disable=protected-access
        claimed,
        lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    with pytest.raises(LeaseLost):
        repository.mark_ready(
            'job-1',
            'worker-a',
            stream_url='/clips/job-1/file',
            download_url='/clips/job-1/file?download=1',
            output_path='/output/job-1.mp4',
            artifact_sha256='f' * 64,
            artifact_bytes=10,
            validation={},
        )

    assert repository.claim('job-1', 'worker-b', 30) is None
    exhausted = repository.get_for_tenant('job-1', 'tenant-a')
    assert exhausted is not None
    assert exhausted.status == 'error'
    assert exhausted.error_code == 'attempts_exhausted'


def test_ready_artifact_and_job_transition_publish_together():
    repository = MemoryRepository()
    repository.create_or_get(record())
    claimed = repository.claim('job-1', 'worker-a', 30)
    assert claimed is not None

    ready = repository.mark_ready(
        'job-1',
        'worker-a',
        stream_url='/clips/job-1/file',
        download_url='/clips/job-1/file?download=1',
        output_path='/output/job-1.mp4',
        artifact_sha256='f' * 64,
        artifact_bytes=10,
        validation={'duration_seconds': 1.0},
    )

    assert ready.status == 'ready'
    assert repository._artifacts['job-1']['sha256'] == 'f' * 64  # pylint: disable=protected-access
    with pytest.raises(LeaseLost):
        repository.mark_failure(
            'job-1',
            'worker-a',
            error_code='late',
            error_message='late worker',
            retryable=False,
            retry_delay_seconds=0,
        )


def test_retention_expires_ready_job_and_detaches_artifact_atomically():
    repository = MemoryRepository()
    repository.create_or_get(record())
    assert repository.claim('job-1', 'worker-a', 30) is not None
    ready = repository.mark_ready(
        'job-1',
        'worker-a',
        stream_url='/clips/job-1/file',
        download_url='/clips/job-1/file?download=1',
        output_path='/output/job-1.mp4',
        artifact_sha256='f' * 64,
        artifact_bytes=10,
        validation={'duration_seconds': 1.0},
    )
    repository._jobs['job-1'] = replace(  # pylint: disable=protected-access
        ready,
        updated_at=datetime.now(timezone.utc) - timedelta(days=2),
    )

    paths = repository.expire_ready_before(
        datetime.now(timezone.utc) - timedelta(days=1),
        100,
    )

    assert paths == ['/output/job-1.mp4']
    expired = repository.get_for_tenant('job-1', 'tenant-a')
    assert expired is not None
    assert expired.status == 'expired'
    assert expired.stream_url is None
    assert expired.download_url is None
    assert expired.output_path is None
    assert expired.error_code == 'artifact_expired'
    assert 'job-1' not in repository._artifacts  # pylint: disable=protected-access
    assert repository.expire_ready_before(datetime.now(timezone.utc), 100) == []
