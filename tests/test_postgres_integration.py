import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from persistence import (
    IdempotencyConflict,
    MediaNotFound,
    PostgresRepository,
    new_job_record,
)


DATABASE_URL = os.getenv('CLIP_TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason='CLIP_TEST_DATABASE_URL is not configured')
TENANT_A = f"ten_{'a' * 64}"
TENANT_B = f"ten_{'b' * 64}"
USER_A = f"usr_{'a' * 64}"


def job(job_id: str, request_hash: str, idempotency_key: str | None = None):
    return new_job_record(
        id=job_id,
        tenant_id=TENANT_A,
        requested_by_user_id=USER_A,
        media_id='media-1',
        request_hash=request_hash,
        idempotency_key=idempotency_key,
        payload={
            'mediaId': 'media-1',
            'start': 0,
            'end': 1,
            'contextMode': 'seconds',
            'padBefore': 0,
            'padAfter': 0,
        },
        render_profile='hq-1080p-v1',
        status='queued',
    )


def test_postgres_schema_tenant_dedupe_lease_and_atomic_artifact_contract():
    assert DATABASE_URL is not None
    repository = PostgresRepository(DATABASE_URL)
    with repository._connect() as connection:  # pylint: disable=protected-access
        connection.execute(
            """
            CREATE TABLE source_channels (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL
            );
            CREATE TABLE tenant_channel_entitlements (
                tenant_id TEXT NOT NULL,
                channel_id TEXT NOT NULL REFERENCES source_channels(id),
                status TEXT NOT NULL,
                PRIMARY KEY (tenant_id, channel_id)
            );
            CREATE TABLE source_videos (
                id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL REFERENCES source_channels(id),
                status TEXT NOT NULL
            );
            CREATE TABLE media_objects (
                sha256 TEXT PRIMARY KEY,
                size_bytes BIGINT NOT NULL,
                mime_type TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE video_media_refs (
                id BIGSERIAL PRIMARY KEY,
                video_id TEXT NOT NULL REFERENCES source_videos(id),
                media_sha256 TEXT NOT NULL REFERENCES media_objects(sha256),
                role TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE media_locations (
                id BIGSERIAL PRIMARY KEY,
                media_sha256 TEXT NOT NULL REFERENCES media_objects(sha256),
                backend TEXT NOT NULL,
                location_key TEXT NOT NULL,
                status TEXT NOT NULL,
                bytes BIGINT NOT NULL,
                verified_at TIMESTAMPTZ
            );
            """
        )
        connection.execute(
            "INSERT INTO source_channels (id, status) VALUES ('channel-1', 'active')"
        )
        connection.execute(
            """
            INSERT INTO tenant_channel_entitlements (tenant_id, channel_id, status)
            VALUES (%s, 'channel-1', 'active')
            """,
            (TENANT_A,),
        )
        connection.execute(
            "INSERT INTO source_videos (id, channel_id, status) VALUES ('media-1', 'channel-1', 'active')"
        )
        connection.execute(
            """
            INSERT INTO media_objects (sha256, size_bytes, mime_type, status)
            VALUES (%s, 1024, 'video/mp4', 'active'), (%s, 512, 'video/mp4', 'active')
            """,
            ('a' * 64, 'b' * 64),
        )
        connection.execute(
            """
            INSERT INTO video_media_refs (video_id, media_sha256, role, status)
            VALUES ('media-1', %s, 'proxy', 'active'), ('media-1', %s, 'source_video', 'active')
            """,
            ('b' * 64, 'a' * 64),
        )
        connection.execute(
            """
            INSERT INTO media_locations (
                media_sha256, backend, location_key, status, bytes, verified_at
            ) VALUES
                (%s, 'hot_local', '/srv/icmfyi/media/proxy.mp4', 'active', 512, NOW()),
                (%s, 'hot_local', '/srv/icmfyi/media/source.mp4', 'active', 1024, NOW())
            """,
            ('b' * 64, 'a' * 64),
        )
    repository.ensure_schema()
    media = repository.resolve_media(TENANT_A, 'media-1')
    assert media.source_path == '/srv/icmfyi/media/source.mp4'
    assert media.source_sha256 == 'a' * 64
    with pytest.raises(MediaNotFound):
        repository.resolve_media(TENANT_B, 'media-1')

    candidates = [job(f'job-{index}', '1' * 64) for index in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(repository.create_or_get, candidates))
    assert sum(1 for _, created in results if created) == 1
    canonical_id = {record.id for record, _ in results}
    assert len(canonical_id) == 1

    repository.create_or_get(job('idempotent-a', '2' * 64, 'request-2'))
    with pytest.raises(IdempotencyConflict):
        repository.create_or_get(job('idempotent-b', '3' * 64, 'request-2'))

    claimed = repository.claim(next(iter(canonical_id)), 'worker-a', 30)
    assert claimed is not None
    ready = repository.mark_ready(
        claimed.id,
        'worker-a',
        stream_url=f'/clips/{claimed.id}/file',
        download_url=f'/clips/{claimed.id}/file?download=1',
        output_path=f'/var/lib/icmfyi/clips/{claimed.id}.mp4',
        artifact_sha256='f' * 64,
        artifact_bytes=1024,
        validation={'duration_seconds': 1.0},
    )
    assert ready.status == 'ready'

    with repository._connect() as connection:  # pylint: disable=protected-access
        artifact = connection.execute(
            'SELECT tenant_id, sha256, bytes FROM clip_artifacts WHERE job_id = %s',
            (claimed.id,),
        ).fetchone()
    assert artifact == {'tenant_id': TENANT_A, 'sha256': 'f' * 64, 'bytes': 1024}

    with repository._connect() as connection:  # pylint: disable=protected-access
        connection.execute(
            "UPDATE clip_jobs SET updated_at = NOW() - INTERVAL '2 days' WHERE id = %s",
            (claimed.id,),
        )
    expired_paths = repository.expire_ready_before(
        datetime.now(timezone.utc) - timedelta(days=1),
        100,
    )
    assert expired_paths == [f'/var/lib/icmfyi/clips/{claimed.id}.mp4']
    expired = repository.get_for_tenant(claimed.id, TENANT_A)
    assert expired is not None
    assert expired.status == 'expired'
    assert expired.output_path is None
    assert expired.stream_url is None
    with repository._connect() as connection:  # pylint: disable=protected-access
        assert connection.execute(
            'SELECT 1 FROM clip_artifacts WHERE job_id = %s',
            (claimed.id,),
        ).fetchone() is None
