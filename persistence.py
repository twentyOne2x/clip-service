"""Durable clip job, artifact, lease, and canonical-media persistence.

PostgreSQL is the production backend.  The in-memory implementation exists only
for local development and unit tests and deliberately exposes the same state
machine so behavior does not fork between environments.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol, Tuple

try:  # Loaded only when CLIP_DATABASE_URL selects PostgreSQL.
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - local dependency is optional
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore


UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(UTC)


class PersistenceError(RuntimeError):
    """Base class for persistence contract failures."""


class IdempotencyConflict(PersistenceError):
    """An idempotency key was reused with a different normalized request."""


class MediaNotFound(PersistenceError):
    """The tenant does not have access to the requested canonical media."""


class MediaConflict(PersistenceError):
    """A canonical media id was reused for different immutable bytes."""


class LeaseLost(PersistenceError):
    """A worker attempted to publish after losing its lease."""


@dataclass(frozen=True)
class MediaRecord:
    media_id: str
    source_path: str
    source_sha256: str
    video_capable: bool = True


@dataclass(frozen=True)
class JobRecord:
    id: str
    tenant_id: str
    requested_by_user_id: str
    media_id: Optional[str]
    request_hash: str
    idempotency_key: Optional[str]
    payload: Dict[str, Any]
    render_profile: str
    status: str
    attempt_count: int = 0
    max_attempts: int = 3
    available_at: datetime = field(default_factory=utcnow)
    lease_owner: Optional[str] = None
    lease_until: Optional[datetime] = None
    stream_url: Optional[str] = None
    download_url: Optional[str] = None
    output_path: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


def new_job_record(**values: Any) -> JobRecord:
    return JobRecord(**values)


class Repository(Protocol):
    durable: bool

    def ensure_schema(self) -> None: ...
    def register_media(self, media: MediaRecord, tenant_ids: Iterable[str]) -> None: ...
    def resolve_media(self, tenant_id: str, media_id: str) -> MediaRecord: ...
    def create_or_get(self, record: JobRecord) -> Tuple[JobRecord, bool]: ...
    def get_for_tenant(self, job_id: str, tenant_id: str) -> Optional[JobRecord]: ...
    def claim(self, job_id: str, owner: str, lease_seconds: int) -> Optional[JobRecord]: ...
    def claim_next(self, owner: str, lease_seconds: int) -> Optional[JobRecord]: ...
    def heartbeat(self, job_id: str, owner: str, lease_seconds: int) -> bool: ...
    def mark_ready(
        self,
        job_id: str,
        owner: str,
        *,
        stream_url: str,
        download_url: str,
        output_path: str,
        artifact_sha256: str,
        artifact_bytes: int,
        validation: Dict[str, Any],
    ) -> JobRecord: ...
    def mark_failure(
        self,
        job_id: str,
        owner: str,
        *,
        error_code: str,
        error_message: str,
        retryable: bool,
        retry_delay_seconds: int,
    ) -> JobRecord: ...
    def expire_ready_before(self, cutoff: datetime, limit: int) -> list[str]: ...


class MemoryRepository:
    durable = False

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._media: Dict[str, MediaRecord] = {}
        self._media_access: set[tuple[str, str]] = set()
        self._jobs: Dict[str, JobRecord] = {}
        self._request_index: Dict[tuple[str, str], str] = {}
        self._idempotency_index: Dict[tuple[str, str, str], str] = {}
        self._artifacts: Dict[str, Dict[str, Any]] = {}

    def ensure_schema(self) -> None:
        return

    def register_media(self, media: MediaRecord, tenant_ids: Iterable[str]) -> None:
        with self._lock:
            existing = self._media.get(media.media_id)
            if existing is not None and existing != media:
                raise MediaConflict("canonical media id already names different bytes or source")
            self._media[media.media_id] = media
            for tenant_id in tenant_ids:
                self._media_access.add((tenant_id, media.media_id))

    def resolve_media(self, tenant_id: str, media_id: str) -> MediaRecord:
        with self._lock:
            if (tenant_id, media_id) not in self._media_access:
                raise MediaNotFound("canonical media is unavailable for this tenant")
            media = self._media.get(media_id)
            if media is None:
                raise MediaNotFound("canonical media is unavailable for this tenant")
            return media

    def create_or_get(self, record: JobRecord) -> Tuple[JobRecord, bool]:
        with self._lock:
            if record.idempotency_key:
                key = (record.tenant_id, record.requested_by_user_id, record.idempotency_key)
                existing_id = self._idempotency_index.get(key)
                if existing_id:
                    existing = self._jobs[existing_id]
                    if existing.request_hash != record.request_hash:
                        raise IdempotencyConflict("idempotency key was reused with a different request")
                    return existing, False
            request_key = (record.tenant_id, record.request_hash)
            existing_id = self._request_index.get(request_key)
            if existing_id:
                if record.idempotency_key:
                    self._idempotency_index[
                        (record.tenant_id, record.requested_by_user_id, record.idempotency_key)
                    ] = existing_id
                return self._jobs[existing_id], False
            self._jobs[record.id] = record
            self._request_index[request_key] = record.id
            if record.idempotency_key:
                self._idempotency_index[
                    (record.tenant_id, record.requested_by_user_id, record.idempotency_key)
                ] = record.id
            return record, True

    def get_for_tenant(self, job_id: str, tenant_id: str) -> Optional[JobRecord]:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None or record.tenant_id != tenant_id:
                return None
            return record

    def _claim_locked(self, record: JobRecord, owner: str, lease_seconds: int) -> Optional[JobRecord]:
        now = utcnow()
        claimable = record.status == "queued" and record.available_at <= now
        expired = record.status == "processing" and record.lease_until is not None and record.lease_until <= now
        if not (claimable or expired):
            return None
        if record.attempt_count >= record.max_attempts:
            if expired:
                self._jobs[record.id] = replace(
                    record,
                    status="error",
                    lease_owner=None,
                    lease_until=None,
                    error_code="attempts_exhausted",
                    error_message="worker lease expired after the final allowed attempt",
                    updated_at=now,
                )
            return None
        claimed = replace(
            record,
            status="processing",
            attempt_count=record.attempt_count + 1,
            lease_owner=owner,
            lease_until=now + timedelta(seconds=lease_seconds),
            error_code=None,
            error_message=None,
            updated_at=now,
        )
        self._jobs[record.id] = claimed
        return claimed

    def claim(self, job_id: str, owner: str, lease_seconds: int) -> Optional[JobRecord]:
        with self._lock:
            record = self._jobs.get(job_id)
            return self._claim_locked(record, owner, lease_seconds) if record else None

    def claim_next(self, owner: str, lease_seconds: int) -> Optional[JobRecord]:
        with self._lock:
            for record in sorted(self._jobs.values(), key=lambda item: (item.available_at, item.created_at, item.id)):
                claimed = self._claim_locked(record, owner, lease_seconds)
                if claimed:
                    return claimed
            return None

    def heartbeat(self, job_id: str, owner: str, lease_seconds: int) -> bool:
        with self._lock:
            record = self._jobs.get(job_id)
            now = utcnow()
            if (
                record is None
                or record.status != "processing"
                or record.lease_owner != owner
                or record.lease_until is None
                or record.lease_until <= now
            ):
                return False
            self._jobs[job_id] = replace(
                record,
                lease_until=now + timedelta(seconds=lease_seconds),
                updated_at=now,
            )
            return True

    def mark_ready(
        self,
        job_id: str,
        owner: str,
        *,
        stream_url: str,
        download_url: str,
        output_path: str,
        artifact_sha256: str,
        artifact_bytes: int,
        validation: Dict[str, Any],
    ) -> JobRecord:
        with self._lock:
            record = self._jobs.get(job_id)
            now = utcnow()
            if (
                record is None
                or record.status != "processing"
                or record.lease_owner != owner
                or record.lease_until is None
                or record.lease_until <= now
            ):
                raise LeaseLost("cannot publish a clip without the active worker lease")
            ready = replace(
                record,
                status="ready",
                stream_url=stream_url,
                download_url=download_url,
                output_path=output_path,
                lease_owner=None,
                lease_until=None,
                error_code=None,
                error_message=None,
                updated_at=now,
            )
            self._artifacts[job_id] = {
                "sha256": artifact_sha256,
                "bytes": artifact_bytes,
                "output_path": output_path,
                "validation": dict(validation),
            }
            self._jobs[job_id] = ready
            return ready

    def mark_failure(
        self,
        job_id: str,
        owner: str,
        *,
        error_code: str,
        error_message: str,
        retryable: bool,
        retry_delay_seconds: int,
    ) -> JobRecord:
        with self._lock:
            record = self._jobs.get(job_id)
            now = utcnow()
            if (
                record is None
                or record.status != "processing"
                or record.lease_owner != owner
                or record.lease_until is None
                or record.lease_until <= now
            ):
                raise LeaseLost("cannot fail a clip without the active worker lease")
            will_retry = retryable and record.attempt_count < record.max_attempts
            failed = replace(
                record,
                status="queued" if will_retry else "error",
                available_at=now + timedelta(seconds=max(0, retry_delay_seconds)) if will_retry else now,
                lease_owner=None,
                lease_until=None,
                error_code=error_code,
                error_message=error_message[:2000],
                updated_at=now,
            )
            self._jobs[job_id] = failed
            return failed

    def expire_ready_before(self, cutoff: datetime, limit: int) -> list[str]:
        with self._lock:
            candidates = sorted(
                (
                    record for record in self._jobs.values()
                    if record.status == "ready" and record.updated_at < cutoff
                ),
                key=lambda item: (item.updated_at, item.id),
            )[:max(0, limit)]
            output_paths: list[str] = []
            now = utcnow()
            for record in candidates:
                artifact = self._artifacts.pop(record.id, None)
                if artifact and artifact.get("output_path"):
                    output_paths.append(str(artifact["output_path"]))
                self._jobs[record.id] = replace(
                    record,
                    status="expired",
                    stream_url=None,
                    download_url=None,
                    output_path=None,
                    error_code="artifact_expired",
                    error_message="clip artifact expired under the configured retention policy",
                    updated_at=now,
                )
            return output_paths


def _record_from_row(row: Dict[str, Any]) -> JobRecord:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return JobRecord(
        id=row["id"],
        tenant_id=row["tenant_id"],
        requested_by_user_id=row["requested_by_user_id"],
        media_id=row.get("media_id"),
        request_hash=row["request_hash"],
        idempotency_key=row.get("idempotency_key"),
        payload=dict(payload),
        render_profile=row["render_profile"],
        status=row["status"],
        attempt_count=int(row["attempt_count"]),
        max_attempts=int(row["max_attempts"]),
        available_at=row["available_at"],
        lease_owner=row.get("lease_owner"),
        lease_until=row.get("lease_until"),
        stream_url=row.get("stream_url"),
        download_url=row.get("download_url"),
        output_path=row.get("output_path"),
        error_code=row.get("error_code"),
        error_message=row.get("error_message"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class PostgresRepository:
    durable = True

    def __init__(self, database_url: str, migration_path: Optional[Path] = None) -> None:
        if psycopg is None:
            raise PersistenceError("psycopg[binary] is required when CLIP_DATABASE_URL is configured")
        self.database_url = database_url
        self.migration_path = migration_path or Path(__file__).parent / "migrations" / "001_clip_jobs.sql"

    def _connect(self):
        return psycopg.connect(self.database_url, row_factory=dict_row)

    def ensure_schema(self) -> None:
        sql = self.migration_path.read_text(encoding="utf-8")
        with self._connect() as connection:
            connection.execute(sql)

    def register_media(self, media: MediaRecord, tenant_ids: Iterable[str]) -> None:
        del media, tenant_ids
        raise PersistenceError(
            "canonical media is owned by ingestion and cannot be registered through the clip database adapter"
        )

    def resolve_media(self, tenant_id: str, media_id: str) -> MediaRecord:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    v.id AS media_id,
                    location.location_key AS source_path,
                    media.sha256 AS source_sha256,
                    TRUE AS video_capable
                FROM tenant_channel_entitlements AS entitlement
                JOIN source_channels AS channel
                  ON channel.id = entitlement.channel_id
                 AND channel.status = 'active'
                JOIN source_videos AS v
                  ON v.channel_id = channel.id
                 AND v.status = 'active'
                JOIN video_media_refs AS media_ref
                  ON media_ref.video_id = v.id
                 AND media_ref.status = 'active'
                 AND media_ref.role IN ('source_video', 'proxy')
                JOIN media_objects AS media
                  ON media.sha256 = media_ref.media_sha256
                 AND media.status = 'active'
                 AND media.mime_type LIKE 'video/%%'
                JOIN media_locations AS location
                  ON location.media_sha256 = media.sha256
                 AND location.backend = 'hot_local'
                 AND location.status = 'active'
                 AND location.verified_at IS NOT NULL
                 AND location.bytes = media.size_bytes
                WHERE entitlement.tenant_id = %s
                  AND entitlement.status = 'active'
                  AND v.id = %s
                ORDER BY
                    CASE media_ref.role WHEN 'source_video' THEN 0 ELSE 1 END,
                    location.id
                LIMIT 1
                """,
                (tenant_id, media_id),
            ).fetchone()
        if not row:
            raise MediaNotFound("canonical media is unavailable for this tenant")
        return MediaRecord(**row)

    def create_or_get(self, record: JobRecord) -> Tuple[JobRecord, bool]:
        with self._connect() as connection:
            if record.idempotency_key:
                existing = connection.execute(
                    """
                    SELECT j.* FROM clip_job_idempotency AS i
                    JOIN clip_jobs AS j ON j.id = i.job_id AND j.tenant_id = i.tenant_id
                    WHERE i.tenant_id = %s AND i.requested_by_user_id = %s AND i.idempotency_key = %s
                    FOR UPDATE OF i
                    """,
                    (record.tenant_id, record.requested_by_user_id, record.idempotency_key),
                ).fetchone()
                if existing:
                    existing_record = _record_from_row(existing)
                    if existing_record.request_hash != record.request_hash:
                        raise IdempotencyConflict("idempotency key was reused with a different request")
                    return existing_record, False
            row = connection.execute(
                """
                INSERT INTO clip_jobs (
                    id, tenant_id, requested_by_user_id, media_id, request_hash, idempotency_key, payload,
                    render_profile, status, max_attempts, available_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, 'queued', %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING *
                """,
                (
                    record.id,
                    record.tenant_id,
                    record.requested_by_user_id,
                    record.media_id,
                    record.request_hash,
                    record.idempotency_key,
                    json.dumps(record.payload, sort_keys=True, separators=(",", ":")),
                    record.render_profile,
                    record.max_attempts,
                    record.available_at,
                ),
            ).fetchone()
            created = row is not None
            if not row:
                row = connection.execute(
                    "SELECT * FROM clip_jobs WHERE tenant_id = %s AND request_hash = %s FOR UPDATE",
                    (record.tenant_id, record.request_hash),
                ).fetchone()
            if not row:
                raise PersistenceError("clip job uniqueness conflict could not be reconciled")
            if record.idempotency_key:
                inserted_binding = connection.execute(
                    """
                    INSERT INTO clip_job_idempotency (
                        tenant_id, requested_by_user_id, idempotency_key, request_hash, job_id
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING job_id
                    """,
                    (
                        record.tenant_id,
                        record.requested_by_user_id,
                        record.idempotency_key,
                        record.request_hash,
                        row["id"],
                    ),
                ).fetchone()
                if not inserted_binding:
                    binding = connection.execute(
                        """
                        SELECT request_hash, job_id FROM clip_job_idempotency
                        WHERE tenant_id = %s AND requested_by_user_id = %s AND idempotency_key = %s
                        FOR UPDATE
                        """,
                        (record.tenant_id, record.requested_by_user_id, record.idempotency_key),
                    ).fetchone()
                    if not binding or binding["request_hash"] != record.request_hash:
                        raise IdempotencyConflict("idempotency key was reused with a different request")
                    if binding["job_id"] != row["id"]:
                        raise PersistenceError("idempotency binding disagrees with canonical request dedupe")
            return _record_from_row(row), created

    def get_for_tenant(self, job_id: str, tenant_id: str) -> Optional[JobRecord]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM clip_jobs WHERE id = %s AND tenant_id = %s",
                (job_id, tenant_id),
            ).fetchone()
        return _record_from_row(row) if row else None

    def _claim_where(self, where_sql: str, params: tuple[Any, ...], owner: str, lease_seconds: int) -> Optional[JobRecord]:
        with self._connect() as connection:
            connection.execute(
                f"""
                UPDATE clip_jobs
                SET status = 'error', lease_owner = NULL, lease_until = NULL,
                    error_code = 'attempts_exhausted',
                    error_message = 'worker lease expired after the final allowed attempt',
                    updated_at = NOW()
                WHERE {where_sql}
                  AND status = 'processing'
                  AND lease_until <= NOW()
                  AND attempt_count >= max_attempts
                """,
                params,
            )
            row = connection.execute(
                f"""
                WITH candidate AS (
                    SELECT id FROM clip_jobs
                    WHERE {where_sql}
                      AND attempt_count < max_attempts
                      AND (
                        (status = 'queued' AND available_at <= NOW())
                        OR (status = 'processing' AND lease_until <= NOW())
                      )
                    ORDER BY available_at, created_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE clip_jobs AS j
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    lease_owner = %s,
                    lease_until = NOW() + (%s * INTERVAL '1 second'),
                    error_code = NULL,
                    error_message = NULL,
                    updated_at = NOW()
                FROM candidate
                WHERE j.id = candidate.id
                RETURNING j.*
                """,
                (*params, owner, lease_seconds),
            ).fetchone()
        return _record_from_row(row) if row else None

    def claim(self, job_id: str, owner: str, lease_seconds: int) -> Optional[JobRecord]:
        return self._claim_where("id = %s", (job_id,), owner, lease_seconds)

    def claim_next(self, owner: str, lease_seconds: int) -> Optional[JobRecord]:
        return self._claim_where("TRUE", (), owner, lease_seconds)

    def heartbeat(self, job_id: str, owner: str, lease_seconds: int) -> bool:
        with self._connect() as connection:
            result = connection.execute(
                """
                UPDATE clip_jobs
                SET lease_until = NOW() + (%s * INTERVAL '1 second'), updated_at = NOW()
                WHERE id = %s AND status = 'processing' AND lease_owner = %s
                  AND lease_until > NOW()
                """,
                (lease_seconds, job_id, owner),
            )
            return result.rowcount == 1

    def mark_ready(
        self,
        job_id: str,
        owner: str,
        *,
        stream_url: str,
        download_url: str,
        output_path: str,
        artifact_sha256: str,
        artifact_bytes: int,
        validation: Dict[str, Any],
    ) -> JobRecord:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT tenant_id FROM clip_jobs
                WHERE id = %s AND status = 'processing' AND lease_owner = %s
                  AND lease_until > NOW()
                FOR UPDATE
                """,
                (job_id, owner),
            ).fetchone()
            if not row:
                raise LeaseLost("cannot publish a clip without the active worker lease")
            connection.execute(
                """
                INSERT INTO clip_artifacts (job_id, tenant_id, sha256, bytes, output_path, validation)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (job_id) DO UPDATE SET
                    sha256 = EXCLUDED.sha256,
                    bytes = EXCLUDED.bytes,
                    output_path = EXCLUDED.output_path,
                    validation = EXCLUDED.validation
                """,
                (job_id, row["tenant_id"], artifact_sha256, artifact_bytes, output_path, json.dumps(validation)),
            )
            updated = connection.execute(
                """
                UPDATE clip_jobs
                SET status = 'ready', stream_url = %s, download_url = %s,
                    output_path = %s, lease_owner = NULL, lease_until = NULL,
                    error_code = NULL, error_message = NULL, updated_at = NOW()
                WHERE id = %s AND lease_owner = %s AND lease_until > NOW()
                RETURNING *
                """,
                (stream_url, download_url, output_path, job_id, owner),
            ).fetchone()
            if not updated:
                raise LeaseLost("worker lease changed during artifact publication")
            return _record_from_row(updated)

    def mark_failure(
        self,
        job_id: str,
        owner: str,
        *,
        error_code: str,
        error_message: str,
        retryable: bool,
        retry_delay_seconds: int,
    ) -> JobRecord:
        with self._connect() as connection:
            row = connection.execute(
                """
                UPDATE clip_jobs
                SET status = CASE WHEN %s AND attempt_count < max_attempts THEN 'queued' ELSE 'error' END,
                    available_at = CASE
                        WHEN %s AND attempt_count < max_attempts
                        THEN NOW() + (%s * INTERVAL '1 second')
                        ELSE NOW()
                    END,
                    lease_owner = NULL,
                    lease_until = NULL,
                    error_code = %s,
                    error_message = %s,
                    updated_at = NOW()
                WHERE id = %s AND status = 'processing' AND lease_owner = %s
                  AND lease_until > NOW()
                RETURNING *
                """,
                (
                    retryable,
                    retryable,
                    max(0, retry_delay_seconds),
                    error_code,
                    error_message[:2000],
                    job_id,
                    owner,
                ),
            ).fetchone()
        if not row:
            raise LeaseLost("cannot fail a clip without the active worker lease")
        return _record_from_row(row)

    def expire_ready_before(self, cutoff: datetime, limit: int) -> list[str]:
        with self._connect() as connection:
            candidates = connection.execute(
                """
                SELECT j.id, a.output_path
                FROM clip_jobs AS j
                JOIN clip_artifacts AS a ON a.job_id = j.id AND a.tenant_id = j.tenant_id
                WHERE j.status = 'ready' AND j.updated_at < %s
                ORDER BY j.updated_at, j.id
                FOR UPDATE OF j SKIP LOCKED
                LIMIT %s
                """,
                (cutoff, max(0, limit)),
            ).fetchall()
            if not candidates:
                return []
            job_ids = [row["id"] for row in candidates]
            connection.execute(
                "DELETE FROM clip_artifacts WHERE job_id = ANY(%s)",
                (job_ids,),
            )
            connection.execute(
                """
                UPDATE clip_jobs
                SET status = 'expired', stream_url = NULL, download_url = NULL,
                    output_path = NULL, error_code = 'artifact_expired',
                    error_message = 'clip artifact expired under the configured retention policy',
                    updated_at = NOW()
                WHERE id = ANY(%s) AND status = 'ready'
                """,
                (job_ids,),
            )
            return [str(row["output_path"]) for row in candidates]


def create_repository(database_url: Optional[str]) -> Repository:
    if database_url:
        normalized = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        if not normalized.startswith(("postgresql://", "postgres://")):
            raise PersistenceError("CLIP_DATABASE_URL must use PostgreSQL")
        return PostgresRepository(normalized)
    return MemoryRepository()
