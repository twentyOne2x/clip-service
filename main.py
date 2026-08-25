import asyncio
import contextlib
import hashlib
import json
import math
import os
import re
import secrets
import signal
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from urllib.request import Request as UrlRequest, urlopen
from uuid import uuid4

from fastapi import BackgroundTasks, Body, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
try:
    from google.cloud import storage  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for GCS downloads
    storage = None  # type: ignore
from pydantic import BaseModel, Field, HttpUrl, root_validator, validator
try:
    from yt_dlp import YoutubeDL  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for web downloads
    YoutubeDL = None  # type: ignore

from persistence import (
    IdempotencyConflict,
    JobRecord,
    LeaseLost,
    MediaConflict,
    MediaNotFound,
    MediaRecord,
    PersistenceError,
    Repository,
    create_repository,
    new_job_record,
)


ClipStatus = Literal['queued', 'processing', 'ready', 'error', 'expired']
ContextMode = Literal['seconds', 'sentence']
RenderProfile = Literal['hq-1080p-v1']

TENANT_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')
USER_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,191}$')
PRODUCTION_TENANT_ID_RE = re.compile(r'^ten_[0-9a-f]{64}$')
PRODUCTION_USER_ID_RE = re.compile(r'^usr_[0-9a-f]{64}$')
MEDIA_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$')


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {'1', 'true', 'yes', 'on'}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = (os.getenv(name) or '').strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return min(maximum, max(minimum, value))


def is_production_environment() -> bool:
    return (os.getenv('CLIP_SERVICE_ENV') or os.getenv('ICMFYI_ENV') or '').strip().lower() in {
        'prod',
        'production',
    }


def max_clip_seconds() -> int:
    return _env_int('CLIP_MAX_CLIP_SECONDS', 600, minimum=1, maximum=1800)


def max_padding_seconds() -> int:
    return _env_int('CLIP_MAX_PADDING_SECONDS', 30, minimum=0, maximum=120)


def max_source_position_seconds() -> int:
    return _env_int('CLIP_MAX_SOURCE_POSITION_SECONDS', 43200, minimum=60, maximum=604800)


class ClipRequest(BaseModel):
    mediaId: Optional[str] = Field(None, alias='mediaId', min_length=1, max_length=192)
    sourceUrl: Optional[HttpUrl] = Field(None, alias='sourceUrl')
    parentTitle: Optional[str] = Field(None, max_length=500)
    clipLabel: Optional[str] = Field(None, max_length=500)
    channel: Optional[str] = Field(None, max_length=256)
    # If true, clip from real video (frames) instead of local mirrored audio (thumbnail-backed MP4).
    preferVideo: bool = Field(default=False, alias='preferVideo')
    start: float = Field(..., ge=0)
    end: float = Field(..., gt=0)
    contextMode: ContextMode = Field(..., alias='contextMode')
    padBefore: float = Field(..., ge=0, alias='padBefore')
    padAfter: float = Field(..., ge=0, alias='padAfter')
    renderProfile: RenderProfile = Field(default='hq-1080p-v1', alias='renderProfile')

    @validator('end', allow_reuse=True)
    def validate_end(cls, value: float, values: Dict[str, float]) -> float:  # pylint: disable=no-self-argument
        start = values.get('start')
        if start is not None and value <= start:
            raise ValueError('end must be greater than start')
        return value

    @validator('mediaId', allow_reuse=True)
    def validate_media_id(cls, value: Optional[str]) -> Optional[str]:  # pylint: disable=no-self-argument
        if value is not None and not MEDIA_ID_RE.fullmatch(value):
            raise ValueError('mediaId has invalid characters')
        return value

    @root_validator(allow_reuse=True)
    def validate_render_bounds(cls, values: Dict[str, Any]) -> Dict[str, Any]:  # pylint: disable=no-self-argument
        start = values.get('start')
        end = values.get('end')
        pad_before = values.get('padBefore')
        pad_after = values.get('padAfter')
        numbers = [value for value in (start, end, pad_before, pad_after) if value is not None]
        if any(not math.isfinite(float(value)) for value in numbers):
            raise ValueError('render timestamps and padding must be finite')
        if not values.get('mediaId') and not values.get('sourceUrl'):
            raise ValueError('mediaId or sourceUrl is required')
        if start is not None and start > max_source_position_seconds():
            raise ValueError('start exceeds the configured source-position limit')
        if end is not None and end > max_source_position_seconds():
            raise ValueError('end exceeds the configured source-position limit')
        if start is not None and end is not None and (end - start) > max_clip_seconds():
            raise ValueError('clip duration exceeds the configured limit')
        if pad_before is not None and pad_before > max_padding_seconds():
            raise ValueError('padBefore exceeds the configured limit')
        if pad_after is not None and pad_after > max_padding_seconds():
            raise ValueError('padAfter exceeds the configured limit')
        if all(value is not None for value in (start, end, pad_before, pad_after)):
            if (end - start + pad_before + pad_after) > (max_clip_seconds() + 2 * max_padding_seconds()):
                raise ValueError('total render window exceeds the configured limit')
        return values

    class Config:
        allow_population_by_field_name = True


class ClipResponse(BaseModel):
    clipId: str
    status: ClipStatus
    streamUrl: Optional[str] = None
    downloadUrl: Optional[str] = None
    errorMessage: Optional[str] = None
    requestPayload: ClipRequest
    lastUpdated: datetime


class MediaRegistrationRequest(BaseModel):
    mediaId: str = Field(..., alias='mediaId', min_length=1, max_length=192)
    sourcePath: str = Field(..., alias='sourcePath', min_length=1, max_length=4096)
    sourceSha256: str = Field(..., alias='sourceSha256', regex=r'^[0-9a-f]{64}$')
    videoCapable: bool = Field(default=True, alias='videoCapable')

    @validator('mediaId', allow_reuse=True)
    def validate_media_id(cls, value: str) -> str:  # pylint: disable=no-self-argument
        if not MEDIA_ID_RE.fullmatch(value):
            raise ValueError('mediaId has invalid characters')
        return value

    class Config:
        allow_population_by_field_name = True


class MediaRegistrationResponse(BaseModel):
    mediaId: str
    tenantId: str
    sourceSha256: str
    videoCapable: bool


@dataclass(frozen=True)
class RequestContext:
    tenant_id: str
    user_id: str


class BundleClipRequest(BaseModel):
    key: str
    mediaId: Optional[str] = Field(None, alias='mediaId', min_length=1, max_length=192)
    sourceUrl: Optional[HttpUrl] = Field(None, alias='sourceUrl')
    preferVideo: bool = Field(default=False, alias='preferVideo')
    start: float = Field(..., ge=0)
    end: float = Field(..., gt=0)
    startHMS: Optional[str] = Field(None, alias='startHMS')
    endHMS: Optional[str] = Field(None, alias='endHMS')
    parentTitle: Optional[str] = Field(None, alias='parentTitle')
    clipLabel: Optional[str] = Field(None, alias='clipLabel')
    channel: Optional[str] = None
    contextMode: ContextMode = Field(default='seconds', alias='contextMode')
    padBefore: float = Field(default=5, ge=0, alias='padBefore')
    padAfter: float = Field(default=5, ge=0, alias='padAfter')
    renderProfile: RenderProfile = Field(default='hq-1080p-v1', alias='renderProfile')

    @validator('end', allow_reuse=True)
    def validate_end(cls, value: float, values: Dict[str, float]) -> float:  # pylint: disable=no-self-argument
        start = values.get('start')
        if start is not None and value <= start:
            raise ValueError('end must be greater than start')
        return value

    class Config:
        allow_population_by_field_name = True


class BundleRequest(BaseModel):
    scope: Optional[str] = None
    dedupe: Optional[bool] = None
    clips: List[BundleClipRequest]


class BundleClipResponse(BaseModel):
    key: str
    clipId: str
    status: ClipStatus
    downloadUrl: Optional[str] = None
    streamUrl: Optional[str] = None
    error: Optional[str] = None


class BundleInfo(BaseModel):
    status: ClipStatus
    downloadUrl: Optional[str] = None


class BundleResponse(BaseModel):
    batchId: str
    status: ClipStatus
    bundle: BundleInfo
    clips: List[BundleClipResponse]
    diagnostics: Optional[Dict[str, Any]] = None


@dataclass
class Job:
    id: str
    payload: ClipRequest
    status: ClipStatus = 'queued'
    stream_url: Optional[str] = None
    download_url: Optional[str] = None
    error_message: Optional[str] = None
    output_path: Optional[Path] = None
    artifact_sha256: Optional[str] = None
    artifact_bytes: Optional[int] = None
    validation: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_response(self) -> ClipResponse:
        return ClipResponse(
            clipId=self.id,
            status=self.status,
            streamUrl=self.stream_url,
            downloadUrl=self.download_url,
            errorMessage=self.error_message,
            requestPayload=self.payload,
            lastUpdated=self.updated_at,
        )


@dataclass
class BundleClip:
    key: str
    job: Job

    def to_response(self) -> BundleClipResponse:
        return BundleClipResponse(
            key=self.key,
            clipId=self.job.id,
            status=self.job.status,
            downloadUrl=self.job.download_url,
            streamUrl=self.job.stream_url,
            error=self.job.error_message,
        )


@dataclass
class BundleJob:
    id: str
    tenant_id: str
    requested_by_user_id: str
    scope: Optional[str]
    clips: Dict[str, BundleClip]
    status: ClipStatus = 'queued'
    download_url: Optional[str] = None
    error_message: Optional[str] = None
    output_path: Optional[Path] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_response(self) -> BundleResponse:
        bundle_info = BundleInfo(status=self.status, downloadUrl=self.download_url)
        clip_responses = [clip.to_response() for clip in self.clips.values()]
        return BundleResponse(
            batchId=self.id,
            status=self.status,
            bundle=bundle_info,
            clips=clip_responses,
            diagnostics=None,
        )


class ClipServiceConfig(BaseModel):
    dry_run: bool = Field(default=True, alias='dryRun')
    output_directory: Path = Field(default=Path('/tmp/clip-service/output'))
    sample_stream_url: Optional[str] = Field(
        default='https://storage.googleapis.com/coverr-public/videos/coverr-sketching-while-sitting-in-a-cafe-7414/1080p.mp4'
    )
    ffmpeg_copy_codec: bool = Field(default=False, alias='ffmpegCopyCodec')
    production: bool = False
    database_url: Optional[str] = None
    auto_migrate: bool = True
    worker_enabled: bool = True
    worker_poll_seconds: float = Field(default=1.0, ge=0.1, le=30.0)
    lease_seconds: int = Field(default=120, ge=15, le=3600)
    render_timeout_seconds: int = Field(default=1800, ge=30, le=7200)
    artifact_retention_seconds: int = Field(default=0, ge=0, le=31_536_000)
    retention_check_seconds: int = Field(default=300, ge=30, le=86_400)
    max_artifact_bytes: int = Field(default=2 * 1024 * 1024 * 1024, ge=1024)
    verify_media_sha256: bool = True
    media_roots: List[Path] = Field(default_factory=list)


class ClipJobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, Job] = {}
        self._dedupe: Dict[str, str] = {}

    def _payload_hash(self, payload: ClipRequest) -> str:
        key = (
            f'{payload.sourceUrl}|{payload.start}|{payload.end}|'
            f'{payload.padBefore}|{payload.padAfter}|{payload.contextMode}|{payload.preferVideo}'
        )
        return hashlib.sha256(key.encode('utf-8')).hexdigest()

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def find_existing(self, payload: ClipRequest) -> Optional[Job]:
        digest = self._payload_hash(payload)
        existing_id = self._dedupe.get(digest)
        if not existing_id:
            return None
        return self._jobs.get(existing_id)

    def create(self, job: Job) -> Job:
        digest = self._payload_hash(job.payload)
        self._jobs[job.id] = job
        self._dedupe[digest] = job.id
        return job


class BundleJobStore:
    def __init__(self) -> None:
        self._bundles: Dict[str, BundleJob] = {}

    def get(self, bundle_id: str, tenant_id: Optional[str] = None) -> Optional[BundleJob]:
        bundle = self._bundles.get(bundle_id)
        if bundle is None or (tenant_id is not None and bundle.tenant_id != tenant_id):
            return None
        return bundle

    def create(self, bundle: BundleJob) -> BundleJob:
        self._bundles[bundle.id] = bundle
        return bundle


def _configured_media_roots() -> List[Path]:
    raw = (os.getenv('CLIP_MEDIA_ROOTS') or '').strip()
    if not raw:
        return [Path('/srv/icmfyi/media')] if is_production_environment() else []
    return [Path(item.strip()) for item in raw.split(',') if item.strip()]


config = ClipServiceConfig(
    dryRun=_env_bool('CLIP_SERVICE_DRY_RUN', True),
    output_directory=Path(os.getenv('CLIP_OUTPUT_DIRECTORY', '/tmp/clip-service/output')),
    sample_stream_url=os.getenv('CLIP_SAMPLE_STREAM_URL'),
    ffmpegCopyCodec=_env_bool('CLIP_FFMPEG_COPY_CODEC', False),
    production=is_production_environment(),
    database_url=(os.getenv('CLIP_DATABASE_URL') or '').strip() or None,
    auto_migrate=_env_bool('CLIP_AUTO_MIGRATE', True),
    worker_enabled=_env_bool('CLIP_WORKER_ENABLED', True),
    worker_poll_seconds=float(os.getenv('CLIP_WORKER_POLL_SECONDS', '1.0')),
    lease_seconds=_env_int('CLIP_WORKER_LEASE_SECONDS', 120, minimum=15, maximum=3600),
    render_timeout_seconds=_env_int(
        'CLIP_RENDER_TIMEOUT_SECONDS', 1800, minimum=30, maximum=7200,
    ),
    artifact_retention_seconds=_env_int(
        'CLIP_ARTIFACT_RETENTION_SECONDS', 0, minimum=0, maximum=31_536_000,
    ),
    retention_check_seconds=_env_int(
        'CLIP_RETENTION_CHECK_SECONDS', 300, minimum=30, maximum=86_400,
    ),
    max_artifact_bytes=_env_int(
        'CLIP_MAX_ARTIFACT_BYTES',
        2 * 1024 * 1024 * 1024,
        minimum=1024,
        maximum=20 * 1024 * 1024 * 1024,
    ),
    verify_media_sha256=_env_bool('CLIP_VERIFY_MEDIA_SHA256', is_production_environment()),
    media_roots=_configured_media_roots(),
)
AUTH_TOKEN = (
    os.getenv('INTERNAL_SERVICE_SECRET')
    or os.getenv('CLIP_SERVICE_AUTH_TOKEN')
    or os.getenv('CLIP_SERVICE_TOKEN')
)
if config.production:
    if not config.database_url:
        raise RuntimeError('CLIP_DATABASE_URL is required in production')
    if not AUTH_TOKEN or len(AUTH_TOKEN) < 32:
        raise RuntimeError('INTERNAL_SERVICE_SECRET (or CLIP_SERVICE_AUTH_TOKEN) must be at least 32 characters')
    if config.dry_run:
        raise RuntimeError('CLIP_SERVICE_DRY_RUN must be false in production')
    if config.ffmpeg_copy_codec:
        raise RuntimeError('CLIP_FFMPEG_COPY_CODEC is not permitted in production')
    if not config.media_roots:
        raise RuntimeError('CLIP_MEDIA_ROOTS must name at least one canonical media root in production')
    if not config.verify_media_sha256:
        raise RuntimeError('CLIP_VERIFY_MEDIA_SHA256 must remain true in production')
    if not config.output_directory.is_absolute() or config.output_directory == Path('/'):
        raise RuntimeError('CLIP_OUTPUT_DIRECTORY must be a narrow absolute path in production')
    if 0 < config.artifact_retention_seconds < 3600:
        raise RuntimeError('CLIP_ARTIFACT_RETENTION_SECONDS must be zero or at least one hour')
    for configured_root in config.media_roots:
        if not configured_root.is_absolute() or configured_root == Path('/'):
            raise RuntimeError('each CLIP_MEDIA_ROOTS entry must be a narrow absolute path')
        normalized_root = configured_root.resolve()
        normalized_output = config.output_directory.resolve()
        if (
            normalized_root == normalized_output
            or normalized_root in normalized_output.parents
            or normalized_output in normalized_root.parents
        ):
            raise RuntimeError('CLIP_MEDIA_ROOTS and CLIP_OUTPUT_DIRECTORY must not overlap')

store = ClipJobStore()
bundle_store = BundleJobStore()
repository: Repository = create_repository(config.database_url)
storage_client: Optional[Any] = None
worker_task: Optional[asyncio.Task[Any]] = None
retention_task: Optional[asyncio.Task[Any]] = None
worker_wakeup: Optional[asyncio.Event] = None
WORKER_ID = f'{os.getenv("HOSTNAME", "clip")}-{os.getpid()}-{uuid4().hex[:12]}'

app = FastAPI(title='HQ Clip Service', version='0.2.0')

# NOTE: YouTube ids are 11 chars of [A-Za-z0-9_-]. Many local corpus filenames
# embed the id surrounded by underscores (which are also valid id characters),
# so we cannot rely on "word boundary" style regexes here.
YOUTUBE_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")


def ensure_output_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_output_path(job_id: str, extension: str = 'mp4') -> Path:
    ensure_output_directory(config.output_directory)
    return config.output_directory / f'{job_id}.{extension}'


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _path_within(path: Path, roots: List[Path]) -> bool:
    return any(path == root or root in path.parents for root in roots)


def resolve_canonical_media_path(source_path: str, expected_sha256: Optional[str] = None) -> Path:
    candidate = Path(source_path)
    if not candidate.is_absolute():
        raise ValueError('canonical media sourcePath must be absolute')
    resolved = candidate.resolve(strict=True)
    roots = [root.resolve(strict=True) for root in config.media_roots if root.exists()]
    if config.production and not roots:
        raise ValueError('no configured canonical media root is mounted')
    if roots and not _path_within(resolved, roots):
        raise ValueError('canonical media sourcePath is outside CLIP_MEDIA_ROOTS')
    if not resolved.is_file():
        raise ValueError('canonical media sourcePath must be a regular file')
    if expected_sha256 and config.verify_media_sha256:
        actual_sha256 = sha256_file(resolved)
        if not secrets.compare_digest(actual_sha256, expected_sha256):
            raise ValueError('canonical media SHA-256 no longer matches its registered identity')
    return resolved


def normalized_request_hash(request: ClipRequest, media_id: Optional[str]) -> str:
    payload = request.dict(by_alias=True, exclude_none=True)
    if media_id:
        payload.pop('sourceUrl', None)
        payload['mediaId'] = media_id
    encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=True)
    return hashlib.sha256(encoded.encode('ascii')).hexdigest()


def persistent_record_response(record: JobRecord) -> ClipResponse:
    payload = ClipRequest.parse_obj(record.payload)
    return ClipResponse(
        clipId=record.id,
        status=record.status,  # type: ignore[arg-type]
        streamUrl=record.stream_url,
        downloadUrl=record.download_url,
        errorMessage=record.error_message,
        requestPayload=payload,
        lastUpdated=record.updated_at,
    )


def _safe_artifact_path(record: JobRecord) -> Path:
    if not record.output_path:
        raise HTTPException(status_code=404, detail='Clip not available')
    try:
        output_root = config.output_directory.resolve(strict=True)
        path = Path(record.output_path).resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise HTTPException(status_code=404, detail='Clip not available') from exc
    if not _path_within(path, [output_root]) or not path.is_file():
        raise HTTPException(status_code=404, detail='Clip not available')
    return path


def parse_byte_range(range_header: str, file_size: int) -> Optional[Tuple[int, int]]:
    raw = range_header.strip()
    if not raw:
        return None
    if not raw.startswith('bytes=') or ',' in raw:
        raise ValueError('only one byte range is supported')
    value = raw[6:].strip()
    if '-' not in value:
        raise ValueError('invalid byte range')
    start_text, end_text = value.split('-', 1)
    if not start_text:
        try:
            suffix_length = int(end_text)
        except ValueError as exc:
            raise ValueError('invalid byte range') from exc
        if suffix_length <= 0:
            raise ValueError('invalid byte range')
        start = max(0, file_size - suffix_length)
        return start, file_size - 1
    try:
        start = int(start_text)
        end = int(end_text) if end_text else file_size - 1
    except ValueError as exc:
        raise ValueError('invalid byte range') from exc
    if start < 0 or start >= file_size or end < start:
        raise ValueError('unsatisfiable byte range')
    return start, min(end, file_size - 1)


def iter_file_range(path: Path, start: int, end: int, chunk_size: int = 1024 * 1024):
    remaining = end - start + 1
    with path.open('rb') as handle:
        handle.seek(start)
        while remaining > 0:
            block = handle.read(min(chunk_size, remaining))
            if not block:
                break
            remaining -= len(block)
            yield block


def build_bundle_output_path(bundle_id: str, extension: str = 'zip') -> Path:
    ensure_output_directory(config.output_directory)
    return config.output_directory / f'{bundle_id}.{extension}'


def parse_gs_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith('gs://'):
        raise ValueError('Invalid GCS URI')
    without_scheme = uri[5:]
    parts = without_scheme.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError('GCS URI must include bucket and object path')
    return parts[0], parts[1]

def _looks_like_youtube_url(url: str) -> bool:
    u = (url or "").lower()
    return "youtube.com" in u or "youtu.be" in u


def _extract_youtube_video_id(url: str) -> Optional[str]:
    raw = (url or "").strip()
    if not raw:
        return None
    try:
        u = urlparse(raw)
        host = (u.netloc or "").lower()
        if "youtu.be" in host:
            slug = (u.path or "").lstrip("/").split("/")[0]
            if slug and YOUTUBE_ID_RE.fullmatch(slug):
                return slug
        if "youtube.com" in host:
            q = parse_qs(u.query or "")
            cand = (q.get("v") or [""])[0]
            if cand and YOUTUBE_ID_RE.fullmatch(cand):
                return cand
    except Exception:
        pass
    # Local corpus paths usually embed the id as its own "_" token:
    #   2025-06-03_dCJ24EiGQXI_title/...mp3
    #   2025-06-09__GdGzpS-Kpc_title/...mp3
    for part in re.split(r"[\\/]", raw):
        for tok in part.split("_"):
            cand = (tok or "").strip()
            if not cand:
                continue
            # Strip extensions and trailing punctuation without touching '-'/'_' which are valid in ids.
            cand = cand.split(".", 1)[0]
            cand = cand.strip("()[]{}<>\"' ,;:")
            if len(cand) == 11 and YOUTUBE_ID_RE.fullmatch(cand):
                return cand

    m = YOUTUBE_ID_RE.search(raw)
    return m.group(0) if m else None


def _local_audio_roots() -> List[Path]:
    raw = (os.environ.get("LOCAL_AUDIO_ROOTS") or "").strip()
    if raw:
        roots = [Path(p.strip()) for p in raw.split(",") if p.strip()]
    else:
        roots = [
            Path("/datasets/gcs_youtube_audio"),
            Path("/pipeline_storage/yt_diarizer"),
        ]
    # De-dupe while preserving order
    seen = set()
    out: List[Path] = []
    for r in roots:
        rp = r.resolve() if r.exists() else r
        if str(rp) in seen:
            continue
        seen.add(str(rp))
        out.append(rp)
    return out


_LOCAL_AUDIO_INDEX: Dict[str, Path] = {}
_LOCAL_AUDIO_INDEX_BUILT = False
_LOCAL_AUDIO_INDEX_BUILT_AT = 0.0
_LOCAL_AUDIO_INDEX_LAST_FORCE_REFRESH_AT = 0.0


def _env_float_value(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _local_audio_index_ttl_s() -> float:
    # Refresh periodically so newly-downloaded audio becomes available without
    # restarting the clip-service container.
    return max(0.0, _env_float_value("LOCAL_AUDIO_INDEX_TTL_S", 300.0))


def _local_audio_index_min_force_refresh_interval_s() -> float:
    # Avoid rebuilding the index too frequently on repeated cache-miss requests.
    return max(0.0, _env_float_value("LOCAL_AUDIO_INDEX_MIN_REFRESH_INTERVAL_S", 30.0))


def _build_local_audio_index(*, force: bool = False) -> None:
    """
    Build an in-memory mapping: youtube_video_id -> local audio file path.

    This lets clip generation avoid yt-dlp (and YouTube anti-bot/rate limits) when
    audio is already mirrored locally from GCS or prior diarization runs.
    """
    global _LOCAL_AUDIO_INDEX_BUILT  # pylint: disable=global-statement
    global _LOCAL_AUDIO_INDEX_BUILT_AT  # pylint: disable=global-statement
    ttl_s = _local_audio_index_ttl_s()
    now = time.time()
    if _LOCAL_AUDIO_INDEX_BUILT and not force and ttl_s > 0 and (now - _LOCAL_AUDIO_INDEX_BUILT_AT) < ttl_s:
        return

    audio_exts = {".mp3", ".m4a", ".wav", ".aac", ".flac", ".ogg", ".opus"}
    index: Dict[str, Path] = {}
    for root in _local_audio_roots():
        if not root.exists():
            continue
        # Only scan audio files; the corpus also has many JSON artifacts.
        for audio in root.rglob("*"):
            try:
                if not audio.is_file():
                    continue
                if audio.suffix.lower() not in audio_exts:
                    continue
                video_id = _extract_youtube_video_id(audio.as_posix())
            except Exception:
                video_id = None
            if not video_id:
                continue
            # Prefer the first seen path (stable). If duplicates exist, pick the larger file.
            existing = index.get(video_id)
            if existing is None:
                index[video_id] = audio
            else:
                try:
                    if audio.stat().st_size > existing.stat().st_size:
                        index[video_id] = audio
                except Exception:
                    pass

    _LOCAL_AUDIO_INDEX.clear()
    _LOCAL_AUDIO_INDEX.update(index)
    _LOCAL_AUDIO_INDEX_BUILT = True
    _LOCAL_AUDIO_INDEX_BUILT_AT = now


def _resolve_local_audio(video_id: str) -> Optional[Path]:
    global _LOCAL_AUDIO_INDEX_LAST_FORCE_REFRESH_AT  # pylint: disable=global-statement
    vid = (video_id or "").strip()
    if not vid:
        return None
    _build_local_audio_index()
    path = _LOCAL_AUDIO_INDEX.get(vid)
    if path and path.exists():
        return path

    # Cache miss: the index might be stale because new audio was downloaded after
    # the last build. Force-refresh at most once every N seconds.
    min_interval_s = _local_audio_index_min_force_refresh_interval_s()
    now = time.time()
    if (now - float(_LOCAL_AUDIO_INDEX_LAST_FORCE_REFRESH_AT)) >= float(min_interval_s):
        _LOCAL_AUDIO_INDEX_LAST_FORCE_REFRESH_AT = now
        _build_local_audio_index(force=True)
        path = _LOCAL_AUDIO_INDEX.get(vid)
        if path and path.exists():
            return path

    return None


def _resolve_local_video(video_id: str) -> Optional[Path]:
    """
    Resolve locally cached video for a YouTube id.

    In the local docker compose stack, yt_diarizer writes:
      /pipeline_storage/yt_diarizer/<id>/<id>.mp4
    and clip-service mounts pipeline storage read-only.
    """
    # Allow callers to override/add mount points (eg. /data/pipeline_storage/yt_diarizer)
    # without changing code.
    raw_roots = (os.environ.get("LOCAL_VIDEO_ROOTS") or "").strip()
    roots = [Path(p.strip()) for p in raw_roots.split(",")] if raw_roots else []
    roots.extend([
        Path("/pipeline_storage/yt_diarizer"),
        Path("/data/pipeline_storage/yt_diarizer"),
        Path("/app/.local-data/pipeline_storage/yt_diarizer"),
    ])
    seen = set()
    candidate_roots: List[Path] = []
    for r in roots:
        if not r:
            continue
        rp = r if r.is_absolute() else r.resolve()
        if str(rp) in seen:
            continue
        seen.add(str(rp))
        candidate_roots.append(rp)

    vid = (video_id or "").strip()
    if not vid:
        return None

    for base in candidate_roots:
        if not base.exists():
            continue
        for ext in (".mp4", ".webm", ".mkv", ".mov", ".m4v"):
            cand = base / vid / f"{vid}{ext}"
            if cand.exists():
                return cand
        for name in ("source.mp4", "source.webm", "source.mkv", "source.m4a", "source.mp3"):
            cand = base / vid / name
            if cand.exists():
                return cand
    return None


def _is_video_source(path: Path) -> bool:
    return (path.suffix or "").lower() in {".mp4", ".webm", ".mkv", ".mov", ".m4v"}


def _download_youtube_thumbnail(video_id: str, workdir: Path) -> Optional[Path]:
    vid = (video_id or "").strip()
    if not vid:
        return None
    variants = ["maxresdefault.jpg", "hq720.jpg", "sddefault.jpg", "hqdefault.jpg", "mqdefault.jpg", "default.jpg"]
    for variant in variants:
        url = f"https://i.ytimg.com/vi/{vid}/{variant}"
        try:
            req = UrlRequest(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=5) as resp:
                if resp.status != 200:
                    continue
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if "image" not in ctype:
                    continue
                data = resp.read()
                if not data:
                    continue
            out = workdir / f"thumb_{vid}_{variant}"
            out.write_bytes(data)
            return out
        except Exception:
            continue
    return None


def download_from_gcs(uri: str, workdir: Path) -> Path:
    if storage is None:
        raise RuntimeError('google-cloud-storage is required to fetch gs:// URIs')
    global storage_client  # pylint: disable=global-statement
    bucket_name, object_name = parse_gs_uri(uri)
    if storage_client is None:
        storage_client = storage.Client()
    destination = workdir / Path(object_name).name
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.download_to_filename(destination)
    return destination


def download_with_ytdlp(
    url: str,
    workdir: Path,
    *,
    prefer_video: bool = False,
    start_s: Optional[float] = None,
    end_s: Optional[float] = None,
) -> Path:
    if YoutubeDL is None:
        raise RuntimeError('yt-dlp is required to download non-GCS sources')
    output_template = str(workdir / 'source.%(ext)s')

    def _env_float(*names: str) -> Optional[float]:
        for name in names:
            raw = (os.environ.get(name) or "").strip()
            if not raw:
                continue
            try:
                return float(raw)
            except ValueError:
                continue
        return None

    def _is_youtube_bot_check(msg: str) -> bool:
        s = (msg or "").lower()
        return (
            ("sign in to confirm" in s and "not a bot" in s)
            or ("confirm you" in s and "not a bot" in s)
            or ("this helps protect our community" in s)
            or ("rate-limited by youtube" in s)
            or ("current session has been rate-limited" in s)
            or ("this content isn't available, try again later" in s)
        )

    def _proxy_pool() -> List[Optional[str]]:
        raw_pool = (os.environ.get("YTDLP_PROXIES") or os.environ.get("YTDLP_PROXY_POOL") or "").strip()
        items: List[str] = []
        if raw_pool:
            items.extend([p.strip() for p in raw_pool.split(",") if p.strip()])

        single = (os.environ.get("YTDLP_PROXY") or "").strip()
        if single:
            items.insert(0, single)

        # de-dupe while preserving order
        seen = set()
        uniq: List[str] = []
        for p in items:
            if p in seen:
                continue
            seen.add(p)
            uniq.append(p)

        if not uniq:
            return [None]

        force = (os.environ.get("YTDLP_PROXY_FORCE") or "").strip().lower() in ("1", "true", "yes", "y", "on")
        if force:
            return list(uniq)
        return [None] + list(uniq)

    def _apply_proxy(ydl_opts: Dict[str, Any], proxy: Optional[str]) -> None:
        if proxy:
            # Avoid logging proxy values (may contain creds). This only configures yt-dlp.
            ydl_opts["proxy"] = proxy
        else:
            ydl_opts.pop("proxy", None)

    def _parse_player_clients() -> List[str]:
        raw = (os.environ.get("YTDLP_PLAYER_CLIENTS") or "").strip()
        if raw:
            out: List[str] = []
            for part in raw.split(","):
                p = part.strip()
                if p:
                    out.append(p)
            if out:
                return out

        # Default: try non-web clients first. These often bypass stricter web experiments.
        return ["android", "ios", "tv", "web_embedded"]

    def _apply_player_client(ydl_opts: Dict[str, Any], client: str) -> None:
        y = ydl_opts.setdefault("extractor_args", {}).setdefault("youtube", {})
        y["player_client"] = [client]

        po_token = (os.environ.get("YTDLP_PO_TOKEN") or "").strip()
        if po_token:
            y["po_token"] = [po_token]

        # Optional: some setups work better when cookies are only used for web clients.
        drop = (os.environ.get("YTDLP_DROP_COOKIES_FOR_NON_WEB") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        if drop and not client.startswith("web"):
            ydl_opts.pop("cookiefile", None)
            ydl_opts.pop("cookiesfrombrowser", None)

    # Reuse the same env conventions as ingestion so local operators can share cookies/proxies.
    cookiefile = (os.getenv('YTDLP_COOKIES_FILE') or os.getenv('YTDLP_COOKIES_PATH') or '').strip()
    if not cookiefile:
        # Default mount path used by the local docker compose stack.
        default_cookie = Path('/cookies/youtube.txt')
        cookiefile = str(default_cookie) if default_cookie.exists() else ''
    user_agent = (os.getenv('YTDLP_USER_AGENT') or '').strip()

    fmt_video = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"
    ydl_opts = {
        'quiet': True,
        'noplaylist': True,
        'outtmpl': output_template,
        'format': fmt_video if prefer_video else 'mp4/mp4-best/best',
        'ignoreerrors': False,
        'no_warnings': True,
    }
    section = None
    if prefer_video and start_s is not None and end_s is not None:
        try:
            start = float(start_s)
            end = float(end_s)
            if end > start:
                window_start = max(0.0, start - 2.0)
                window_end = end + 2.0
                section = f"*{window_start:.3f}-{window_end:.3f}"
                ydl_opts['download_sections'] = [section]
        except Exception:
            section = None
    if cookiefile and Path(cookiefile).exists():
        ydl_opts['cookiefile'] = cookiefile
    if user_agent:
        ydl_opts['user_agent'] = user_agent

    remote_components = (os.environ.get("YTDLP_REMOTE_COMPONENTS") or "").strip()
    if remote_components:
        parts = [p.strip() for p in remote_components.split(",") if p.strip()]
        if parts:
            ydl_opts["remote_components"] = parts

    sleep_requests = _env_float("YTDLP_SLEEP_REQUESTS", "YTDLP_SLEEP_REQUESTS_S")
    if sleep_requests is not None:
        ydl_opts["sleep_requests"] = sleep_requests
    sleep_interval = _env_float(
        "YTDLP_SLEEP_INTERVAL",
        "YTDLP_SLEEP_INTERVAL_S",
        "YTDLP_MIN_SLEEP_INTERVAL",
        "YTDLP_MIN_SLEEP_INTERVAL_S",
    )
    if sleep_interval is not None:
        ydl_opts["sleep_interval"] = sleep_interval
    max_sleep_interval = _env_float("YTDLP_MAX_SLEEP_INTERVAL", "YTDLP_MAX_SLEEP_INTERVAL_S")
    if max_sleep_interval is not None:
        ydl_opts["max_sleep_interval"] = max_sleep_interval
    sleep_subtitles = _env_float("YTDLP_SLEEP_SUBTITLES", "YTDLP_SLEEP_SUBTITLES_S")
    if sleep_subtitles is not None:
        ydl_opts["sleep_subtitles"] = sleep_subtitles

    last_exc: Optional[Exception] = None
    proxies = _proxy_pool()
    clients = _parse_player_clients()
    for proxy in proxies:
        for client in clients:
            attempt_opts: Dict[str, Any] = dict(ydl_opts)
            _apply_proxy(attempt_opts, proxy)
            _apply_player_client(attempt_opts, client)
            sectioned_attempted = section is not None
            try:
                with YoutubeDL(attempt_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)
                if not filename:
                    raise RuntimeError("yt-dlp did not produce a filename")
                path = Path(filename).resolve()
                if prefer_video and not _is_video_source(path):
                    # Some configurations resolve to audio-only formats depending on region.
                    # Retry below with the same proxy/client pair so we can force video.
                    raise RuntimeError("yt-dlp selected audio-only asset while video was requested")
                return path
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                # If sectioned video download fails, retry once with full download so we can still produce a result when partial download
                # is unsupported by a given extractor/client.
                if sectioned_attempted:
                    sectioned_attempted = False
                    attempt_opts_full = dict(ydl_opts)
                    attempt_opts_full.pop('download_sections', None)
                    attempt_opts_full.pop('format', None)
                    attempt_opts_full['format'] = fmt_video if prefer_video else 'mp4/mp4-best/best'
                    _apply_proxy(attempt_opts_full, proxy)
                    _apply_player_client(attempt_opts_full, client)
                    try:
                        with YoutubeDL(attempt_opts_full) as ydl:
                            info = ydl.extract_info(url, download=True)
                            filename = ydl.prepare_filename(info)
                        if not filename:
                            raise RuntimeError('yt-dlp did not produce a filename')
                        path = Path(filename).resolve()
                        if prefer_video and not _is_video_source(path):
                            raise RuntimeError("yt-dlp selected audio-only asset while video was requested")
                        return path
                    except Exception as exc2:  # pylint: disable=broad-except
                        last_exc = exc2
                        # continue loop for normal retry behavior
                # Bot-checks are usually IP-level. If a proxy pool is configured, rotate
                # quickly instead of burning attempts across many clients.
                if _is_youtube_bot_check(str(exc)) and len(proxies) > 1:
                    break
                continue

    raise RuntimeError(str(last_exc) or "yt-dlp failed") from last_exc


async def fetch_source(payload: ClipRequest, workdir: Path) -> Path:
    if not payload.sourceUrl:
        raise ValueError('sourceUrl is required for clip generation when dry-run is disabled')
    source_url = str(payload.sourceUrl)
    # Fast-paths for YouTube sources:
    # - preferVideo=true: use locally cached mp4 from yt_diarizer if present; otherwise yt-dlp download.
    # - preferVideo=false: prefer locally mirrored audio (mp3) to avoid yt-dlp/YouTube bot checks.
    if _looks_like_youtube_url(source_url):
        vid = _extract_youtube_video_id(source_url)
        if vid:
            if getattr(payload, "preferVideo", False):
                local_video = _resolve_local_video(vid)
                if local_video:
                    print(f"[clip-service] using local video video_id={vid} file={local_video.name}", flush=True)
                    return local_video
            else:
                local_audio = _resolve_local_audio(vid)
                if local_audio:
                    print(f"[clip-service] using local audio video_id={vid} file={local_audio.name}", flush=True)
                    return local_audio
    if source_url.startswith('gs://'):
        return await asyncio.to_thread(download_from_gcs, source_url, workdir)
    return await asyncio.to_thread(
        download_with_ytdlp,
        source_url,
        workdir,
        prefer_video=bool(getattr(payload, "preferVideo", False)),
        start_s=(payload.start if getattr(payload, "preferVideo", False) else None),
        end_s=(payload.end if getattr(payload, "preferVideo", False) else None),
    )

def _is_audio_source(path: Path) -> bool:
    suf = path.suffix.lower()
    return suf in {".mp3", ".m4a", ".wav", ".aac", ".flac", ".ogg", ".opus"}


async def run_bounded_subprocess(args: List[str], timeout_seconds: int) -> Tuple[int, bytes, bytes]:
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
        raise
    return process.returncode or 0, stdout, stderr


async def validate_rendered_artifact(path: Path, expected_duration: float) -> Dict[str, Any]:
    if not path.is_file():
        raise RuntimeError('ffmpeg did not publish an artifact candidate')
    artifact_bytes = path.stat().st_size
    if artifact_bytes <= 0:
        raise RuntimeError('ffmpeg produced an empty artifact')
    if artifact_bytes > config.max_artifact_bytes:
        raise RuntimeError('rendered artifact exceeds CLIP_MAX_ARTIFACT_BYTES')
    returncode, stdout, stderr = await run_bounded_subprocess(
        [
            'ffprobe',
            '-v',
            'error',
            '-show_entries',
            'format=duration,format_name:stream=codec_type,codec_name,width,height',
            '-of',
            'json',
            str(path),
        ],
        30,
    )
    if returncode != 0:
        detail = stderr.decode('utf-8', errors='replace')[:1000]
        raise RuntimeError(detail or 'ffprobe rejected the rendered artifact')
    try:
        probe = json.loads(stdout.decode('utf-8'))
        duration = float(probe['format']['duration'])
        streams = probe.get('streams') or []
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError('ffprobe returned an invalid artifact description') from exc
    if not math.isfinite(duration) or duration <= 0:
        raise RuntimeError('rendered artifact has no finite positive duration')
    tolerance = max(2.0, min(10.0, expected_duration * 0.10))
    if duration > expected_duration + tolerance:
        raise RuntimeError('rendered artifact duration exceeds the bounded request window')
    video_streams = [stream for stream in streams if stream.get('codec_type') == 'video']
    audio_streams = [stream for stream in streams if stream.get('codec_type') == 'audio']
    if not video_streams:
        raise RuntimeError('rendered artifact has no video stream')
    if not audio_streams:
        raise RuntimeError('rendered artifact has no audio stream')
    return {
        'format_name': str(probe['format'].get('format_name') or ''),
        'duration_seconds': round(duration, 6),
        'artifact_bytes': artifact_bytes,
        'video_codec': str(video_streams[0].get('codec_name') or ''),
        'width': int(video_streams[0].get('width') or 0),
        'height': int(video_streams[0].get('height') or 0),
        'audio_codec': str(audio_streams[0].get('codec_name') or ''),
        'render_profile': 'hq-1080p-v1',
    }


def publish_artifact_atomically(staged_path: Path, job_id: str, artifact_sha256: str) -> Path:
    artifact_directory = config.output_directory / 'artifacts' / job_id
    artifact_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
    final_path = artifact_directory / f'{artifact_sha256}.mp4'
    with staged_path.open('rb') as handle:
        os.fsync(handle.fileno())
    if final_path.exists():
        if sha256_file(final_path) != artifact_sha256:
            raise RuntimeError('existing immutable artifact path has unexpected bytes')
        staged_path.unlink()
    else:
        os.replace(staged_path, final_path)
        final_path.chmod(0o640)
    directory_fd = os.open(str(artifact_directory), os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return final_path


async def run_ffmpeg_clip(
    job: Job,
    *,
    source_override: Optional[Path] = None,
    raise_errors: bool = False,
) -> None:
    job.status = 'processing'
    job.updated_at = datetime.now(timezone.utc)

    if config.dry_run:
        await asyncio.sleep(0)
        job.stream_url = config.sample_stream_url
        job.download_url = config.sample_stream_url
        marker = (config.sample_stream_url or 'dry-run').encode('utf-8')
        job.output_path = None
        job.artifact_sha256 = hashlib.sha256(marker).hexdigest()
        job.artifact_bytes = len(marker)
        job.validation = {'dry_run': True, 'render_profile': job.payload.renderProfile}
        job.status = 'ready'
        job.updated_at = datetime.now(timezone.utc)
        return

    staged_path: Optional[Path] = None
    try:
        with tempfile.TemporaryDirectory(prefix=f'clip-job-{job.id}-') as tmp:
            workdir = Path(tmp)
            source_path = source_override or await fetch_source(job.payload, workdir)
            if not source_path.exists():
                raise FileNotFoundError('Downloaded source not found')

            staging_directory = config.output_directory / '.staging'
            staging_directory.mkdir(parents=True, exist_ok=True, mode=0o750)
            staged_path = staging_directory / f'{job.id}-{uuid4().hex}.mp4'
            output_path = staged_path

            effective_pad_before = job.payload.padBefore
            effective_pad_after = job.payload.padAfter

            start_time = max(0.0, job.payload.start - effective_pad_before)
            target_end = job.payload.end + effective_pad_after
            duration = max(0.5, target_end - start_time)

            args: List[str]
            fallback_args: Optional[List[str]] = None
            if _is_audio_source(source_path):
                # Audio-only sources (GCS corpus mp3s, diarizer cache).
                if getattr(job.payload, "preferVideo", False):
                    raise RuntimeError("requested preferVideo=true but source resolved to audio-only")
                #
                # Generate an mp4 with a still image video track so the frontend can
                # play it in a <video> element consistently.
                vid = _extract_youtube_video_id(str(job.payload.sourceUrl or "")) or _extract_youtube_video_id(source_path.as_posix())
                thumb_path = _download_youtube_thumbnail(vid or "", workdir) if vid else None

                if thumb_path and thumb_path.exists():
                    args = [
                        "ffmpeg",
                        "-y",
                        "-loop",
                        "1",
                        "-i",
                        str(thumb_path),
                        "-ss",
                        f"{start_time:.3f}",
                        "-t",
                        f"{duration:.3f}",
                        "-i",
                        str(source_path),
                        "-c:v",
                        "libx264",
                        "-tune",
                        "stillimage",
                        "-preset",
                        "medium",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "192k",
                        "-shortest",
                        "-movflags",
                        "+faststart",
                        str(output_path),
                    ]
                else:
                    # No thumbnail available: render a black background video.
                    args = [
                        "ffmpeg",
                        "-y",
                        "-f",
                        "lavfi",
                        "-i",
                        "color=c=black:s=1280x720:r=30",
                        "-ss",
                        f"{start_time:.3f}",
                        "-t",
                        f"{duration:.3f}",
                        "-i",
                        str(source_path),
                        "-c:v",
                        "libx264",
                        "-tune",
                        "stillimage",
                        "-preset",
                        "medium",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "192k",
                        "-shortest",
                        "-movflags",
                        "+faststart",
                        str(output_path),
                    ]
            else:
                # Video source: cut directly.
                prefer_video = bool(getattr(job.payload, "preferVideo", False))
                # Stream-copy seeks are often unusable for tiny segments (keyframe snapping -> "3 frames").
                # Force a transcode for preferVideo clips and other very short segments.
                force_transcode = prefer_video or duration <= 2.0

                if force_transcode or not config.ffmpeg_copy_codec:
                    # For accuracy, put -ss after -i when transcoding.
                    args = [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(source_path),
                        "-ss",
                        f"{start_time:.3f}",
                        "-t",
                        f"{duration:.3f}",
                        "-c:v",
                        "libx264",
                        "-preset",
                        "medium",
                        "-crf",
                        "18",
                        "-vf",
                        "scale='min(1920,iw)':-2",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "192k",
                        "-movflags",
                        "+faststart",
                        str(output_path),
                    ]
                else:
                    base_args = [
                        "ffmpeg",
                        "-y",
                        "-ss",
                        f"{start_time:.3f}",
                        "-i",
                        str(source_path),
                        "-t",
                        f"{duration:.3f}",
                    ]
                    args = [*base_args, "-c", "copy", "-movflags", "+faststart", str(output_path)]
                    # If stream copy cannot satisfy seek/cut constraints, retry with transcode.
                    fallback_args = [
                        *base_args,
                        "-c:v",
                        "libx264",
                        "-preset",
                        "medium",
                        "-crf",
                        "18",
                        "-vf",
                        "scale='min(1920,iw)':-2",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
                        "-b:a",
                        "192k",
                        "-movflags",
                        "+faststart",
                        str(output_path),
                    ]

            returncode, _, stderr = await run_bounded_subprocess(args, config.render_timeout_seconds)

            if returncode != 0:
                stderr_text = stderr.decode('utf-8', errors='ignore') if stderr else ''
                if fallback_args:
                    try:
                        if output_path.exists():
                            output_path.unlink()
                    except Exception:
                        pass
                    print('[clip-service] ffmpeg copy cut failed; retrying with transcode', flush=True)
                    returncode, _, stderr = await run_bounded_subprocess(
                        fallback_args,
                        config.render_timeout_seconds,
                    )
                    if returncode != 0:
                        stderr_text = stderr.decode('utf-8', errors='ignore') if stderr else ''
                        raise RuntimeError(stderr_text or 'ffmpeg exited with non-zero status')
                else:
                    raise RuntimeError(stderr_text or 'ffmpeg exited with non-zero status')

            validation = await validate_rendered_artifact(output_path, duration)
            artifact_sha256 = await asyncio.to_thread(sha256_file, output_path)
            artifact_bytes = output_path.stat().st_size
            final_path = await asyncio.to_thread(publish_artifact_atomically, output_path, job.id, artifact_sha256)
            staged_path = None
            job.output_path = final_path
            job.artifact_sha256 = artifact_sha256
            job.artifact_bytes = artifact_bytes
            job.validation = validation
            job.stream_url = f'/clips/{job.id}/file'
            job.download_url = f'/clips/{job.id}/file?download=1'
            job.status = 'ready'
        job.updated_at = datetime.now(timezone.utc)
    except Exception as exc:  # pylint: disable=broad-except
        if staged_path is not None:
            try:
                staged_path.unlink(missing_ok=True)
            except OSError:
                pass
        job.status = 'error'
        job.error_message = str(exc)[:2000]
        job.updated_at = datetime.now(timezone.utc)
        if raise_errors:
            raise


def create_bundle_archive(bundle: BundleJob) -> Path:
    output_path = build_bundle_output_path(bundle.id)
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        added_clip = False
        for clip in bundle.clips.values():
            job = clip.job
            if job.output_path and job.output_path.exists():
                archive.write(job.output_path, arcname=f'{clip.key}.mp4')
                added_clip = True
        if not added_clip:
            archive.writestr('README.txt', 'No clip files were generated for this bundle.')
    return output_path


async def process_job(job: Job) -> None:
    await run_ffmpeg_clip(job)


def clip_request_from_bundle(clip: BundleClipRequest) -> ClipRequest:
    payload = ClipRequest(
        mediaId=clip.mediaId,
        sourceUrl=clip.sourceUrl,
        parentTitle=clip.parentTitle,
        clipLabel=clip.clipLabel,
        channel=clip.channel,
        preferVideo=clip.preferVideo,
        start=clip.start,
        end=clip.end,
        contextMode=clip.contextMode,
        padBefore=clip.padBefore,
        padAfter=clip.padAfter,
        renderProfile=clip.renderProfile,
    )
    return payload


async def process_bundle_job(bundle: BundleJob) -> None:
    bundle.status = 'processing'
    bundle.updated_at = datetime.utcnow()

    for clip in bundle.clips.values():
        job = clip.job
        if job.status == 'ready' and job.output_path and job.output_path.exists():
            continue
        # reset job output before processing
        job.stream_url = None
        job.download_url = None
        job.error_message = None
        job.output_path = None
        job.status = 'queued'
        job.updated_at = datetime.utcnow()
        await run_ffmpeg_clip(job)

    errors = [clip for clip in bundle.clips.values() if clip.job.status == 'error']
    if errors:
        bundle.status = 'error'
        bundle.download_url = None
        bundle.error_message = 'One or more clips failed to generate.'
        bundle.updated_at = datetime.utcnow()
        return

    bundle.status = 'ready'
    bundle.error_message = None
    if config.dry_run:
        bundle.download_url = config.sample_stream_url
        bundle.output_path = None
    else:
        bundle.output_path = await asyncio.to_thread(create_bundle_archive, bundle)
        bundle.download_url = f'/clips/batch/{bundle.id}/file?download=1'
    bundle.updated_at = datetime.utcnow()


async def _lease_heartbeat(job_id: str, stop: asyncio.Event, lease_lost: asyncio.Event) -> None:
    interval = max(5.0, config.lease_seconds / 3.0)
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            renewed = await asyncio.to_thread(
                repository.heartbeat,
                job_id,
                WORKER_ID,
                config.lease_seconds,
            )
            if not renewed:
                lease_lost.set()
                return


async def process_claimed_record(record: JobRecord) -> None:
    stop_heartbeat = asyncio.Event()
    lease_lost = asyncio.Event()
    heartbeat_task = asyncio.create_task(_lease_heartbeat(record.id, stop_heartbeat, lease_lost))
    try:
        payload = ClipRequest.parse_obj(record.payload)
        source_override: Optional[Path] = None
        if record.media_id:
            media = await asyncio.to_thread(repository.resolve_media, record.tenant_id, record.media_id)
            if (config.production or payload.preferVideo) and not media.video_capable:
                raise ValueError('canonical media is not clip-ready video')
            source_override = await asyncio.to_thread(
                resolve_canonical_media_path,
                media.source_path,
                media.source_sha256,
            )
        elif config.production:
            raise ValueError('production clip jobs require canonical mediaId')

        job = Job(id=record.id, payload=payload)
        await run_ffmpeg_clip(job, source_override=source_override, raise_errors=True)
        if lease_lost.is_set():
            raise LeaseLost('worker lease expired during rendering')
        output_path = str(job.output_path) if job.output_path else (job.stream_url or 'dry-run')
        await asyncio.to_thread(
            repository.mark_ready,
            record.id,
            WORKER_ID,
            stream_url=job.stream_url or f'/clips/{record.id}/file',
            download_url=job.download_url or f'/clips/{record.id}/file?download=1',
            output_path=output_path,
            artifact_sha256=job.artifact_sha256 or hashlib.sha256(output_path.encode('utf-8')).hexdigest(),
            artifact_bytes=job.artifact_bytes or max(1, len(output_path.encode('utf-8'))),
            validation=job.validation,
        )
    except LeaseLost:
        return
    except Exception as exc:  # pylint: disable=broad-except
        retryable = not isinstance(exc, (FileNotFoundError, MediaNotFound, ValueError))
        delay = min(300, 5 * (2 ** max(0, record.attempt_count - 1)))
        error_message = 'clip render failed' if config.production else str(exc)
        print(
            f'[clip-service] render_failed job_id={record.id} '
            f'error_type={type(exc).__name__} retryable={str(retryable).lower()}',
            flush=True,
        )
        try:
            await asyncio.to_thread(
                repository.mark_failure,
                record.id,
                WORKER_ID,
                error_code='render_failed',
                error_message=error_message,
                retryable=retryable,
                retry_delay_seconds=delay,
            )
        except LeaseLost:
            return
    finally:
        stop_heartbeat.set()
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task


async def process_persistent_job(job_id: str) -> None:
    record = await asyncio.to_thread(repository.claim, job_id, WORKER_ID, config.lease_seconds)
    if record is not None:
        await process_claimed_record(record)


async def persistent_worker_loop() -> None:
    assert worker_wakeup is not None
    while True:
        try:
            record = await asyncio.to_thread(repository.claim_next, WORKER_ID, config.lease_seconds)
        except Exception as exc:  # pylint: disable=broad-except
            print(
                f'[clip-service] worker_poll_failed error_type={type(exc).__name__}',
                flush=True,
            )
            await asyncio.sleep(config.worker_poll_seconds)
            continue
        if record is not None:
            await process_claimed_record(record)
            continue
        worker_wakeup.clear()
        try:
            await asyncio.wait_for(worker_wakeup.wait(), timeout=config.worker_poll_seconds)
        except asyncio.TimeoutError:
            pass


def delete_expired_artifact(path_value: str) -> bool:
    try:
        output_root = config.output_directory.resolve(strict=True)
        artifact_path = Path(path_value).resolve(strict=True)
    except (FileNotFoundError, OSError):
        return False
    if not _path_within(artifact_path, [output_root]) or not artifact_path.is_file():
        return False
    artifact_path.unlink()
    return True


async def run_retention_cleanup_once() -> Tuple[int, int]:
    if config.artifact_retention_seconds <= 0:
        return 0, 0
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=config.artifact_retention_seconds)
    expired_paths = await asyncio.to_thread(repository.expire_ready_before, cutoff, 1000)
    deleted = 0
    for path_value in expired_paths:
        try:
            deleted += int(await asyncio.to_thread(delete_expired_artifact, path_value))
        except OSError as exc:
            print(
                f'[clip-service] retention_unlink_failed error_type={type(exc).__name__}',
                flush=True,
            )
    if expired_paths:
        print(
            f'[clip-service] retention_complete expired={len(expired_paths)} deleted={deleted}',
            flush=True,
        )
    return len(expired_paths), deleted


async def retention_cleanup_loop() -> None:
    while True:
        await asyncio.sleep(config.retention_check_seconds)
        try:
            await run_retention_cleanup_once()
        except Exception as exc:  # pylint: disable=broad-except
            print(
                f'[clip-service] retention_failed error_type={type(exc).__name__}',
                flush=True,
            )


@app.on_event('startup')
async def startup_service() -> None:
    global retention_task, worker_task, worker_wakeup  # pylint: disable=global-statement
    ensure_output_directory(config.output_directory)
    if config.auto_migrate:
        await asyncio.to_thread(repository.ensure_schema)
    worker_wakeup = asyncio.Event()
    if config.worker_enabled:
        worker_task = asyncio.create_task(persistent_worker_loop())
    if config.artifact_retention_seconds > 0:
        retention_task = asyncio.create_task(retention_cleanup_loop())


@app.on_event('shutdown')
async def shutdown_service() -> None:
    global retention_task, worker_task  # pylint: disable=global-statement
    if worker_task is not None:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        worker_task = None
    if retention_task is not None:
        retention_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await retention_task
        retention_task = None


def _validate_identity(value: str, pattern: re.Pattern[str], label: str) -> str:
    normalized = value.strip()
    if not pattern.fullmatch(normalized):
        raise HTTPException(status_code=400, detail=f'Invalid {label} header')
    return normalized


def require_request_context(
    authorization: str = Header(default=''),
    internal_secret: str = Header(default='', alias='x-icmfyi-internal-secret'),
    tenant_id: str = Header(default='', alias='x-icmfyi-tenant-id'),
    user_id: str = Header(default='', alias='x-icmfyi-user-id'),
) -> RequestContext:
    if config.production:
        if not internal_secret:
            raise HTTPException(status_code=401, detail='Missing internal service secret')
        if not AUTH_TOKEN or not secrets.compare_digest(internal_secret, AUTH_TOKEN):
            raise HTTPException(status_code=401, detail='Invalid internal service secret')
        if not tenant_id:
            raise HTTPException(status_code=400, detail='Missing x-icmfyi-tenant-id header')
        if not user_id:
            raise HTTPException(status_code=400, detail='Missing x-icmfyi-user-id header')
    elif AUTH_TOKEN:
        supplied = internal_secret
        if not supplied and authorization.startswith('Bearer '):
            supplied = authorization[7:].strip()
        if not supplied:
            raise HTTPException(status_code=401, detail='Missing bearer token')
        if not secrets.compare_digest(supplied, AUTH_TOKEN):
            raise HTTPException(status_code=401, detail='Invalid bearer token')

    effective_tenant = tenant_id or 'local'
    effective_user = user_id or 'local'
    tenant_pattern = PRODUCTION_TENANT_ID_RE if config.production else TENANT_ID_RE
    user_pattern = PRODUCTION_USER_ID_RE if config.production else USER_ID_RE
    return RequestContext(
        tenant_id=_validate_identity(effective_tenant, tenant_pattern, 'tenant'),
        user_id=_validate_identity(effective_user, user_pattern, 'user'),
    )


def validate_idempotency_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized or len(normalized) > 200 or any(ord(char) < 33 or ord(char) > 126 for char in normalized):
        raise HTTPException(status_code=400, detail='Invalid Idempotency-Key header')
    return normalized


@app.get('/healthz')
async def healthcheck() -> Dict[str, Any]:
    return {
        'status': True,
        'dry_run': config.dry_run,
        'production': config.production,
        'durable': repository.durable,
        'worker_enabled': config.worker_enabled,
        'retention_enabled': config.artifact_retention_seconds > 0,
    }


@app.get('/readyz')
async def readinesscheck() -> Dict[str, bool]:
    try:
        await asyncio.to_thread(repository.get_for_tenant, '__readiness__', '__readiness__')
    except Exception as exc:  # pylint: disable=broad-except
        raise HTTPException(status_code=503, detail='Clip persistence unavailable') from exc
    return {'status': True, 'persistence': True}


@app.post(
    '/internal/media',
    response_model=MediaRegistrationResponse,
    include_in_schema=not config.production,
)
async def register_canonical_media(
    request: MediaRegistrationRequest,
    context: RequestContext = Depends(require_request_context),
) -> MediaRegistrationResponse:
    if config.production:
        raise HTTPException(status_code=404, detail='Not found')
    try:
        resolved = await asyncio.to_thread(
            resolve_canonical_media_path,
            request.sourcePath,
            request.sourceSha256,
        )
        media = MediaRecord(
            media_id=request.mediaId,
            source_path=str(resolved),
            source_sha256=request.sourceSha256,
            video_capable=request.videoCapable,
        )
        await asyncio.to_thread(repository.register_media, media, [context.tenant_id])
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except MediaConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return MediaRegistrationResponse(
        mediaId=request.mediaId,
        tenantId=context.tenant_id,
        sourceSha256=request.sourceSha256,
        videoCapable=request.videoCapable,
    )


@app.post('/clips', response_model=ClipResponse)
async def create_clip(
    request: ClipRequest,
    background_tasks: BackgroundTasks,
    idempotency_key: Optional[str] = Header(default=None, alias='Idempotency-Key'),
    context: RequestContext = Depends(require_request_context),
) -> ClipResponse:
    if config.production and (not request.mediaId or request.sourceUrl is not None):
        raise HTTPException(
            status_code=400,
            detail='Production clip requests require mediaId and do not accept sourceUrl',
        )
    if request.mediaId:
        try:
            media = await asyncio.to_thread(repository.resolve_media, context.tenant_id, request.mediaId)
        except MediaNotFound as exc:
            raise HTTPException(status_code=404, detail='Canonical media not found') from exc
        if (config.production or request.preferVideo) and not media.video_capable:
            raise HTTPException(status_code=400, detail='Canonical media is not clip-ready video')

    key = validate_idempotency_key(idempotency_key)
    request_hash = normalized_request_hash(request, request.mediaId)
    record = new_job_record(
        id=uuid4().hex,
        tenant_id=context.tenant_id,
        requested_by_user_id=context.user_id,
        media_id=request.mediaId,
        request_hash=request_hash,
        idempotency_key=key,
        payload=request.dict(by_alias=True, exclude_none=True),
        render_profile=request.renderProfile,
        status='queued',
    )
    try:
        persisted, created = await asyncio.to_thread(repository.create_or_get, record)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except PersistenceError as exc:
        raise HTTPException(status_code=503, detail='Clip persistence unavailable') from exc
    if created:
        if worker_wakeup is not None:
            worker_wakeup.set()
        if not config.production:
            background_tasks.add_task(process_persistent_job, persisted.id)
    return persistent_record_response(persisted)


@app.get('/clips/{clip_id}', response_model=ClipResponse)
async def get_clip(
    clip_id: str,
    context: RequestContext = Depends(require_request_context),
) -> ClipResponse:
    record = await asyncio.to_thread(repository.get_for_tenant, clip_id, context.tenant_id)
    if not record:
        raise HTTPException(status_code=404, detail='Clip not found')
    return persistent_record_response(record)


@app.get('/clips/{clip_id}/file')
async def serve_clip_file(
    clip_id: str,
    http_request: Request,
    download: bool = Query(False),
    context: RequestContext = Depends(require_request_context),
) -> Any:
    record = await asyncio.to_thread(repository.get_for_tenant, clip_id, context.tenant_id)
    if not record or record.status != 'ready':
        raise HTTPException(status_code=404, detail='Clip not available')
    output_path = _safe_artifact_path(record)

    filename = output_path.name if download else None
    artifact_bytes = output_path.stat().st_size
    etag_value = output_path.stem if re.fullmatch(r'[0-9a-f]{64}', output_path.stem) else sha256_file(output_path)
    etag = f'"{etag_value}"'
    response_headers = {'Accept-Ranges': 'bytes', 'ETag': etag}
    if download:
        response_headers['Content-Disposition'] = f'attachment; filename="{output_path.name}"'

    range_header = http_request.headers.get('range', '')
    if_range = http_request.headers.get('if-range', '')
    if range_header and (not if_range or secrets.compare_digest(if_range.strip(), etag)):
        try:
            parsed_range = parse_byte_range(range_header, artifact_bytes)
        except ValueError as exc:
            raise HTTPException(
                status_code=416,
                detail=str(exc),
                headers={'Content-Range': f'bytes */{artifact_bytes}', 'Accept-Ranges': 'bytes'},
            ) from exc
        assert parsed_range is not None
        start, end = parsed_range
        response_headers.update({
            'Content-Range': f'bytes {start}-{end}/{artifact_bytes}',
            'Content-Length': str(end - start + 1),
        })
        return StreamingResponse(
            iter_file_range(output_path, start, end),
            status_code=206,
            media_type='video/mp4',
            headers=response_headers,
        )

    return FileResponse(
        output_path,
        media_type='video/mp4',
        filename=filename,
        headers=response_headers,
    )


def ensure_unique_keys(clips: List[BundleClipRequest]) -> None:
    seen = set()
    for clip in clips:
        if clip.key in seen:
            raise HTTPException(status_code=400, detail=f'Duplicate clip key: {clip.key}')
        seen.add(clip.key)


def build_bundle(bundle_request: BundleRequest, context: RequestContext) -> BundleJob:
    ensure_unique_keys(bundle_request.clips)
    clips: Dict[str, BundleClip] = {}
    for clip_req in bundle_request.clips:
        payload = clip_request_from_bundle(clip_req)
        job = Job(id=uuid4().hex, payload=payload)
        store.create(job)
        clips[clip_req.key] = BundleClip(key=clip_req.key, job=job)
    bundle = BundleJob(
        id=uuid4().hex,
        tenant_id=context.tenant_id,
        requested_by_user_id=context.user_id,
        scope=bundle_request.scope,
        clips=clips,
    )
    bundle_store.create(bundle)
    return bundle


@app.post('/clips/batch', response_model=BundleResponse)
async def create_bundle(
    request: BundleRequest,
    background_tasks: BackgroundTasks,
    context: RequestContext = Depends(require_request_context),
) -> BundleResponse:
    if config.production:
        raise HTTPException(status_code=503, detail='Durable batch clips are not enabled in production')
    if not request.clips:
        raise HTTPException(status_code=400, detail='At least one clip is required.')
    bundle = build_bundle(request, context)
    background_tasks.add_task(process_bundle_job, bundle)
    return bundle.to_response()


@app.get('/clips/batch/{batch_id}', response_model=BundleResponse)
async def get_bundle(
    batch_id: str,
    context: RequestContext = Depends(require_request_context),
) -> BundleResponse:
    if config.production:
        raise HTTPException(status_code=503, detail='Durable batch clips are not enabled in production')
    bundle = bundle_store.get(batch_id, context.tenant_id)
    if not bundle:
        raise HTTPException(status_code=404, detail='Bundle not found')
    return bundle.to_response()


@app.get('/clips/batch/{batch_id}/file')
async def serve_bundle_file(
    batch_id: str,
    download: bool = Query(True),
    context: RequestContext = Depends(require_request_context),
) -> FileResponse:
    if config.production:
        raise HTTPException(status_code=503, detail='Durable batch clips are not enabled in production')
    bundle = bundle_store.get(batch_id, context.tenant_id)
    if not bundle or bundle.status != 'ready':
        raise HTTPException(status_code=404, detail='Bundle is not ready')

    if config.dry_run or not bundle.output_path:
        raise HTTPException(status_code=404, detail='Bundle archive unavailable in dry-run mode')

    filename = bundle.output_path.name if download else None
    return FileResponse(
        bundle.output_path,
        media_type='application/zip',
        filename=filename,
    )


@app.patch('/clips/batch/{batch_id}', response_model=BundleResponse)
async def retry_bundle_clip(
    batch_id: str,
    payload: Dict[str, str] = Body(...),
    context: RequestContext = Depends(require_request_context),
) -> BundleResponse:
    if config.production:
        raise HTTPException(status_code=503, detail='Durable batch clips are not enabled in production')
    bundle = bundle_store.get(batch_id, context.tenant_id)
    if not bundle:
        raise HTTPException(status_code=404, detail='Bundle not found')

    clip_key = payload.get('clipKey')
    if not clip_key:
        raise HTTPException(status_code=400, detail='clipKey is required')

    clip = bundle.clips.get(clip_key)
    if not clip:
        raise HTTPException(status_code=404, detail='Clip not found in bundle')

    job = clip.job
    job.status = 'queued'
    job.stream_url = None
    job.download_url = None
    job.error_message = None
    job.output_path = None
    job.updated_at = datetime.utcnow()

    bundle.status = 'processing'
    bundle.download_url = None
    bundle.error_message = None
    bundle.output_path = None
    bundle.updated_at = datetime.utcnow()

    asyncio.create_task(process_bundle_job(bundle))
    return bundle.to_response()
