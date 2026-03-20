import asyncio
import hashlib
import os
import re
import secrets
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
from uuid import uuid4

from fastapi import BackgroundTasks, Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
try:
    from google.cloud import storage  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for GCS downloads
    storage = None  # type: ignore
from pydantic import BaseModel, Field, HttpUrl, validator
try:
    from yt_dlp import YoutubeDL  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for web downloads
    YoutubeDL = None  # type: ignore


ClipStatus = Literal['queued', 'processing', 'ready', 'error']
ContextMode = Literal['seconds', 'sentence']


class ClipRequest(BaseModel):
    sourceUrl: Optional[HttpUrl] = Field(None, alias='sourceUrl')
    parentTitle: Optional[str]
    clipLabel: Optional[str]
    channel: Optional[str]
    # If true, clip from real video (frames) instead of local mirrored audio (thumbnail-backed MP4).
    preferVideo: bool = Field(default=False, alias='preferVideo')
    start: float = Field(..., ge=0)
    end: float = Field(..., gt=0)
    contextMode: ContextMode = Field(..., alias='contextMode')
    padBefore: float = Field(..., ge=0, alias='padBefore')
    padAfter: float = Field(..., ge=0, alias='padAfter')

    @validator('end')
    def validate_end(cls, value: float, values: Dict[str, float]) -> float:  # pylint: disable=no-self-argument
        start = values.get('start')
        if start is not None and value <= start:
            raise ValueError('end must be greater than start')
        return value

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


class BundleClipRequest(BaseModel):
    key: str
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

    @validator('end')
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

    def get(self, bundle_id: str) -> Optional[BundleJob]:
        return self._bundles.get(bundle_id)

    def create(self, bundle: BundleJob) -> BundleJob:
        self._bundles[bundle.id] = bundle
        return bundle


config = ClipServiceConfig(
    dryRun=os.getenv('CLIP_SERVICE_DRY_RUN', 'true').lower() != 'false',
    output_directory=Path(os.getenv('CLIP_OUTPUT_DIRECTORY', '/tmp/clip-service/output')),
    sample_stream_url=os.getenv('CLIP_SAMPLE_STREAM_URL'),
    ffmpegCopyCodec=os.getenv('CLIP_FFMPEG_COPY_CODEC', 'false').lower() == 'true',
)
AUTH_TOKEN = os.getenv('CLIP_SERVICE_AUTH_TOKEN') or os.getenv('CLIP_SERVICE_TOKEN')
store = ClipJobStore()
bundle_store = BundleJobStore()
storage_client: Optional[Any] = None

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
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
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


async def run_ffmpeg_clip(job: Job) -> None:
    job.status = 'processing'
    job.updated_at = datetime.utcnow()

    if config.dry_run:
        await asyncio.sleep(2.5)
        job.stream_url = config.sample_stream_url
        job.download_url = config.sample_stream_url
        job.status = 'ready'
        job.updated_at = datetime.utcnow()
        return

    try:
        with tempfile.TemporaryDirectory(prefix=f'clip-job-{job.id}-') as tmp:
            workdir = Path(tmp)
            source_path = await fetch_source(job.payload, workdir)
            if not source_path.exists():
                raise FileNotFoundError('Downloaded source not found')

            output_path = build_output_path(job.id)

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
                        "veryfast",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
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
                        "veryfast",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "aac",
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
                        "veryfast",
                        "-c:a",
                        "aac",
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
                        "veryfast",
                        "-c:a",
                        "aac",
                        "-movflags",
                        "+faststart",
                        str(output_path),
                    ]

            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            if proc.returncode != 0:
                stderr_text = stderr.decode('utf-8', errors='ignore') if stderr else ''
                if fallback_args:
                    try:
                        if output_path.exists():
                            output_path.unlink()
                    except Exception:
                        pass
                    print('[clip-service] ffmpeg copy cut failed; retrying with transcode', flush=True)
                    proc = await asyncio.create_subprocess_exec(
                        *fallback_args,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    _, stderr = await proc.communicate()
                    if proc.returncode != 0:
                        stderr_text = stderr.decode('utf-8', errors='ignore') if stderr else ''
                        raise RuntimeError(stderr_text or 'ffmpeg exited with non-zero status')
                else:
                    raise RuntimeError(stderr_text or 'ffmpeg exited with non-zero status')

            job.output_path = output_path
            job.stream_url = f'/clips/{job.id}/file'
            job.download_url = f'/clips/{job.id}/file?download=1'
            job.status = 'ready'
        job.updated_at = datetime.utcnow()
    except Exception as exc:  # pylint: disable=broad-except
        job.status = 'error'
        job.error_message = str(exc)
        job.updated_at = datetime.utcnow()


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


def verify_auth_header(authorization: str = Header(default='')) -> None:
    if not AUTH_TOKEN:
        return
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail='Missing bearer token')
    token = authorization[7:].strip()
    if not token or not secrets.compare_digest(token, AUTH_TOKEN):
        raise HTTPException(status_code=401, detail='Invalid bearer token')


@app.get('/healthz')
async def healthcheck() -> Dict[str, bool]:
    return {'status': True, 'dry_run': config.dry_run}


@app.post('/clips', response_model=ClipResponse)
async def create_clip(
    request: ClipRequest,
    background_tasks: BackgroundTasks,
    _: None = Depends(verify_auth_header),
) -> ClipResponse:
    existing = store.find_existing(request)
    if existing:
        return existing.to_response()

    job = Job(id=uuid4().hex, payload=request)
    store.create(job)

    background_tasks.add_task(process_job, job)
    return job.to_response()


@app.get('/clips/{clip_id}', response_model=ClipResponse)
async def get_clip(clip_id: str, _: None = Depends(verify_auth_header)) -> ClipResponse:
    job = store.get(clip_id)
    if not job:
        raise HTTPException(status_code=404, detail='Clip not found')
    return job.to_response()


@app.get('/clips/{clip_id}/file')
async def serve_clip_file(
    clip_id: str,
    download: bool = Query(False),
    _: None = Depends(verify_auth_header),
) -> FileResponse:
    job = store.get(clip_id)
    if not job or job.status != 'ready' or not job.output_path or not job.output_path.exists():
        raise HTTPException(status_code=404, detail='Clip not available')

    filename = job.output_path.name if download else None
    return FileResponse(
        job.output_path,
        media_type='video/mp4',
        filename=filename,
    )


def ensure_unique_keys(clips: List[BundleClipRequest]) -> None:
    seen = set()
    for clip in clips:
        if clip.key in seen:
            raise HTTPException(status_code=400, detail=f'Duplicate clip key: {clip.key}')
        seen.add(clip.key)


def build_bundle(bundle_request: BundleRequest) -> BundleJob:
    ensure_unique_keys(bundle_request.clips)
    clips: Dict[str, BundleClip] = {}
    for clip_req in bundle_request.clips:
        payload = clip_request_from_bundle(clip_req)
        job = Job(id=uuid4().hex, payload=payload)
        store.create(job)
        clips[clip_req.key] = BundleClip(key=clip_req.key, job=job)
    bundle = BundleJob(id=uuid4().hex, scope=bundle_request.scope, clips=clips)
    bundle_store.create(bundle)
    return bundle


@app.post('/clips/batch', response_model=BundleResponse)
async def create_bundle(
    request: BundleRequest,
    background_tasks: BackgroundTasks,
    _: None = Depends(verify_auth_header),
) -> BundleResponse:
    if not request.clips:
        raise HTTPException(status_code=400, detail='At least one clip is required.')
    bundle = build_bundle(request)
    background_tasks.add_task(process_bundle_job, bundle)
    return bundle.to_response()


@app.get('/clips/batch/{batch_id}', response_model=BundleResponse)
async def get_bundle(
    batch_id: str,
    _: None = Depends(verify_auth_header),
) -> BundleResponse:
    bundle = bundle_store.get(batch_id)
    if not bundle:
        raise HTTPException(status_code=404, detail='Bundle not found')
    return bundle.to_response()


@app.get('/clips/batch/{batch_id}/file')
async def serve_bundle_file(
    batch_id: str,
    download: bool = Query(True),
    _: None = Depends(verify_auth_header),
) -> FileResponse:
    bundle = bundle_store.get(batch_id)
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
    _: None = Depends(verify_auth_header),
) -> BundleResponse:
    bundle = bundle_store.get(batch_id)
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
