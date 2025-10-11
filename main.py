import asyncio
import hashlib
import os
import secrets
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Tuple
from uuid import uuid4

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query
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
            f'{payload.padBefore}|{payload.padAfter}|{payload.contextMode}'
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


config = ClipServiceConfig(
    dryRun=os.getenv('CLIP_SERVICE_DRY_RUN', 'true').lower() != 'false',
    output_directory=Path(os.getenv('CLIP_OUTPUT_DIRECTORY', '/tmp/clip-service/output')),
    sample_stream_url=os.getenv('CLIP_SAMPLE_STREAM_URL'),
    ffmpegCopyCodec=os.getenv('CLIP_FFMPEG_COPY_CODEC', 'false').lower() == 'true',
)
AUTH_TOKEN = os.getenv('CLIP_SERVICE_AUTH_TOKEN') or os.getenv('CLIP_SERVICE_TOKEN')
store = ClipJobStore()
storage_client: Optional[Any] = None

app = FastAPI(title='HQ Clip Service', version='0.2.0')


def ensure_output_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_output_path(job_id: str, extension: str = 'mp4') -> Path:
    ensure_output_directory(config.output_directory)
    return config.output_directory / f'{job_id}.{extension}'


def parse_gs_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith('gs://'):
        raise ValueError('Invalid GCS URI')
    without_scheme = uri[5:]
    parts = without_scheme.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError('GCS URI must include bucket and object path')
    return parts[0], parts[1]


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


def download_with_ytdlp(url: str, workdir: Path) -> Path:
    if YoutubeDL is None:
        raise RuntimeError('yt-dlp is required to download non-GCS sources')
    output_template = str(workdir / 'source.%(ext)s')
    ydl_opts = {
        'quiet': True,
        'noplaylist': True,
        'outtmpl': output_template,
        'format': 'mp4/mp4-best/best',
        'ignoreerrors': False,
        'no_warnings': True,
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
    return Path(filename).resolve()


async def fetch_source(payload: ClipRequest, workdir: Path) -> Path:
    if not payload.sourceUrl:
        raise ValueError('sourceUrl is required for clip generation when dry-run is disabled')
    source_url = str(payload.sourceUrl)
    if source_url.startswith('gs://'):
        return await asyncio.to_thread(download_from_gcs, source_url, workdir)
    return await asyncio.to_thread(download_with_ytdlp, source_url, workdir)


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

            args = [
                'ffmpeg',
                '-y',
                '-ss',
                f'{start_time:.3f}',
                '-i',
                str(source_path),
                '-t',
                f'{duration:.3f}',
            ]

            if config.ffmpeg_copy_codec:
                args.extend(['-c', 'copy'])
            else:
                args.extend(
                    [
                        '-c:v',
                        'libx264',
                        '-preset',
                        'veryfast',
                        '-c:a',
                        'aac',
                    ]
                )

            args.extend(['-movflags', '+faststart', str(output_path)])

            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()

            if proc.returncode != 0:
                stderr_text = stderr.decode('utf-8', errors='ignore') if stderr else ''
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


async def process_job(job: Job) -> None:
    await run_ffmpeg_clip(job)


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
