# HQ Clip Service

The service renders bounded, high-quality MP4 clips from canonical media. PostgreSQL is the production control plane for tenant-scoped media access, idempotent jobs, worker leases, retries, and immutable artifact metadata.

Primary endpoints:

- `POST /clips` – queue a high-resolution clip job (deduplicated by payload hash)
- `POST /clips/{id}/retry` – create one idempotent new generation from a terminal error/expired job
- `GET /clips/{id}` – poll for job status/URLs
- `GET /clips/{id}/file` – stream or download the generated clip (`?download=1` forces attachment)
- `POST /internal/media` – local-development media fixture endpoint (disabled and hidden in production)
- `GET /healthz` and `GET /readyz` – process and persistence health

Local development defaults to an in-memory repository and dry-run rendering. Production fails closed unless PostgreSQL, a strong internal secret, real rendering, canonical media roots, and SHA-256 verification are configured. Arbitrary `sourceUrl` acquisition is never accepted in production.

## Running locally

```bash
cd clip-service
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8080
```

Visit `http://localhost:8080/docs` for the interactive OpenAPI UI.

## Docker

```bash
docker build -t hq-clip-service .
docker run --rm -p 8080:8080 hq-clip-service
```

The image runs as uid/gid `10001`, pins the Python base-image manifest and Deno release assets, and verifies each Deno archive against its official SHA-256 digest. Mount `/var/lib/icmfyi/clips` writable for output and mount every `CLIP_MEDIA_ROOTS` entry read-only.

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `CLIP_SERVICE_DRY_RUN` | `true` | When `false`, the worker downloads the source and executes ffmpeg. |
| `CLIP_SERVICE_ENV` | — | Set to `production` to enable fail-closed production gates. |
| `CLIP_DATABASE_URL` | — | PostgreSQL DSN. Required in production. |
| `INTERNAL_SERVICE_SECRET` | — | Shared app-gateway secret, at least 32 characters in production. `CLIP_SERVICE_AUTH_TOKEN` is the compatible fallback name. |
| `CLIP_MEDIA_ROOTS` | `/srv/icmfyi/media` in production | Comma-separated absolute roots containing canonical media; mount read-only. |
| `CLIP_OUTPUT_DIRECTORY` | `/var/lib/icmfyi/clips` in the image | Writable artifact root. |
| `CLIP_SAMPLE_STREAM_URL` | sample MP4 URL | URL returned during dry-run to emulate a playable clip. |
| `CLIP_FFMPEG_COPY_CODEC` | `false` | Development-only stream-copy option; prohibited in production. |
| `CLIP_VERIFY_MEDIA_SHA256` | production: `true` | Re-hash canonical media before each render; cannot be disabled in production. |
| `CLIP_WORKER_LEASE_SECONDS` | `120` | Durable worker lease, bounded to 15–3600 seconds. |
| `CLIP_RENDER_TIMEOUT_SECONDS` | `1800` | ffmpeg wall-clock limit, bounded to 30–7200 seconds. |
| `CLIP_ARTIFACT_RETENTION_SECONDS` | `0` | Artifact retention age. `0` disables deletion; production values must be at least 3600 seconds. |
| `CLIP_RETENTION_CHECK_SECONDS` | `300` | Interval between bounded retention sweeps. |
| `CLIP_MAX_CLIP_SECONDS` | `600` | Maximum requested clip duration, hard-capped at 1800 seconds. |
| `CLIP_MAX_PADDING_SECONDS` | `30` | Maximum padding on either side. |
| `CLIP_MAX_ARTIFACT_BYTES` | `2147483648` | Maximum published MP4 size. |
| `CLIP_AUTO_MIGRATE` | `true` | Apply every ordered idempotent SQL migration under `migrations/` at startup. |

The ingestion Alembic migration `20260825_0002_tenant_exports_canonical_media` must run before clip traffic because it owns the shared canonical-media and entitlement tables. A separate clip migration service is not required while `CLIP_AUTO_MIGRATE=true`. Deployments using a least-privileged runtime role can instead run the following one-shot command with a DDL role, then set `CLIP_AUTO_MIGRATE=false` for the service:

```bash
python -c "import os; from persistence import create_repository; create_repository(os.environ['CLIP_DATABASE_URL']).ensure_schema()"
```

## Production gateway contract

The public app authenticates the caller and must strip any client-supplied internal identity headers. It then forwards all three values below:

- `x-icmfyi-internal-secret`: `INTERNAL_SERVICE_SECRET`
- `x-icmfyi-tenant-id`: validated production scope `ten_<64 lowercase hex>`
- `x-icmfyi-user-id`: validated production scope `usr_<64 lowercase hex>`

The clip service never derives tenancy from request payloads. Production rejects legacy ids, uppercase hashes, and swapped `usr_`/`ten_` prefixes. Create, status, file, batch, and retry lookups are scoped by the forwarded tenant. Development retains the historical bearer-token path, accepts its legacy broad ids, and uses `local` tenant/user defaults when identity headers are absent.

`POST /clips` accepts `Idempotency-Key`. Reuse by the same tenant/user with different normalized clip parameters returns HTTP 409. Identical normalized requests deduplicate within a tenant. `POST /clips/{id}/retry` requires a new `Idempotency-Key` and is allowed only when the tenant-owned source job is `error` or `expired`. It copies the exact persisted request into the next numeric generation. Repeated keys and concurrent distinct retry intents converge on the same queued/processing/ready generation, preventing duplicate rendering effects.

`migrations/002_clip_job_generations_rls.sql` forces PostgreSQL row-level security on clip jobs, retry aliases, and artifacts. The API transaction sets `app.tenant_id` before every tenant read or write. Production runs the HTTP API as fixed `icmfyi_clip_api` with a read-only artifact mount and runs the sole renderer/retention loop as fixed `icmfyi_clip_worker` with the writable artifact mount. Both roles are non-owner, `NOBYPASSRLS`; only the worker policy can claim across tenants.

## Canonical media and rendering

Production media ids are ingestion-owned `source_videos.id` values. Resolution is a read-only entitlement join through `tenant_channel_entitlements`, active channels/videos, an active `source_video` (preferred) or `proxy` media ref, an active video `media_object`, and its verified active `hot_local` `media_location`. The location byte count must match the media object. A missing entitlement, inactive row, unverified location, non-video MIME type, or Storage-Box-only object is indistinguishable from an unknown media id. The worker resolves the same chain again after claiming a job, so revoked access fails before rendering.

`POST /internal/media` exists only for local in-memory fixtures. It returns `404` and is omitted from OpenAPI in production; callers cannot grant themselves a path or media entitlement through the clip service.

Production `POST /clips` requires that `mediaId`; `sourceUrl` is rejected. The fixed `hq-1080p-v1` profile uses H.264 CRF 18, AAC 192 kb/s, yuv420p, fast-start MP4, and at most 1080p without upscaling. ffprobe validates duration and audio/video streams before publication. Artifacts are staged, fsynced, named by SHA-256, and atomically moved to `artifacts/<job>/<sha256>.mp4`; PostgreSQL then commits artifact metadata and the ready transition in one transaction.

### Development source handling

- `sourceUrl` starting with `gs://` is fetched from Google Cloud Storage (requires `GOOGLE_APPLICATION_CREDENTIALS` or workload identity).
- Any other URL is passed to `yt-dlp` to download the best MP4 rendition before clipping.

### Responses

When a clip finishes, the service sets `streamUrl` to `/clips/{id}/file` and `downloadUrl` to `/clips/{id}/file?download=1`. The Next.js proxy rewrites these relative paths to `/api/clips/{id}/stream` so the frontend can stream/download through the same domain.

Artifact delivery supports one RFC-style byte range (`bytes=start-end`, open-ended, or suffix) and returns `206`, `Content-Range`, `Accept-Ranges`, and a SHA-derived `ETag`. Invalid or multi-range requests fail with `416`; an `If-Range` value that does not match the artifact ETag receives the complete file. Tenant authorization is checked before any file metadata or bytes are exposed.

Retention is deliberately opt-in. When `CLIP_ARTIFACT_RETENTION_SECONDS` is nonzero, each sweep first atomically marks eligible ready jobs `expired` and removes their artifact rows in PostgreSQL, then unlinks only regular files that resolve beneath the configured output root. A crash or unlink error can therefore leave an unreferenced file, but cannot leave a database-ready job pointing at deleted bytes or delete media outside the clip volume. Status remains tenant-scoped and reports `expired`; the file endpoint returns `404`.

Legacy batch ZIP endpoints remain available only in development because their aggregate bundle state is in memory. They are tenant-scoped and fail closed with HTTP 503 in production; production clients should submit durable clip jobs individually and bundle downloaded artifacts at the app layer.
