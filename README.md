# HQ Clip Service (FastAPI Prototype)

This prototype mirrors the Cloud Run service described in `docs/hq-clip-roadmap.md`. It exposes three endpoints:

- `POST /clips` – queue a high-resolution clip job (deduplicated by payload hash)
- `GET /clips/{id}` – poll for job status/URLs
- `GET /clips/{id}/file` – stream or download the generated clip (`?download=1` forces attachment)

In **dry-run mode** (default) the worker simulates queue/processing and returns a sample MP4 URL. Disable dry-run by setting `CLIP_SERVICE_DRY_RUN=false`; the worker will then download the source, run ffmpeg, and serve the generated file from `/clips/{id}/file`.

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

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `CLIP_SERVICE_DRY_RUN` | `true` | When `false`, the worker downloads the source and executes ffmpeg. |
| `CLIP_OUTPUT_DIRECTORY` | `/tmp/clip-service/output` | Directory where generated clips are written when dry-run is disabled. |
| `CLIP_SAMPLE_STREAM_URL` | sample MP4 URL | URL returned during dry-run to emulate a playable clip. |
| `CLIP_FFMPEG_COPY_CODEC` | `false` | When `true`, the worker attempts `-c copy` to avoid re-encoding. |
| `CLIP_SERVICE_AUTH_TOKEN` | — | Optional bearer token required on every request (`Authorization: Bearer …`). If unset, the service runs in open mode. You can also reuse `CLIP_SERVICE_TOKEN` to share a value with the Next.js proxy. |

### Source handling

- `sourceUrl` starting with `gs://` is fetched from Google Cloud Storage (requires `GOOGLE_APPLICATION_CREDENTIALS` or workload identity).
- Any other URL is passed to `yt-dlp` to download the best MP4 rendition before clipping.

### Responses

When a clip finishes, the service sets `streamUrl` to `/clips/{id}/file` and `downloadUrl` to `/clips/{id}/file?download=1`. The Next.js proxy rewrites these relative paths to `/api/clips/{id}/stream` so the frontend can stream/download through the same domain.

## Next steps

- Replace the in-memory job store with Firestore/Datastore (or Redis).
- Integrate Cloud Tasks or Pub/Sub for resilient job execution.
- Upload generated clips to Google Cloud Storage and return signed URLs.
- Emit structured logs/metrics for monitoring and analytics.
