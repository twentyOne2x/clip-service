# Architecture

## Overview
`icmfyi/clip-service` is a FastAPI service that accepts tenant-scoped clip jobs and serves validated, content-addressed media artifacts.

## Main Components
- `main.py` defines gateway identity enforcement, bounded request validation, the durable worker loop, rendering, ffprobe validation, and atomic filesystem publication.
- `persistence.py` provides the shared repository contract plus PostgreSQL and local in-memory implementations.
- Ingestion migration `20260825_0002_tenant_exports_canonical_media` owns canonical media, hot-local locations, and tenant/channel entitlements.
- `migrations/001_clip_jobs.sql` defines leased clip jobs, idempotency aliases, and published artifacts without duplicating ingestion ownership.
- Docker runtime packages ffmpeg/yt-dlp execution path for non-dry-run mode.
- PostgreSQL is mandatory in production. In-memory persistence is a development/test adapter only.

## Data and Control Flow
1. The app gateway authenticates a user and forwards its internal secret, tenant id, and user id.
2. Ingestion records immutable media bytes, a verified hot-local location, and the tenant/channel entitlement; its `source_videos.id` is the client media id.
3. `POST /clips` validates the fixed render profile and time/size bounds, then commits an idempotent queued job.
4. A worker atomically claims the job with a PostgreSQL lease and heartbeats during rendering.
5. The worker resolves only tenant-authorized media, verifies its SHA-256, and renders within a process timeout.
6. ffprobe validates the MP4; the worker publishes a content-addressed file atomically.
7. The active lease holder commits artifact metadata and the ready transition in one database transaction.
8. Status and file reads filter by tenant and return 404 across tenant boundaries; file reads support a single bounded byte range.
9. Optional retention atomically expires database state before safely unlinking files confined to the artifact root.

## Ops Notes
- Production requires the internal gateway headers and a secret of at least 32 characters.
- Production rejects public `sourceUrl`, dry-run, stream-copy, missing PostgreSQL, and disabled media hashing.
- Production disables the clip-local media-registration endpoint and resolves only ingestion-owned entitled hot-local video bytes.
- Expired leases are reclaimable; exhausted expired attempts transition to terminal error.
- Cancellation and timeout terminate the complete ffmpeg/ffprobe process group. A stale worker cannot publish through an expired lease.
- Retention is disabled by default. Enabled sweeps are metadata-first and path-confined, so interruption can only leave an unreferenced artifact for later cleanup.
- The output volume is writable by uid/gid 10001; canonical media mounts should be read-only.
- Use `scripts/knowledge_check.py` to validate repository knowledge-base hygiene.
