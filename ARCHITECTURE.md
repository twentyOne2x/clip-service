# Architecture

## Overview
`icmfyi/clip-service` is a FastAPI service that accepts clip jobs and serves generated media artifacts.

## Main Components
- `main.py` defines API endpoints, auth checks, job state machine, and processing logic.
- Docker runtime packages ffmpeg/yt-dlp execution path for non-dry-run mode.
- In-memory state tracks queued/running/completed jobs in the prototype.

## Data and Control Flow
1. Client submits clip payload to `POST /clips`.
2. Service deduplicates and enqueues job.
3. Worker simulates output (dry-run) or downloads source and executes ffmpeg.
4. Client polls status and streams/downloads output file.

## Ops Notes
- Optional bearer token gating via env var.
- Use `scripts/knowledge_check.py` to validate repo knowledge-base hygiene.
