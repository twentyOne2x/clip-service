CREATE TABLE IF NOT EXISTS clip_jobs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    requested_by_user_id TEXT NOT NULL,
    media_id TEXT,
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    idempotency_key TEXT,
    payload JSONB NOT NULL,
    render_profile TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'processing', 'ready', 'error', 'expired')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    stream_url TEXT,
    download_url TEXT,
    output_path TEXT,
    error_code TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (status = 'processing' AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR (status <> 'processing' AND lease_owner IS NULL AND lease_until IS NULL)
    ),
    UNIQUE (tenant_id, request_hash),
    UNIQUE (id, tenant_id)
);

CREATE INDEX IF NOT EXISTS clip_jobs_claim_idx
    ON clip_jobs (status, available_at, created_at)
    WHERE status IN ('queued', 'processing');

CREATE TABLE IF NOT EXISTS clip_job_idempotency (
    tenant_id TEXT NOT NULL,
    requested_by_user_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    job_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, requested_by_user_id, idempotency_key),
    FOREIGN KEY (job_id, tenant_id) REFERENCES clip_jobs(id, tenant_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS clip_artifacts (
    job_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    bytes BIGINT NOT NULL CHECK (bytes > 0),
    output_path TEXT NOT NULL,
    validation JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (job_id, tenant_id) REFERENCES clip_jobs(id, tenant_id) ON DELETE CASCADE
);
