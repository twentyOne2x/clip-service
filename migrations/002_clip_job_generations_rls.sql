ALTER TABLE clip_jobs
    ADD COLUMN IF NOT EXISTS generation INTEGER NOT NULL DEFAULT 0
    CHECK (generation >= 0);

ALTER TABLE clip_jobs
    DROP CONSTRAINT IF EXISTS clip_jobs_tenant_id_request_hash_key;

CREATE UNIQUE INDEX IF NOT EXISTS clip_jobs_request_generation_unique
    ON clip_jobs (tenant_id, request_hash, generation);

ALTER TABLE clip_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE clip_jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE clip_job_idempotency ENABLE ROW LEVEL SECURITY;
ALTER TABLE clip_job_idempotency FORCE ROW LEVEL SECURITY;
ALTER TABLE clip_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE clip_artifacts FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS clip_jobs_tenant_or_worker ON clip_jobs;
CREATE POLICY clip_jobs_tenant_or_worker ON clip_jobs
    USING (
        CURRENT_USER = 'icmfyi_clip_worker'
        OR tenant_id = NULLIF(current_setting('app.tenant_id', TRUE), '')
    )
    WITH CHECK (
        CURRENT_USER = 'icmfyi_clip_worker'
        OR tenant_id = NULLIF(current_setting('app.tenant_id', TRUE), '')
    );

DROP POLICY IF EXISTS clip_job_idempotency_tenant_or_worker ON clip_job_idempotency;
CREATE POLICY clip_job_idempotency_tenant_or_worker ON clip_job_idempotency
    USING (
        CURRENT_USER = 'icmfyi_clip_worker'
        OR tenant_id = NULLIF(current_setting('app.tenant_id', TRUE), '')
    )
    WITH CHECK (
        CURRENT_USER = 'icmfyi_clip_worker'
        OR tenant_id = NULLIF(current_setting('app.tenant_id', TRUE), '')
    );

DROP POLICY IF EXISTS clip_artifacts_tenant_or_worker ON clip_artifacts;
CREATE POLICY clip_artifacts_tenant_or_worker ON clip_artifacts
    USING (
        CURRENT_USER = 'icmfyi_clip_worker'
        OR tenant_id = NULLIF(current_setting('app.tenant_id', TRUE), '')
    )
    WITH CHECK (
        CURRENT_USER = 'icmfyi_clip_worker'
        OR tenant_id = NULLIF(current_setting('app.tenant_id', TRUE), '')
    );
