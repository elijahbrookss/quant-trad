-- Async-job ownership fencing v1.
--
-- Run manually against an existing PostgreSQL database before starting code
-- that requires claim generations and heartbeats:
--   docker exec -i quant-trad-tsdb-1 psql -U quanttrad -d quanttrad < scripts/db/manual_migration_async_job_fencing_v1.sql
--
-- REQUIRED: stop every backend and worker process first. The script refuses to
-- run while another client session is connected to this database. Old workers
-- do not understand claim tokens and must not resume after rows are migrated.
--
-- Existing running claims cannot carry a token minted by the new worker
-- contract. Requeue them explicitly instead of allowing an unfenced worker to
-- commit after the first installation. Reapplying the migration does not
-- requeue claims created by the fenced implementation.

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND backend_type = 'client backend'
    ) THEN
        RAISE EXCEPTION
            'async job fencing migration requires exclusive database access; stop all backend and worker processes';
    END IF;
END $$;

LOCK TABLE public.portal_async_jobs IN ACCESS EXCLUSIVE MODE;

CREATE TEMP TABLE async_job_fencing_install_state (
    first_install BOOLEAN NOT NULL
) ON COMMIT DROP;

INSERT INTO async_job_fencing_install_state (first_install)
SELECT NOT (
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_async_jobs'
          AND column_name = 'heartbeat_at'
    )
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_async_jobs'
          AND column_name = 'claim_token_hash'
    )
    AND EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_async_jobs'
          AND column_name = 'claim_generation'
    )
);

ALTER TABLE public.portal_async_jobs
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP NULL,
    ADD COLUMN IF NOT EXISTS claim_token_hash VARCHAR(64) NULL,
    ADD COLUMN IF NOT EXISTS claim_generation INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS request_fingerprint VARCHAR(64) NULL;

UPDATE public.portal_async_jobs
SET request_fingerprint = NULLIF(payload ->> 'request_fingerprint', '')
WHERE request_fingerprint IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.portal_async_jobs
        WHERE status IN ('queued', 'running', 'retry')
          AND request_fingerprint IS NOT NULL
        GROUP BY job_type, partition_key, request_fingerprint
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'duplicate in-flight async request identities must be resolved before fencing migration';
    END IF;
END $$;

UPDATE public.portal_async_jobs
SET
    status = 'retry',
    available_at = NOW(),
    updated_at = NOW(),
    error = 'async_job_requeued_for_fencing_migration',
    lock_owner = NULL,
    locked_at = NULL,
    heartbeat_at = NULL,
    claim_token_hash = NULL,
    claim_generation = claim_generation + 1
WHERE status = 'running'
  AND (SELECT first_install FROM async_job_fencing_install_state);

UPDATE public.portal_async_jobs
SET
    lock_owner = NULL,
    locked_at = NULL,
    heartbeat_at = NULL,
    claim_token_hash = NULL
WHERE status <> 'running';

ALTER TABLE public.portal_async_jobs
    DROP CONSTRAINT IF EXISTS ck_portal_async_jobs_claim_generation_nonnegative,
    DROP CONSTRAINT IF EXISTS ck_portal_async_jobs_claim_state;

ALTER TABLE public.portal_async_jobs
    ADD CONSTRAINT ck_portal_async_jobs_claim_generation_nonnegative
        CHECK (claim_generation >= 0),
    ADD CONSTRAINT ck_portal_async_jobs_claim_state
        CHECK (
            (
                status = 'running'
                AND lock_owner IS NOT NULL
                AND locked_at IS NOT NULL
                AND heartbeat_at IS NOT NULL
                AND claim_token_hash IS NOT NULL
            )
            OR
            (
                status <> 'running'
                AND lock_owner IS NULL
                AND locked_at IS NULL
                AND heartbeat_at IS NULL
                AND claim_token_hash IS NULL
            )
        );

DROP INDEX IF EXISTS public.ix_portal_async_jobs_claimable;
DROP INDEX IF EXISTS public.ix_portal_async_jobs_running_heartbeat;
DROP INDEX IF EXISTS public.uq_portal_async_jobs_inflight_request;

CREATE INDEX ix_portal_async_jobs_claimable
    ON public.portal_async_jobs (status, job_type, available_at, created_at);

CREATE INDEX ix_portal_async_jobs_running_heartbeat
    ON public.portal_async_jobs (status, job_type, heartbeat_at);

CREATE UNIQUE INDEX uq_portal_async_jobs_inflight_request
    ON public.portal_async_jobs (
        job_type,
        partition_key,
        request_fingerprint
    )
    WHERE status IN ('queued', 'running', 'retry')
      AND request_fingerprint IS NOT NULL;

COMMIT;
