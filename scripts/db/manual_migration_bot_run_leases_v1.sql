-- Per-run runner ownership leases.
--
-- Runtime code can provision this table if missing. Use this migration when
-- preparing an existing database intentionally.

CREATE TABLE IF NOT EXISTS portal_bot_run_leases (
    run_id VARCHAR(64) PRIMARY KEY REFERENCES portal_bot_runs(run_id) ON DELETE CASCADE,
    bot_id VARCHAR(64) NOT NULL REFERENCES portal_bots(id) ON DELETE CASCADE,
    runner_id VARCHAR(128) NOT NULL,
    lease_token_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    generation INTEGER NOT NULL DEFAULT 1,
    acquired_at TIMESTAMP NOT NULL DEFAULT NOW(),
    renewed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    released_at TIMESTAMP NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_leases_bot_status_expires
    ON portal_bot_run_leases (bot_id, status, expires_at);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_leases_runner_status
    ON portal_bot_run_leases (runner_id, status);
