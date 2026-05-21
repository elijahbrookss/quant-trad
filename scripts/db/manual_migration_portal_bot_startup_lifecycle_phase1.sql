-- Manual DDL for Phase 1 bot startup lifecycle contract.
-- Safe to run repeatedly on PostgreSQL.

CREATE TABLE IF NOT EXISTS portal_bot_run_lifecycle (
    run_id VARCHAR(64) PRIMARY KEY REFERENCES portal_bot_runs(run_id) ON DELETE CASCADE,
    bot_id VARCHAR(64) NOT NULL REFERENCES portal_bots(id) ON DELETE CASCADE,
    phase VARCHAR(64) NOT NULL DEFAULT 'start_requested',
    status VARCHAR(32) NOT NULL DEFAULT 'starting',
    owner VARCHAR(32) NOT NULL DEFAULT 'backend',
    message VARCHAR(1024),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure JSONB,
    checkpoint_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portal_bot_run_lifecycle_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    run_id VARCHAR(64) NOT NULL REFERENCES portal_bot_runs(run_id) ON DELETE CASCADE,
    bot_id VARCHAR(64) NOT NULL REFERENCES portal_bots(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    phase VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    owner VARCHAR(32) NOT NULL,
    message VARCHAR(1024),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure JSONB,
    checkpoint_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_portal_bot_run_lifecycle_events_event_id
    ON portal_bot_run_lifecycle_events (event_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_portal_bot_run_lifecycle_events_run_seq
    ON portal_bot_run_lifecycle_events (run_id, seq);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_lifecycle_bot_checkpoint
    ON portal_bot_run_lifecycle (bot_id, checkpoint_at DESC);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_lifecycle_events_run_seq
    ON portal_bot_run_lifecycle_events (run_id, seq ASC);
