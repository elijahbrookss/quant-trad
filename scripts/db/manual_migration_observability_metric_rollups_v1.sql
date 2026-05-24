-- Manual migration artifact for durable observability metric rollups.
-- Apply manually; do not execute from application code.

BEGIN;

CREATE SCHEMA IF NOT EXISTS observability_metrics;

CREATE TABLE IF NOT EXISTS observability_metrics.botlens_backend_metric_rollups_v1 (
    id SERIAL PRIMARY KEY,
    bucket_start TIMESTAMP NOT NULL,
    bucket_seconds INTEGER NOT NULL DEFAULT 10,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    component VARCHAR(128) NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    metric_kind VARCHAR(32) NOT NULL,
    bot_id VARCHAR(64) NOT NULL DEFAULT '',
    run_id VARCHAR(64) NOT NULL DEFAULT '',
    instrument_id VARCHAR(128) NOT NULL DEFAULT '',
    series_key VARCHAR(255) NOT NULL DEFAULT '',
    worker_id VARCHAR(128) NOT NULL DEFAULT '',
    queue_name VARCHAR(128) NOT NULL DEFAULT '',
    pipeline_stage VARCHAR(128) NOT NULL DEFAULT '',
    message_kind VARCHAR(128) NOT NULL DEFAULT '',
    delta_type VARCHAR(128) NOT NULL DEFAULT '',
    storage_target VARCHAR(128) NOT NULL DEFAULT '',
    failure_mode VARCHAR(128) NOT NULL DEFAULT '',
    label_hash VARCHAR(64) NOT NULL DEFAULT 'none',
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    sample_count INTEGER NOT NULL DEFAULT 0,
    value_sum DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    value_min DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    value_max DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    latest_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p95_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p99_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    raw_sample_count INTEGER NOT NULL DEFAULT 0,
    source_metric_record_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_botlens_backend_metric_rollups_v1_bucket_identity UNIQUE (
        bucket_start,
        bucket_seconds,
        component,
        metric_name,
        metric_kind,
        bot_id,
        run_id,
        instrument_id,
        series_key,
        worker_id,
        queue_name,
        pipeline_stage,
        message_kind,
        delta_type,
        storage_target,
        failure_mode,
        label_hash
    )
);

ALTER TABLE observability_metrics.botlens_backend_metric_rollups_v1
    ADD COLUMN IF NOT EXISTS source_metric_record_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_rollups_v1_bucket_start
    ON observability_metrics.botlens_backend_metric_rollups_v1 (bucket_start);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_rollups_v1_metric_bucket
    ON observability_metrics.botlens_backend_metric_rollups_v1 (metric_name, bucket_start);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_rollups_v1_run_bucket
    ON observability_metrics.botlens_backend_metric_rollups_v1 (run_id, bucket_start);

COMMIT;
