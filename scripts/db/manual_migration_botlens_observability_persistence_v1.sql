-- Manual migration artifact for BotLens durable backend observability.
-- Apply manually; do not execute from application code.

BEGIN;

CREATE SCHEMA IF NOT EXISTS runtime_state;
CREATE SCHEMA IF NOT EXISTS observability_events;
CREATE SCHEMA IF NOT EXISTS observability_metrics;

CREATE TABLE IF NOT EXISTS observability_events.botlens_backend_events_v1 (
    id SERIAL PRIMARY KEY,
    observed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    component VARCHAR(128) NOT NULL,
    event_name VARCHAR(128) NOT NULL,
    level VARCHAR(32) NOT NULL DEFAULT 'INFO',
    bot_id VARCHAR(64),
    run_id VARCHAR(64),
    instrument_id VARCHAR(128),
    series_key VARCHAR(255),
    worker_id VARCHAR(128),
    queue_name VARCHAR(128),
    pipeline_stage VARCHAR(128),
    message_kind VARCHAR(128),
    delta_type VARCHAR(128),
    storage_target VARCHAR(128),
    failure_mode VARCHAR(128),
    phase VARCHAR(128),
    status VARCHAR(128),
    run_seq INTEGER,
    bridge_session_id VARCHAR(128),
    bridge_seq INTEGER,
    message VARCHAR(2048),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_botlens_backend_events_v1_observed_at
    ON observability_events.botlens_backend_events_v1 (observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_events_v1_event_name_observed_at
    ON observability_events.botlens_backend_events_v1 (event_name, observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_events_v1_run_id_observed_at
    ON observability_events.botlens_backend_events_v1 (run_id, observed_at);

CREATE TABLE IF NOT EXISTS observability_metrics.botlens_backend_metric_samples_v1 (
    id SERIAL PRIMARY KEY,
    observed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    component VARCHAR(128) NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    metric_kind VARCHAR(32) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    bot_id VARCHAR(64),
    run_id VARCHAR(64),
    instrument_id VARCHAR(128),
    series_key VARCHAR(255),
    worker_id VARCHAR(128),
    queue_name VARCHAR(128),
    pipeline_stage VARCHAR(128),
    message_kind VARCHAR(128),
    delta_type VARCHAR(128),
    storage_target VARCHAR(128),
    failure_mode VARCHAR(128),
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_samples_v1_observed_at
    ON observability_metrics.botlens_backend_metric_samples_v1 (observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_samples_v1_metric_name_observed_at
    ON observability_metrics.botlens_backend_metric_samples_v1 (metric_name, observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_samples_v1_run_id_observed_at
    ON observability_metrics.botlens_backend_metric_samples_v1 (run_id, observed_at);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_payload_series_key
    ON public.portal_bot_run_events ((payload ->> 'series_key'));

CREATE OR REPLACE VIEW runtime_state.bot_runtime_events_v1 AS
SELECT
    e.id,
    e.event_id,
    e.bot_id,
    e.run_id,
    e.seq,
    e.event_type,
    e.critical,
    e.schema_version,
    e.event_time,
    e.known_at,
    e.created_at,
    e.payload,
    NULLIF(e.payload ->> 'series_key', '') AS series_key,
    NULLIF(e.payload ->> 'bridge_session_id', '') AS bridge_session_id,
    CASE
        WHEN NULLIF(e.payload ->> 'bridge_seq', '') ~ '^-?[0-9]+$'
            THEN (e.payload ->> 'bridge_seq')::INTEGER
        ELSE NULL
    END AS bridge_seq,
    CASE
        WHEN NULLIF(e.payload ->> 'run_seq', '') ~ '^-?[0-9]+$'
            THEN (e.payload ->> 'run_seq')::INTEGER
        ELSE NULL
    END AS run_seq,
    NULLIF(e.payload ->> 'instrument_id', '') AS instrument_id,
    NULLIF(e.payload ->> 'symbol', '') AS symbol,
    NULLIF(e.payload ->> 'timeframe', '') AS timeframe,
    NULLIF(e.payload ->> 'event_name', '') AS runtime_event_name,
    NULLIF(e.payload ->> 'category', '') AS runtime_event_category
FROM public.portal_bot_run_events e;

CREATE OR REPLACE VIEW runtime_state.bot_run_view_state_v1 AS
SELECT *
FROM public.portal_bot_run_view_state;

CREATE OR REPLACE VIEW runtime_state.bot_run_lifecycle_v1 AS
SELECT *
FROM public.portal_bot_run_lifecycle;

CREATE OR REPLACE VIEW runtime_state.bot_run_lifecycle_events_v1 AS
SELECT *
FROM public.portal_bot_run_lifecycle_events;

COMMIT;
