-- Hard cutover from versioned physical names to field-owned versions/provenance.
-- Run manually with:
--   docker exec -i quant-trad-tsdb-1 psql -U quanttrad -d quanttrad < scripts/db/manual_migration_versioning_hard_cutover.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS runtime_state;
CREATE SCHEMA IF NOT EXISTS observability_events;
CREATE SCHEMA IF NOT EXISTS observability_metrics;

DO $$
BEGIN
    IF to_regclass('public.portal_report_materializations') IS NOT NULL
       AND to_regclass('public.portal_report_materializations_v1') IS NOT NULL THEN
        RAISE EXCEPTION 'both portal_report_materializations and portal_report_materializations_v1 exist; resolve before hard cutover';
    END IF;
    IF to_regclass('public.portal_report_materializations') IS NULL
       AND to_regclass('public.portal_report_materializations_v1') IS NOT NULL THEN
        ALTER TABLE public.portal_report_materializations_v1 RENAME TO portal_report_materializations;
    END IF;

    IF to_regclass('public.portal_bot_run_step_rollups') IS NOT NULL
       AND to_regclass('public.portal_bot_run_step_rollups_v1') IS NOT NULL THEN
        RAISE EXCEPTION 'both portal_bot_run_step_rollups and portal_bot_run_step_rollups_v1 exist; resolve before hard cutover';
    END IF;
    IF to_regclass('public.portal_bot_run_step_rollups') IS NULL
       AND to_regclass('public.portal_bot_run_step_rollups_v1') IS NOT NULL THEN
        ALTER TABLE public.portal_bot_run_step_rollups_v1 RENAME TO portal_bot_run_step_rollups;
    END IF;

    IF to_regclass('observability_events.botlens_backend_events') IS NOT NULL
       AND to_regclass('observability_events.botlens_backend_events_v1') IS NOT NULL THEN
        RAISE EXCEPTION 'both observability_events.botlens_backend_events and botlens_backend_events_v1 exist; resolve before hard cutover';
    END IF;
    IF to_regclass('observability_events.botlens_backend_events') IS NULL
       AND to_regclass('observability_events.botlens_backend_events_v1') IS NOT NULL THEN
        ALTER TABLE observability_events.botlens_backend_events_v1 RENAME TO botlens_backend_events;
    END IF;

    IF to_regclass('observability_metrics.botlens_backend_metric_rollups') IS NOT NULL
       AND to_regclass('observability_metrics.botlens_backend_metric_rollups_v1') IS NOT NULL THEN
        RAISE EXCEPTION 'both observability_metrics.botlens_backend_metric_rollups and botlens_backend_metric_rollups_v1 exist; resolve before hard cutover';
    END IF;
    IF to_regclass('observability_metrics.botlens_backend_metric_rollups') IS NULL
       AND to_regclass('observability_metrics.botlens_backend_metric_rollups_v1') IS NOT NULL THEN
        ALTER TABLE observability_metrics.botlens_backend_metric_rollups_v1 RENAME TO botlens_backend_metric_rollups;
    END IF;
END $$;

ALTER INDEX IF EXISTS public.ix_portal_report_materializations_v1_input_fingerprint
    RENAME TO ix_portal_report_materializations_input_fingerprint;
ALTER INDEX IF EXISTS public.ix_portal_bot_run_step_rollups_v1_run_bucket
    RENAME TO ix_portal_bot_run_step_rollups_run_bucket;
ALTER INDEX IF EXISTS public.ix_portal_bot_run_step_rollups_v1_run_step_metric_bucket
    RENAME TO ix_portal_bot_run_step_rollups_run_step_metric_bucket;
ALTER INDEX IF EXISTS public.ix_portal_bot_run_step_rollups_v1_bot_bucket
    RENAME TO ix_portal_bot_run_step_rollups_bot_bucket;
ALTER INDEX IF EXISTS observability_events.ix_botlens_backend_events_v1_observed_at
    RENAME TO ix_botlens_backend_events_observed_at;
ALTER INDEX IF EXISTS observability_events.ix_botlens_backend_events_v1_event_name_observed_at
    RENAME TO ix_botlens_backend_events_event_name_observed_at;
ALTER INDEX IF EXISTS observability_events.ix_botlens_backend_events_v1_run_id_observed_at
    RENAME TO ix_botlens_backend_events_run_id_observed_at;
ALTER INDEX IF EXISTS observability_metrics.ix_botlens_backend_metric_rollups_v1_bucket_start
    RENAME TO ix_botlens_backend_metric_rollups_bucket_start;
ALTER INDEX IF EXISTS observability_metrics.ix_botlens_backend_metric_rollups_v1_metric_bucket
    RENAME TO ix_botlens_backend_metric_rollups_metric_bucket;
ALTER INDEX IF EXISTS observability_metrics.ix_botlens_backend_metric_rollups_v1_run_bucket
    RENAME TO ix_botlens_backend_metric_rollups_run_bucket;

DO $$
BEGIN
    IF to_regclass('public.portal_bot_run_step_rollups') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_constraint
           WHERE conrelid = 'public.portal_bot_run_step_rollups'::regclass
             AND conname = 'uq_portal_bot_run_step_rollups_v1_bucket_identity'
       ) THEN
        ALTER TABLE public.portal_bot_run_step_rollups
            RENAME CONSTRAINT uq_portal_bot_run_step_rollups_v1_bucket_identity
            TO uq_portal_bot_run_step_rollups_bucket_identity;
    END IF;

    IF to_regclass('observability_metrics.botlens_backend_metric_rollups') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_constraint
           WHERE conrelid = 'observability_metrics.botlens_backend_metric_rollups'::regclass
             AND conname = 'uq_botlens_backend_metric_rollups_v1_bucket_identity'
       ) THEN
        ALTER TABLE observability_metrics.botlens_backend_metric_rollups
            RENAME CONSTRAINT uq_botlens_backend_metric_rollups_v1_bucket_identity
            TO uq_botlens_backend_metric_rollups_bucket_identity;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_portal_report_materializations_input_fingerprint
    ON public.portal_report_materializations (input_fingerprint);
CREATE INDEX IF NOT EXISTS ix_portal_bot_run_step_rollups_run_bucket
    ON public.portal_bot_run_step_rollups (run_id, bucket_start);
CREATE INDEX IF NOT EXISTS ix_portal_bot_run_step_rollups_run_step_metric_bucket
    ON public.portal_bot_run_step_rollups (run_id, step_name, metric_name, bucket_start);
CREATE INDEX IF NOT EXISTS ix_portal_bot_run_step_rollups_bot_bucket
    ON public.portal_bot_run_step_rollups (bot_id, bucket_start);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_events_observed_at
    ON observability_events.botlens_backend_events (observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_events_event_name_observed_at
    ON observability_events.botlens_backend_events (event_name, observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_events_run_id_observed_at
    ON observability_events.botlens_backend_events (run_id, observed_at);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_rollups_bucket_start
    ON observability_metrics.botlens_backend_metric_rollups (bucket_start);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_rollups_metric_bucket
    ON observability_metrics.botlens_backend_metric_rollups (metric_name, bucket_start);
CREATE INDEX IF NOT EXISTS ix_botlens_backend_metric_rollups_run_bucket
    ON observability_metrics.botlens_backend_metric_rollups (run_id, bucket_start);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_bot_runs'
          AND column_name = 'source_revision'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_bot_runs'
          AND column_name = 'runtime_source_revision'
    ) THEN
        ALTER TABLE public.portal_bot_runs RENAME COLUMN source_revision TO runtime_source_revision;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_bot_runs'
          AND column_name = 'schema_contract_version'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'portal_bot_runs'
          AND column_name = 'storage_schema_version'
    ) THEN
        ALTER TABLE public.portal_bot_runs RENAME COLUMN schema_contract_version TO storage_schema_version;
    END IF;
END $$;

ALTER TABLE public.portal_bot_runs
    ADD COLUMN IF NOT EXISTS runtime_source_revision varchar(128),
    ADD COLUMN IF NOT EXISTS storage_schema_version varchar(64),
    DROP COLUMN IF EXISTS report_dataset_schema_version,
    DROP COLUMN IF EXISTS source_revision,
    DROP COLUMN IF EXISTS schema_contract_version;

ALTER TABLE public.portal_report_materializations
    ADD COLUMN IF NOT EXISTS report_schema_version varchar(64),
    ADD COLUMN IF NOT EXISTS dataset_schema_version varchar(64),
    ADD COLUMN IF NOT EXISTS builder_source_revision varchar(128),
    ADD COLUMN IF NOT EXISTS storage_schema_version varchar(64);

ALTER TABLE public.portal_report_materializations
    ALTER COLUMN contract_version SET DEFAULT 'run_report.v2';

UPDATE public.portal_report_materializations
SET
    contract_version = CASE
        WHEN contract_version = 'run_report_v2' THEN 'run_report.v2'
        ELSE contract_version
    END,
    report_schema_version = COALESCE(report_schema_version, artifact ->> 'schema_version', 'run_report.v2'),
    dataset_schema_version = COALESCE(dataset_schema_version, artifact #>> '{raw_refs,dataset_schema_version}', 'run_research_dataset.v1'),
    storage_schema_version = COALESCE(storage_schema_version, 'portal_report_materialization_storage.v2'),
    updated_at = now();

DROP VIEW IF EXISTS runtime_state.bot_runtime_events_v1;
DROP VIEW IF EXISTS runtime_state.bot_run_lifecycle_v1;
DROP VIEW IF EXISTS runtime_state.bot_run_lifecycle_events_v1;

CREATE OR REPLACE VIEW runtime_state.bot_runtime_events AS
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
    e.series_key,
    NULLIF(e.payload #>> '{context,bridge_session_id}', '') AS bridge_session_id,
    CASE
        WHEN NULLIF(e.payload #>> '{context,bridge_seq}', '') ~ '^-?[0-9]+$'
            THEN (e.payload #>> '{context,bridge_seq}')::INTEGER
        ELSE NULL
    END AS bridge_seq,
    e.run_seq,
    e.instrument_id,
    e.symbol,
    e.timeframe,
    e.event_name AS runtime_event_name,
    NULLIF(e.payload #>> '{context,category}', '') AS runtime_event_category,
    e.root_id,
    e.correlation_id,
    e.bar_time,
    e.signal_id,
    e.decision_id,
    e.trade_id,
    e.reason_code
FROM public.portal_bot_run_events e;

CREATE OR REPLACE VIEW runtime_state.bot_run_lifecycle AS
SELECT *
FROM public.portal_bot_run_lifecycle;

CREATE OR REPLACE VIEW runtime_state.bot_run_lifecycle_events AS
SELECT *
FROM public.portal_bot_run_lifecycle_events;

COMMIT;
