-- Tighten run/report storage around lean provenance and fingerprinted artifacts.
--
-- Apply out of band. The application expects these columns to exist after this
-- migration; missing columns fail loud at startup.

BEGIN;

ALTER TABLE public.portal_bot_runs
    ADD COLUMN IF NOT EXISTS config_hash varchar(64),
    ADD COLUMN IF NOT EXISTS material_config_hash varchar(64),
    ADD COLUMN IF NOT EXISTS strategy_hash varchar(64),
    ADD COLUMN IF NOT EXISTS data_snapshot_hash varchar(64),
    ADD COLUMN IF NOT EXISTS runtime_contract_version varchar(64),
    ADD COLUMN IF NOT EXISTS report_dataset_schema_version varchar(64),
    ADD COLUMN IF NOT EXISTS source_revision varchar(128),
    ADD COLUMN IF NOT EXISTS runtime_image varchar(255),
    ADD COLUMN IF NOT EXISTS schema_contract_version varchar(64);

ALTER TABLE public.portal_bot_runs
    DROP COLUMN IF EXISTS decision_ledger;

CREATE INDEX IF NOT EXISTS ix_portal_bot_runs_report_list
    ON public.portal_bot_runs (run_type, status, ended_at DESC, started_at DESC, run_id);

CREATE INDEX IF NOT EXISTS ix_portal_bot_runs_bot_report_list
    ON public.portal_bot_runs (bot_id, run_type, status, ended_at DESC, started_at DESC, run_id);

ALTER TABLE public.portal_report_materializations_v1
    ADD COLUMN IF NOT EXISTS input_fingerprint varchar(64),
    ADD COLUMN IF NOT EXISTS input_fingerprint_payload jsonb,
    ADD COLUMN IF NOT EXISTS source_event_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source_event_high_water_run_seq integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source_trade_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source_run_updated_at timestamp without time zone;

CREATE INDEX IF NOT EXISTS ix_portal_report_materializations_v1_input_fingerprint
    ON public.portal_report_materializations_v1 (input_fingerprint);

-- Old ready artifacts predate input fingerprints. They must be rebuilt through
-- the current report contract rather than served as durable truth.
UPDATE public.portal_report_materializations_v1
SET status = 'stale',
    stale_reason = 'input_fingerprint_missing',
    artifact = NULL,
    artifact_id = NULL,
    cache_key = NULL,
    updated_at = now()
WHERE status = 'ready'
  AND input_fingerprint IS NULL;

-- Step rollups are now phase-duration profiler rows only. Queue depth, lag,
-- worker health, payload-size, and sub-phase debug metrics belong to bounded
-- observability rollups.
DELETE FROM public.portal_bot_run_step_rollups_v1
WHERE metric_name <> 'duration_ms';

COMMIT;
