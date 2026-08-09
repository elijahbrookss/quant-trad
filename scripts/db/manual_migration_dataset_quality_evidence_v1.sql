\set ON_ERROR_STOP on

-- Pin the exact quality evidence used to construct each immutable Dataset
-- series. Historical rows remain NULL intentionally: an empty quality hash is
-- self-proving, while non-empty historical quality cannot be reconstructed
-- safely from the later mutable gap-evidence catalog.
--
-- Run with market-data writers stopped:
--
--   make db-file file=scripts/db/manual_migration_dataset_quality_evidence_v1.sql

BEGIN;
SELECT pg_advisory_xact_lock(9021012);

DO $$
BEGIN
    IF to_regclass('market.dataset_series') IS NULL THEN
        RAISE EXCEPTION
            'dataset quality-evidence migration requires market.dataset_series';
    END IF;
END $$;

LOCK TABLE market.dataset_series IN ACCESS EXCLUSIVE MODE;

ALTER TABLE market.dataset_series
    ADD COLUMN IF NOT EXISTS quality_evidence jsonb;

COMMENT ON COLUMN market.dataset_series.quality_evidence IS
    'Exact quality material frozen with this Dataset series; NULL means historical unpinned quality evidence.';

COMMIT;
