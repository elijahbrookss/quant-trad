\set ON_ERROR_STOP on

-- Add first-class provider identity to immutable market gap evidence.
--
-- Run with market-data writers stopped:
--
--   make db-file file=scripts/db/manual_migration_gap_source_identity_v1.sql

BEGIN;
SELECT pg_advisory_xact_lock(9021011);

DO $$
BEGIN
    IF to_regclass('market.gap_evidence') IS NULL
       OR to_regclass('market.sources') IS NULL
       OR to_regclass('market.ingestion_runs') IS NULL THEN
        RAISE EXCEPTION
            'gap source migration requires market gap_evidence, sources, and ingestion_runs';
    END IF;
END $$;

LOCK TABLE market.gap_evidence IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM market.gap_evidence
        WHERE evidence ? 'source_id'
          AND (evidence->>'source_id') !~ '^[1-9][0-9]*$'
    ) THEN
        RAISE EXCEPTION 'gap evidence contains an invalid source_id projection';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM market.gap_evidence AS gaps
        JOIN market.ingestion_runs AS runs ON runs.id = gaps.ingestion_run_id
        WHERE gaps.evidence ? 'source_id'
          AND (gaps.evidence->>'source_id')::bigint <> runs.source_id
    ) THEN
        RAISE EXCEPTION 'gap evidence source_id disagrees with its ingestion run';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM market.gap_evidence AS gaps
        LEFT JOIN market.sources AS sources
          ON sources.id = (gaps.evidence->>'source_id')::bigint
        WHERE gaps.evidence ? 'source_id'
          AND sources.id IS NULL
    ) THEN
        RAISE EXCEPTION 'gap evidence references an unknown source_id';
    END IF;
END $$;

DROP TRIGGER IF EXISTS trg_reject_mutation_gap_evidence
    ON market.gap_evidence;

ALTER TABLE market.gap_evidence
    ADD COLUMN IF NOT EXISTS source_id bigint;

UPDATE market.gap_evidence AS gaps
SET source_id = runs.source_id
FROM market.ingestion_runs AS runs
WHERE gaps.ingestion_run_id = runs.id
  AND gaps.source_id IS NULL;

UPDATE market.gap_evidence AS gaps
SET source_id = (gaps.evidence->>'source_id')::bigint
WHERE gaps.source_id IS NULL
  AND gaps.evidence ? 'source_id';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'market.gap_evidence'::regclass
          AND conname = 'fk_market_gap_evidence_source'
    ) THEN
        ALTER TABLE market.gap_evidence
            ADD CONSTRAINT fk_market_gap_evidence_source
            FOREIGN KEY (source_id)
            REFERENCES market.sources(id)
            ON DELETE RESTRICT;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_market_gap_evidence_source_window
    ON market.gap_evidence (source_id, start_time, end_time);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM market.gap_evidence AS gaps
        LEFT JOIN market.ingestion_runs AS runs ON runs.id = gaps.ingestion_run_id
        WHERE gaps.source_id IS NULL
          AND (runs.source_id IS NOT NULL OR gaps.evidence ? 'source_id')
    ) THEN
        RAISE EXCEPTION 'gap source migration left resolvable lineage unbound';
    END IF;
END $$;

CREATE TRIGGER trg_reject_mutation_gap_evidence
BEFORE UPDATE OR DELETE ON market.gap_evidence
FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation();

COMMIT;
