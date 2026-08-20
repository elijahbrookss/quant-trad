\set ON_ERROR_STOP on

-- Provider-neutral exact numeric fact store v1.
--
-- Run manually after the market-data v2 cutover and shared market fact clock
-- migrations, with backend, collector, worker, and paper-runtime processes
-- stopped:
--
--   make db-file file=scripts/db/manual_migration_numeric_fact_store_v1.sql
--
-- This is additive. The supported rollback is to revert application code and
-- leave the unused immutable relations in place; production DROP rollback is
-- intentionally not provided.

BEGIN;
SELECT pg_advisory_xact_lock(9021010);

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
            'numeric fact migration requires exclusive database access; stop backend, collector, worker, and paper processes';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) THEN
        RAISE EXCEPTION 'numeric fact migration requires TimescaleDB';
    END IF;
    IF to_regclass('market.sources') IS NULL
       OR to_regclass('market.series') IS NULL
       OR to_regclass('market.ingestion_runs') IS NULL
       OR to_regclass('market.fact_commit_seq') IS NULL THEN
        RAISE EXCEPTION
            'numeric fact migration requires market sources, series, ingestion_runs, and fact_commit_seq';
    END IF;
END $$;

LOCK TABLE market.series IN ACCESS EXCLUSIVE MODE;

ALTER TABLE market.series
    ADD COLUMN IF NOT EXISTS dimensions jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'market.series'::regclass
          AND conname = 'ck_market_series_dimensions_object'
    ) THEN
        ALTER TABLE market.series
            ADD CONSTRAINT ck_market_series_dimensions_object
            CHECK (jsonb_typeof(dimensions) = 'object');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS market.numeric_fact_versions (
    series_id bigint NOT NULL
        REFERENCES market.series(id) ON DELETE RESTRICT,
    source_event_key varchar(512) NOT NULL,
    revision integer NOT NULL,
    market_commit_seq bigint NOT NULL
        DEFAULT nextval('market.fact_commit_seq'::regclass),
    ingestion_run_id varchar(64) NOT NULL
        REFERENCES market.ingestion_runs(id) ON DELETE RESTRICT,
    fact_type varchar(64) NOT NULL,
    contract_version varchar(64) NOT NULL,
    numeric_value numeric NOT NULL,
    raw_value text NOT NULL,
    unit varchar(64) NOT NULL,
    dimensions jsonb NOT NULL DEFAULT '{}'::jsonb,
    effective_at timestamptz NOT NULL,
    effective_at_method varchar(64) NOT NULL,
    source_published_at timestamptz,
    received_at timestamptz,
    accepted_at timestamptz NOT NULL,
    known_at timestamptz NOT NULL,
    known_at_method varchar(64) NOT NULL,
    source_event_group_key varchar(512),
    source_event_component_key varchar(256),
    state varchar(16) NOT NULL,
    source_event_material_hash varchar(64) NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    row_hash varchar(64) NOT NULL,
    CONSTRAINT pk_market_numeric_fact_versions
        PRIMARY KEY (series_id, source_event_key, revision),
    CONSTRAINT ck_market_numeric_fact_revision_positive CHECK (revision > 0),
    CONSTRAINT ck_market_numeric_fact_type CHECK (fact_type <> ''),
    CONSTRAINT ck_market_numeric_fact_contract CHECK (contract_version <> ''),
    CONSTRAINT ck_market_numeric_fact_raw_value CHECK (raw_value <> ''),
    CONSTRAINT ck_market_numeric_fact_unit CHECK (unit <> ''),
    CONSTRAINT ck_market_numeric_fact_dimensions_object
        CHECK (jsonb_typeof(dimensions) = 'object'),
    CONSTRAINT ck_market_numeric_fact_state
        CHECK (state IN ('active', 'invalidated')),
    CONSTRAINT ck_market_numeric_fact_known_after_effective
        CHECK (known_at >= effective_at),
    CONSTRAINT ck_market_numeric_fact_known_after_publication
        CHECK (source_published_at IS NULL OR known_at >= source_published_at),
    CONSTRAINT ck_market_numeric_fact_acceptance_after_receipt
        CHECK (received_at IS NULL OR accepted_at >= received_at),
    CONSTRAINT ck_market_numeric_fact_source_material_hash
        CHECK (source_event_material_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_market_numeric_fact_row_hash
        CHECK (row_hash ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS ix_market_numeric_fact_series_time_revision
    ON market.numeric_fact_versions
    (series_id, effective_at DESC, revision DESC);
CREATE INDEX IF NOT EXISTS ix_market_numeric_fact_series_commit
    ON market.numeric_fact_versions (series_id, market_commit_seq);
CREATE INDEX IF NOT EXISTS ix_market_numeric_fact_series_known
    ON market.numeric_fact_versions (series_id, known_at);
CREATE INDEX IF NOT EXISTS ix_market_numeric_fact_event_group
    ON market.numeric_fact_versions (series_id, source_event_group_key);

CREATE TABLE IF NOT EXISTS market.fact_acquisition_coverage (
    identity_key varchar(64) PRIMARY KEY,
    series_id bigint NOT NULL
        REFERENCES market.series(id) ON DELETE RESTRICT,
    source_id bigint NOT NULL
        REFERENCES market.sources(id) ON DELETE RESTRICT,
    binding_id varchar(128) NOT NULL,
    manifest_hash varchar(64) NOT NULL,
    interface_version varchar(64) NOT NULL,
    confirmation_depth integer NOT NULL,
    range_start timestamptz NOT NULL,
    range_end timestamptz NOT NULL,
    source_position_start varchar(128) NOT NULL,
    source_position_end varchar(128) NOT NULL,
    source_position_head varchar(128),
    status varchar(16) NOT NULL,
    ingestion_run_id varchar(64)
        REFERENCES market.ingestion_runs(id) ON DELETE RESTRICT,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_market_fact_acquisition_coverage_range
        CHECK (range_end > range_start),
    CONSTRAINT ck_market_fact_acquisition_coverage_status
        CHECK (status IN ('complete', 'partial', 'failed')),
    CONSTRAINT ck_market_fact_acquisition_confirmation_depth
        CHECK (confirmation_depth >= 0),
    CONSTRAINT ck_market_fact_acquisition_evidence_object
        CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE INDEX IF NOT EXISTS ix_market_fact_acquisition_coverage_lookup
    ON market.fact_acquisition_coverage
    (series_id, binding_id, manifest_hash, status, range_start, range_end);

CREATE OR REPLACE FUNCTION market.reject_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable market-data relation %.% rejects %',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
$$;

DO $$
DECLARE
    table_name text;
    trigger_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'numeric_fact_versions',
        'fact_acquisition_coverage'
    ]
    LOOP
        trigger_name := 'trg_reject_mutation_' || table_name;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_trigger
            WHERE tgname = trigger_name
              AND tgrelid = ('market.' || table_name)::regclass
              AND NOT tgisinternal
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON market.%I '
                'FOR EACH ROW EXECUTE FUNCTION market.reject_immutable_mutation()',
                trigger_name,
                table_name
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    numeric_type text;
    commit_default text;
BEGIN
    SELECT format_type(attribute.atttypid, attribute.atttypmod)
    INTO numeric_type
    FROM pg_attribute AS attribute
    WHERE attribute.attrelid = 'market.numeric_fact_versions'::regclass
      AND attribute.attname = 'numeric_value'
      AND NOT attribute.attisdropped;
    IF numeric_type <> 'numeric' THEN
        RAISE EXCEPTION
            'numeric fact migration verification failed: numeric_value must be unbounded numeric, got %',
            numeric_type;
    END IF;

    SELECT pg_get_expr(default_value.adbin, default_value.adrelid)
    INTO commit_default
    FROM pg_attribute AS attribute
    JOIN pg_attrdef AS default_value
      ON default_value.adrelid = attribute.attrelid
     AND default_value.adnum = attribute.attnum
    WHERE attribute.attrelid = 'market.numeric_fact_versions'::regclass
      AND attribute.attname = 'market_commit_seq';
    IF position('market.fact_commit_seq' in COALESCE(commit_default, '')) = 0 THEN
        RAISE EXCEPTION
            'numeric fact migration verification failed: shared commit clock missing';
    END IF;
END $$;

COMMIT;
