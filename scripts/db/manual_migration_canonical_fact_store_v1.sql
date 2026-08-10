\set ON_ERROR_STOP on

-- Schema-registered canonical Fact store v1.
--
-- This migration creates the empty generalized store and immutable payload
-- registry. It performs no runtime cutover and no legacy deletion. Run it with
-- backend, workers, collectors, and paper runtimes stopped:
--
--   make db-file file=scripts/db/manual_migration_canonical_fact_store_v1.sql
--
-- A separately verified hard-cutover migration owns historical population,
-- equivalence validation, reader/writer activation, and old-table removal.

BEGIN;
SELECT pg_advisory_xact_lock(9021011);

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
            'canonical fact store migration requires exclusive database access; stop backend, collectors, workers, and paper runtimes';
    END IF;
    IF to_regclass('market.series') IS NULL
       OR to_regclass('market.sources') IS NULL
       OR to_regclass('market.ingestion_runs') IS NULL
       OR to_regclass('market.fact_commit_seq') IS NULL
       OR to_regclass('market.dataset_series') IS NULL THEN
        RAISE EXCEPTION
            'canonical fact store migration requires market series, sources, ingestion_runs, fact_commit_seq, and dataset_series';
    END IF;
    IF to_regclass('market.candle_versions') IS NULL
       OR to_regclass('market.open_interest_versions') IS NULL
       OR to_regclass('market.funding_rate_versions') IS NULL
       OR to_regclass('market.numeric_fact_versions') IS NULL THEN
        RAISE EXCEPTION
            'canonical fact store migration requires the complete pre-cutover fact store';
    END IF;
END $$;

ALTER TABLE market.dataset_series
    ADD COLUMN IF NOT EXISTS payload_schemas jsonb NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'market.dataset_series'::regclass
          AND conname = 'ck_market_dataset_series_payload_schemas_array'
    ) THEN
        ALTER TABLE market.dataset_series
            ADD CONSTRAINT ck_market_dataset_series_payload_schemas_array
            CHECK (jsonb_typeof(payload_schemas) = 'array');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS market.fact_schemas (
    schema_id varchar(128) PRIMARY KEY,
    fact_type varchar(64) NOT NULL,
    contract_hash varchar(64) NOT NULL,
    contract jsonb NOT NULL,
    observation_time_field varchar(64) NOT NULL,
    material_hash_version varchar(64) NOT NULL,
    row_hash_version varchar(64) NOT NULL,
    query_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
    dataset_eligible boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_market_fact_schema_contract
        UNIQUE (schema_id, fact_type, contract_hash),
    CONSTRAINT uq_market_fact_schema_contract_hash UNIQUE (contract_hash),
    CONSTRAINT ck_market_fact_schema_id CHECK (schema_id <> ''),
    CONSTRAINT ck_market_fact_schema_type CHECK (fact_type <> ''),
    CONSTRAINT ck_market_fact_schema_contract_hash
        CHECK (contract_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_market_fact_schema_contract_object
        CHECK (jsonb_typeof(contract) = 'object'),
    CONSTRAINT ck_market_fact_schema_query_fields_array
        CHECK (jsonb_typeof(query_fields) = 'array')
);

CREATE OR REPLACE FUNCTION market.validate_fact_fields(
    field_contracts jsonb,
    candidate jsonb
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    field_contract jsonb;
    field_name text;
    field_kind text;
    field_value jsonb;
    field_text text;
    required boolean;
    nullable boolean;
    minimum_text text;
    minimum_inclusive boolean;
    numeric_value numeric;
    items_contract jsonb;
    array_item jsonb;
BEGIN
    IF jsonb_typeof(field_contracts) <> 'array'
       OR jsonb_typeof(candidate) <> 'object' THEN
        RETURN false;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_object_keys(candidate) AS candidate_key
        WHERE NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements(field_contracts) AS declared
            WHERE declared->>'name' = candidate_key
        )
    ) THEN
        RETURN false;
    END IF;

    FOR field_contract IN
        SELECT value FROM jsonb_array_elements(field_contracts)
    LOOP
        field_name := field_contract->>'name';
        field_kind := field_contract->>'kind';
        required := COALESCE((field_contract->>'required')::boolean, true);
        nullable := COALESCE((field_contract->>'nullable')::boolean, false);

        IF NOT (candidate ? field_name) THEN
            IF required THEN
                RETURN false;
            END IF;
            CONTINUE;
        END IF;

        field_value := candidate->field_name;
        IF jsonb_typeof(field_value) = 'null' THEN
            IF NOT nullable THEN
                RETURN false;
            END IF;
            CONTINUE;
        END IF;
        field_text := candidate->>field_name;
        numeric_value := NULL;

        IF field_kind IN ('decimal', 'float64') THEN
            IF jsonb_typeof(field_value) <> 'string'
               OR field_text !~ '^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$' THEN
                RETURN false;
            END IF;
            numeric_value := field_text::numeric;
        ELSIF field_kind = 'integer' THEN
            IF jsonb_typeof(field_value) <> 'number'
               OR field_text !~ '^-?(0|[1-9][0-9]*)$' THEN
                RETURN false;
            END IF;
            numeric_value := field_text::numeric;
        ELSIF field_kind = 'string' THEN
            IF jsonb_typeof(field_value) <> 'string' OR field_text = '' THEN
                RETURN false;
            END IF;
            IF jsonb_array_length(COALESCE(field_contract->'enum', '[]'::jsonb)) > 0
               AND NOT (field_contract->'enum' ? field_text) THEN
                RETURN false;
            END IF;
        ELSIF field_kind = 'timestamp' THEN
            IF jsonb_typeof(field_value) <> 'string' THEN
                RETURN false;
            END IF;
            PERFORM field_text::timestamptz;
        ELSIF field_kind = 'boolean' THEN
            IF jsonb_typeof(field_value) <> 'boolean' THEN
                RETURN false;
            END IF;
        ELSIF field_kind = 'object' THEN
            IF jsonb_typeof(field_value) <> 'object' THEN
                RETURN false;
            END IF;
        ELSIF field_kind = 'array' THEN
            IF jsonb_typeof(field_value) <> 'array' THEN
                RETURN false;
            END IF;
            items_contract := field_contract->'items';
            IF items_contract IS NOT NULL
               AND jsonb_typeof(items_contract) <> 'null' THEN
                IF items_contract->>'kind' <> 'object'
                   OR jsonb_typeof(items_contract->'fields') <> 'array' THEN
                    RETURN false;
                END IF;
                FOR array_item IN
                    SELECT value FROM jsonb_array_elements(field_value)
                LOOP
                    IF NOT market.validate_fact_fields(
                        items_contract->'fields',
                        array_item
                    ) THEN
                        RETURN false;
                    END IF;
                END LOOP;
            END IF;
        ELSE
            RETURN false;
        END IF;

        minimum_text := field_contract->>'minimum';
        minimum_inclusive := COALESCE(
            (field_contract->>'minimum_inclusive')::boolean,
            true
        );
        IF minimum_text IS NOT NULL AND numeric_value IS NOT NULL THEN
            IF numeric_value < minimum_text::numeric
               OR (NOT minimum_inclusive AND numeric_value = minimum_text::numeric) THEN
                RETURN false;
            END IF;
        END IF;
    END LOOP;
    RETURN true;
EXCEPTION
    WHEN invalid_text_representation
       OR datetime_field_overflow
       OR numeric_value_out_of_range THEN
        RETURN false;
END;
$$;

CREATE OR REPLACE FUNCTION market.validate_fact_payload(
    requested_schema_id text,
    candidate jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_contract jsonb;
BEGIN
    SELECT contract
    INTO schema_contract
    FROM market.fact_schemas
    WHERE schema_id = requested_schema_id;
    IF schema_contract IS NULL THEN
        RETURN false;
    END IF;
    RETURN market.validate_fact_fields(
        schema_contract->'fields',
        candidate
    );
END;
$$;

CREATE TABLE IF NOT EXISTS market.fact_versions (
    id varchar(64) PRIMARY KEY,
    series_id bigint NOT NULL
        REFERENCES market.series(id) ON DELETE RESTRICT,
    observation_key varchar(512) NOT NULL,
    revision integer NOT NULL,
    market_commit_seq bigint NOT NULL
        DEFAULT nextval('market.fact_commit_seq'::regclass),
    source_id bigint NOT NULL
        REFERENCES market.sources(id) ON DELETE RESTRICT,
    ingestion_run_id varchar(64)
        REFERENCES market.ingestion_runs(id) ON DELETE RESTRICT,
    fact_type varchar(64) NOT NULL,
    payload_schema_id varchar(128) NOT NULL,
    payload_contract_hash varchar(64) NOT NULL,
    observation_time timestamptz NOT NULL,
    observation_time_method varchar(64) NOT NULL,
    source_published_at timestamptz,
    received_at timestamptz,
    accepted_at timestamptz NOT NULL,
    known_at timestamptz NOT NULL,
    known_at_method varchar(64) NOT NULL,
    transformation_id varchar(128) NOT NULL,
    external_event_key varchar(512),
    external_event_group_key varchar(512),
    external_event_component_key varchar(256),
    state varchar(16) NOT NULL,
    payload jsonb NOT NULL,
    payload_hash varchar(64) NOT NULL,
    material_hash varchar(64) NOT NULL,
    provenance_schema_id varchar(64) NOT NULL,
    provenance jsonb NOT NULL DEFAULT '{}'::jsonb,
    provenance_hash varchar(64) NOT NULL,
    quality_schema_id varchar(64) NOT NULL,
    quality jsonb NOT NULL DEFAULT '{}'::jsonb,
    quality_hash varchar(64) NOT NULL,
    row_hash varchar(64) NOT NULL,
    CONSTRAINT uq_market_fact_observation_revision
        UNIQUE (series_id, observation_key, revision),
    CONSTRAINT fk_market_fact_payload_contract
        FOREIGN KEY (payload_schema_id, fact_type, payload_contract_hash)
        REFERENCES market.fact_schemas(schema_id, fact_type, contract_hash)
        ON DELETE RESTRICT,
    CONSTRAINT ck_market_fact_revision_positive CHECK (revision > 0),
    CONSTRAINT ck_market_fact_type CHECK (fact_type <> ''),
    CONSTRAINT ck_market_fact_observation_key CHECK (observation_key <> ''),
    CONSTRAINT ck_market_fact_observation_method
        CHECK (observation_time_method <> ''),
    CONSTRAINT ck_market_fact_known_method CHECK (known_at_method <> ''),
    CONSTRAINT ck_market_fact_transformation CHECK (transformation_id <> ''),
    CONSTRAINT ck_market_fact_state CHECK (state IN ('active', 'invalidated')),
    CONSTRAINT ck_market_fact_acceptance_after_receipt
        CHECK (received_at IS NULL OR accepted_at >= received_at),
    CONSTRAINT ck_market_fact_receipt_known_after_acceptance
        CHECK (
            known_at_method NOT IN (
                'platform_acceptance',
                'platform_receipt',
                'stream_receipt'
            )
            OR known_at >= accepted_at
        ),
    CONSTRAINT ck_market_fact_payload_object
        CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT ck_market_fact_provenance_object
        CHECK (jsonb_typeof(provenance) = 'object'),
    CONSTRAINT ck_market_fact_quality_object
        CHECK (jsonb_typeof(quality) = 'object'),
    CONSTRAINT ck_market_fact_payload_valid
        CHECK (market.validate_fact_payload(payload_schema_id, payload)),
    CONSTRAINT ck_market_fact_payload_hash
        CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_market_fact_material_hash
        CHECK (material_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_market_fact_provenance_hash
        CHECK (provenance_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_market_fact_quality_hash
        CHECK (quality_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_market_fact_row_hash
        CHECK (row_hash ~ '^[0-9a-f]{64}$')
);

-- Provider clocks can lead QT's clock. Causality is enforced on QT receipt,
-- acceptance, and known-at clocks; it is not inferred from external clocks.
ALTER TABLE market.fact_versions
    DROP CONSTRAINT IF EXISTS ck_market_fact_known_after_observation;
ALTER TABLE market.fact_versions
    DROP CONSTRAINT IF EXISTS ck_market_fact_known_after_publication;

CREATE INDEX IF NOT EXISTS ix_market_fact_series_time_revision
    ON market.fact_versions
    (series_id, observation_time DESC, observation_key, revision DESC);
CREATE INDEX IF NOT EXISTS ix_market_fact_series_commit
    ON market.fact_versions (series_id, market_commit_seq);
CREATE INDEX IF NOT EXISTS ix_market_fact_series_known
    ON market.fact_versions (series_id, known_at, observation_time);
CREATE INDEX IF NOT EXISTS ix_market_fact_schema_time
    ON market.fact_versions (payload_schema_id, observation_time);
CREATE INDEX IF NOT EXISTS ix_market_fact_source_time
    ON market.fact_versions (source_id, observation_time);
CREATE INDEX IF NOT EXISTS ix_market_fact_external_group
    ON market.fact_versions (series_id, external_event_group_key);
CREATE INDEX IF NOT EXISTS ix_market_fact_payload_gin
    ON market.fact_versions USING gin (payload jsonb_path_ops);
CREATE INDEX IF NOT EXISTS ix_market_fact_provenance_gin
    ON market.fact_versions USING gin (provenance jsonb_path_ops);
CREATE INDEX IF NOT EXISTS ix_market_fact_exact_value
    ON market.fact_versions
    (series_id, ((payload->>'value')::numeric), observation_time)
    WHERE payload_schema_id IN (
        'derivatives.open_interest.v2',
        'market.reference_price.v1',
        'market.reserve_balance.v1'
    );
CREATE INDEX IF NOT EXISTS ix_market_fact_exact_rate
    ON market.fact_versions
    (series_id, ((payload->>'rate')::numeric), observation_time)
    WHERE payload_schema_id = 'derivatives.funding_rate.v2';

CREATE OR REPLACE FUNCTION market.canonical_fact_utc_timestamp(value text)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
    SELECT (
        substring(value FROM 1 FOR 26)::timestamp without time zone
        AT TIME ZONE 'UTC'
    )
$$;

CREATE INDEX IF NOT EXISTS ix_market_fact_funding_time
    ON market.fact_versions
    (
        series_id,
        market.canonical_fact_utc_timestamp(payload->>'funding_time'),
        observation_time
    )
    WHERE payload_schema_id IN (
        'derivatives.funding_rate.v1',
        'derivatives.funding_rate.v2'
    );

CREATE OR REPLACE FUNCTION market.assert_fact_version_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    series_fact_type text;
    series_contract_version text;
BEGIN
    SELECT fact_type, contract_version
    INTO series_fact_type, series_contract_version
    FROM market.series
    WHERE id = NEW.series_id;
    IF series_fact_type IS NULL THEN
        RAISE EXCEPTION
            'canonical_fact_invalid: unknown series_id=%', NEW.series_id;
    END IF;
    IF NEW.fact_type <> series_fact_type
       OR NEW.payload_schema_id <> series_contract_version THEN
        RAISE EXCEPTION
            'canonical_fact_invalid: series/schema mismatch series_id=% series_fact_type=% series_contract_version=% fact_type=% payload_schema_id=%',
            NEW.series_id,
            series_fact_type,
            series_contract_version,
            NEW.fact_type,
            NEW.payload_schema_id;
    END IF;
    IF NOT market.validate_fact_payload(NEW.payload_schema_id, NEW.payload) THEN
        RAISE EXCEPTION
            'canonical_fact_invalid: payload does not satisfy schema_id=% observation_key=%',
            NEW.payload_schema_id,
            NEW.observation_key;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_assert_fact_version_valid ON market.fact_versions;
CREATE TRIGGER trg_assert_fact_version_valid
BEFORE INSERT ON market.fact_versions
FOR EACH ROW EXECUTE FUNCTION market.assert_fact_version_valid();

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
    FOREACH table_name IN ARRAY ARRAY['fact_schemas', 'fact_versions']
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

\ir canonical_fact_registry_seed_v1.sql

DO $$
DECLARE
    invalid_registry_rows bigint;
    schema_count bigint;
    commit_default text;
BEGIN
    SELECT count(*)
    INTO invalid_registry_rows
    FROM market.fact_schemas
    WHERE contract->>'schema_id' <> schema_id
       OR contract->>'fact_type' <> fact_type
       OR contract->>'observation_time_field' <> observation_time_field
       OR contract->>'material_hash_version' <> material_hash_version
       OR contract->>'row_hash_version' <> row_hash_version
       OR contract->'query_fields' <> query_fields
       OR (contract->>'dataset_eligible')::boolean <> dataset_eligible;
    IF invalid_registry_rows > 0 THEN
        RAISE EXCEPTION
            'canonical fact store verification failed: % registry rows disagree with their contract',
            invalid_registry_rows;
    END IF;

    SELECT count(*) INTO schema_count FROM market.fact_schemas;
    IF schema_count = 0 THEN
        RAISE EXCEPTION
            'canonical fact store verification failed: registry is empty (found %)',
            schema_count;
    END IF;

    SELECT pg_get_expr(default_value.adbin, default_value.adrelid)
    INTO commit_default
    FROM pg_attribute AS attribute
    JOIN pg_attrdef AS default_value
      ON default_value.adrelid = attribute.attrelid
     AND default_value.adnum = attribute.attnum
    WHERE attribute.attrelid = 'market.fact_versions'::regclass
      AND attribute.attname = 'market_commit_seq';
    IF position('market.fact_commit_seq' in COALESCE(commit_default, '')) = 0 THEN
        RAISE EXCEPTION
            'canonical fact store verification failed: shared commit clock missing';
    END IF;
END $$;

COMMIT;
