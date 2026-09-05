\set ON_ERROR_STOP on

-- Seed bounded Level 2 operator counters for an existing canonical Fact store.
--
-- This is an out-of-band migration. Stop backend and collector writers first.
-- The seed performs one all-history L2 Fact aggregation, so it deliberately
-- disables query parallelism and has no statement timeout. It is safe to rerun:
-- every counter is recomputed as an absolute value rather than incremented.
--
--   make db-file statement_timeout=0 \
--     file=scripts/db/manual_migration_book_operational_rollups_v1.sql

BEGIN;

SET LOCAL statement_timeout = 0;
SET LOCAL lock_timeout = '30s';
SET LOCAL max_parallel_workers_per_gather = 0;
SET LOCAL max_parallel_maintenance_workers = 0;

SELECT pg_advisory_xact_lock(
    hashtextextended('quant-trad:book-operational-rollups:v1', 0)
);

DO $preflight$
DECLARE
    required_relation text;
BEGIN
    FOREACH required_relation IN ARRAY ARRAY[
        'market.series',
        'market.stream_definitions',
        'market.stream_lease_state',
        'market.fact_versions',
        'market.book_checkpoint_manifests'
    ]
    LOOP
        IF to_regclass(required_relation) IS NULL THEN
            RAISE EXCEPTION
                'book_operational_rollup_migration_blocked: required relation % is missing',
                required_relation;
        END IF;
    END LOOP;
END
$preflight$;

LOCK TABLE market.stream_lease_state IN SHARE MODE;

DO $lease_guard$
DECLARE
    active_lease_count bigint;
BEGIN
    SELECT count(*)
      INTO active_lease_count
      FROM market.stream_lease_state
     WHERE expires_at > now();

    IF active_lease_count > 0 THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_blocked: % active stream leases; stop collectors and wait for leases to expire',
            active_lease_count;
    END IF;
END
$lease_guard$;

LOCK TABLE market.stream_definitions IN SHARE MODE;
LOCK TABLE market.series IN SHARE MODE;
LOCK TABLE market.fact_versions IN SHARE MODE;
LOCK TABLE market.book_checkpoint_manifests IN SHARE MODE;

DO $validate_definition_contracts$
DECLARE
    mismatch text;
    checkpoint_mismatch text;
BEGIN
    SELECT string_agg(
               format(
                   'definition_id=%s series_id=%s definition=%s series=%s',
                   definitions.id,
                   definitions.series_id,
                   definitions.contract_version,
                   series.contract_version
               ),
               '; ' ORDER BY definitions.id
           )
      INTO mismatch
      FROM market.stream_definitions AS definitions
      JOIN market.series AS series
        ON series.id = definitions.series_id
     WHERE definitions.contract_version <> series.contract_version;

    IF mismatch IS NOT NULL THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_blocked: stream definition contract mismatch: %',
            mismatch;
    END IF;

    SELECT string_agg(
               format(
                   'series_id=%s contract_version=%s',
                   scope.series_id,
                   scope.contract_version
               ),
               '; ' ORDER BY scope.series_id
           )
      INTO checkpoint_mismatch
      FROM (
          SELECT DISTINCT checkpoints.series_id,
                 series.contract_version
            FROM market.book_checkpoint_manifests AS checkpoints
            JOIN market.series AS series
              ON series.id = checkpoints.series_id
           WHERE series.contract_version <> 'market.l2_book.v1'
      ) AS scope;

    IF checkpoint_mismatch IS NOT NULL THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_blocked: book checkpoints reference non-L2 series: %',
            checkpoint_mismatch;
    END IF;
END
$validate_definition_contracts$;

DO $create_rollup$
BEGIN
    IF to_regclass('market.book_operational_rollups') IS NULL THEN
        CREATE TABLE market.book_operational_rollups (
            series_id bigint PRIMARY KEY
                REFERENCES market.series(id) ON DELETE CASCADE,
            snapshot_count bigint NOT NULL DEFAULT 0,
            batch_count bigint NOT NULL DEFAULT 0,
            mutation_count bigint NOT NULL DEFAULT 0,
            checkpoint_count bigint NOT NULL DEFAULT 0,
            checkpoint_high_water_acknowledged_at timestamptz,
            checkpoint_high_water_id varchar(128),
            fact_high_water_commit_seq bigint NOT NULL DEFAULT 0,
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT ck_market_book_rollup_snapshot_count
                CHECK (snapshot_count >= 0),
            CONSTRAINT ck_market_book_rollup_batch_count
                CHECK (batch_count >= 0),
            CONSTRAINT ck_market_book_rollup_mutation_count
                CHECK (mutation_count >= 0),
            CONSTRAINT ck_market_book_rollup_checkpoint_count
                CHECK (checkpoint_count >= 0),
            CONSTRAINT ck_market_book_rollup_high_water
                CHECK (fact_high_water_commit_seq >= 0),
            CONSTRAINT ck_market_book_rollup_checkpoint_high_water
                CHECK (
                    (checkpoint_count = 0
                     AND checkpoint_high_water_acknowledged_at IS NULL
                     AND checkpoint_high_water_id IS NULL)
                    OR
                    (checkpoint_count > 0
                     AND checkpoint_high_water_acknowledged_at IS NOT NULL
                     AND checkpoint_high_water_id IS NOT NULL)
                )
        );
    END IF;
END
$create_rollup$;

DO $validate_shape$
DECLARE
    missing_columns text;
BEGIN
    WITH expected(column_name) AS (
        VALUES
            ('series_id'::text),
            ('snapshot_count'::text),
            ('batch_count'::text),
            ('mutation_count'::text),
            ('checkpoint_count'::text),
            ('checkpoint_high_water_acknowledged_at'::text),
            ('checkpoint_high_water_id'::text),
            ('fact_high_water_commit_seq'::text),
            ('updated_at'::text)
    )
    SELECT string_agg(expected.column_name, ', ' ORDER BY expected.column_name)
      INTO missing_columns
      FROM expected
      LEFT JOIN information_schema.columns AS actual
        ON actual.table_schema = 'market'
       AND actual.table_name = 'book_operational_rollups'
       AND actual.column_name = expected.column_name
     WHERE actual.column_name IS NULL;

    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_blocked: market.book_operational_rollups is missing columns: %',
            missing_columns;
    END IF;
END
$validate_shape$;

DO $repair_status_indexes$
DECLARE
    target record;
    exact_definition boolean;
BEGIN
    FOR target IN
        SELECT *
          FROM (
              VALUES
                  (
                      'fact_versions'::text,
                      'ix_market_fact_series_commit'::text,
                      ARRAY['series_id', 'market_commit_seq']::text[],
                      'CREATE INDEX ix_market_fact_series_commit ON market.fact_versions USING btree (series_id, market_commit_seq)'::text
                  ),
                  (
                      'book_checkpoint_manifests'::text,
                      'ix_market_book_checkpoint_series_acknowledged'::text,
                      ARRAY['series_id', 'acknowledged_at', 'id']::text[],
                      'CREATE INDEX ix_market_book_checkpoint_series_acknowledged ON market.book_checkpoint_manifests USING btree (series_id, acknowledged_at, id)'::text
                  )
          ) AS expected(
              table_name,
              index_name,
              key_columns,
              index_definition
          )
    LOOP
        SELECT index_state.indisvalid
               AND index_state.indisready
               AND NOT index_state.indisunique
               AND access_method.amname = 'btree'
               AND index_state.indexprs IS NULL
               AND index_state.indpred IS NULL
               AND index_state.indnkeyatts = cardinality(target.key_columns)
               AND index_state.indnatts = cardinality(target.key_columns)
               AND ARRAY(
                   SELECT COALESCE(
                       attribute.attname::text,
                       '<expression>'
                   )
                     FROM unnest(index_state.indkey::smallint[])
                          WITH ORDINALITY
                          AS index_key(attnum, position)
                     LEFT JOIN pg_attribute AS attribute
                       ON attribute.attrelid = index_state.indrelid
                      AND attribute.attnum = index_key.attnum
                    WHERE index_key.position <= index_state.indnkeyatts
                    ORDER BY index_key.position
               ) IS NOT DISTINCT FROM target.key_columns
               AND pg_get_indexdef(index_state.indexrelid)
                   = target.index_definition
          INTO exact_definition
          FROM pg_index AS index_state
          JOIN pg_class AS index_class
            ON index_class.oid = index_state.indexrelid
          JOIN pg_namespace AS index_namespace
            ON index_namespace.oid = index_class.relnamespace
          JOIN pg_class AS table_class
            ON table_class.oid = index_state.indrelid
          JOIN pg_namespace AS table_namespace
            ON table_namespace.oid = table_class.relnamespace
          JOIN pg_am AS access_method
            ON access_method.oid = index_class.relam
         WHERE index_namespace.nspname = 'market'
           AND index_class.relname = target.index_name
           AND table_namespace.nspname = 'market'
           AND table_class.relname = target.table_name;

        IF COALESCE(exact_definition, false) THEN
            CONTINUE;
        END IF;
        IF to_regclass(format('%I.%I', 'market', target.index_name))
           IS NOT NULL THEN
            EXECUTE format(
                'DROP INDEX %I.%I',
                'market',
                target.index_name
            );
        END IF;
    END LOOP;
END
$repair_status_indexes$;

CREATE INDEX IF NOT EXISTS ix_market_fact_series_commit
    ON market.fact_versions (series_id, market_commit_seq);

CREATE INDEX IF NOT EXISTS ix_market_book_checkpoint_series_acknowledged
    ON market.book_checkpoint_manifests (
        series_id,
        acknowledged_at,
        id
    );

DO $validate_status_indexes$
DECLARE
    mismatched_indexes text;
BEGIN
    WITH expected(
        table_name,
        index_name,
        key_columns,
        index_definition
    ) AS (
        VALUES
            (
                'fact_versions'::text,
                'ix_market_fact_series_commit'::text,
                ARRAY['series_id', 'market_commit_seq']::text[],
                'CREATE INDEX ix_market_fact_series_commit ON market.fact_versions USING btree (series_id, market_commit_seq)'::text
            ),
            (
                'book_checkpoint_manifests'::text,
                'ix_market_book_checkpoint_series_acknowledged'::text,
                ARRAY['series_id', 'acknowledged_at', 'id']::text[],
                'CREATE INDEX ix_market_book_checkpoint_series_acknowledged ON market.book_checkpoint_manifests USING btree (series_id, acknowledged_at, id)'::text
            )
    ),
    actual AS (
        SELECT table_class.relname::text AS table_name,
               index_class.relname::text AS index_name,
               index_state.indisvalid,
               index_state.indisready,
               index_state.indisunique,
               index_state.indnkeyatts,
               index_state.indnatts,
               index_state.indexprs IS NULL AS has_plain_columns,
               index_state.indpred IS NULL AS is_unfiltered,
               access_method.amname::text AS access_method,
               ARRAY(
                   SELECT COALESCE(
                       attribute.attname::text,
                       '<expression>'
                   )
                     FROM unnest(index_state.indkey::smallint[])
                          WITH ORDINALITY
                          AS index_key(attnum, position)
                     LEFT JOIN pg_attribute AS attribute
                       ON attribute.attrelid = index_state.indrelid
                      AND attribute.attnum = index_key.attnum
                    WHERE index_key.position <= index_state.indnkeyatts
                    ORDER BY index_key.position
               ) AS key_columns,
               pg_get_indexdef(index_state.indexrelid) AS definition
          FROM pg_index AS index_state
          JOIN pg_class AS index_class
            ON index_class.oid = index_state.indexrelid
          JOIN pg_namespace AS index_namespace
            ON index_namespace.oid = index_class.relnamespace
          JOIN pg_class AS table_class
            ON table_class.oid = index_state.indrelid
          JOIN pg_namespace AS table_namespace
            ON table_namespace.oid = table_class.relnamespace
          JOIN pg_am AS access_method
            ON access_method.oid = index_class.relam
         WHERE index_namespace.nspname = 'market'
           AND table_namespace.nspname = 'market'
           AND index_class.relname IN (
               'ix_market_fact_series_commit',
               'ix_market_book_checkpoint_series_acknowledged'
           )
    )
    SELECT string_agg(
               format(
                   '%s expected_keys=%s actual=%s',
                   expected.index_name,
                   expected.key_columns,
                   COALESCE(actual.definition, '<missing>')
               ),
               '; ' ORDER BY expected.index_name
           )
      INTO mismatched_indexes
      FROM expected
      LEFT JOIN actual
        ON actual.table_name = expected.table_name
       AND actual.index_name = expected.index_name
     WHERE actual.index_name IS NULL
        OR NOT actual.indisvalid
        OR NOT actual.indisready
        OR actual.indisunique
        OR actual.access_method <> 'btree'
        OR NOT actual.has_plain_columns
        OR NOT actual.is_unfiltered
        OR actual.indnkeyatts <> cardinality(expected.key_columns)
        OR actual.indnatts <> cardinality(expected.key_columns)
        OR actual.key_columns IS DISTINCT FROM expected.key_columns
        OR actual.definition IS DISTINCT FROM expected.index_definition;

    IF mismatched_indexes IS NOT NULL THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_failed: bounded status indexes do not match required definitions: %',
            mismatched_indexes;
    END IF;
END
$validate_status_indexes$;

CREATE TEMP TABLE book_operational_rollup_seed_v1
ON COMMIT DROP
AS
WITH l2_series AS MATERIALIZED (
    SELECT series.id AS series_id
      FROM market.series AS series
     WHERE series.contract_version = 'market.l2_book.v1'
    UNION
    SELECT definitions.series_id
      FROM market.stream_definitions AS definitions
     WHERE definitions.contract_version = 'market.l2_book.v1'
    UNION
    SELECT checkpoints.series_id
      FROM market.book_checkpoint_manifests AS checkpoints
),
fact_counts AS MATERIALIZED (
    SELECT facts.series_id,
           count(*) FILTER (
               WHERE facts.payload ->> 'event_type' = 'snapshot'
           )::bigint AS snapshot_count,
           count(*) FILTER (
               WHERE facts.payload ->> 'event_type' = 'update'
           )::bigint AS batch_count,
           COALESCE(sum(
               CASE WHEN facts.payload ->> 'event_type' = 'update'
                    THEN CAST(facts.payload ->> 'entry_count' AS bigint)
                    ELSE 0
               END
           ), 0)::bigint AS mutation_count,
           COALESCE(max(facts.market_commit_seq), 0)::bigint
               AS fact_high_water_commit_seq,
           count(*)::bigint AS fact_count
      FROM market.fact_versions AS facts
     WHERE facts.payload_schema_id = 'market.l2_book.v1'
     GROUP BY facts.series_id
),
checkpoint_counts AS MATERIALIZED (
    SELECT checkpoints.series_id,
           count(*)::bigint AS checkpoint_count
     FROM market.book_checkpoint_manifests AS checkpoints
     GROUP BY checkpoints.series_id
),
checkpoint_latest AS MATERIALIZED (
    SELECT DISTINCT ON (checkpoints.series_id)
           checkpoints.series_id,
           checkpoints.id AS checkpoint_high_water_id,
           checkpoints.acknowledged_at
               AS checkpoint_high_water_acknowledged_at
      FROM market.book_checkpoint_manifests AS checkpoints
     ORDER BY checkpoints.series_id,
              checkpoints.acknowledged_at DESC,
              checkpoints.id DESC
)
SELECT scope.series_id,
       COALESCE(facts.snapshot_count, 0)::bigint AS snapshot_count,
       COALESCE(facts.batch_count, 0)::bigint AS batch_count,
       COALESCE(facts.mutation_count, 0)::bigint AS mutation_count,
       COALESCE(checkpoint_counts.checkpoint_count, 0)::bigint
           AS checkpoint_count,
       checkpoint_latest.checkpoint_high_water_acknowledged_at,
       checkpoint_latest.checkpoint_high_water_id,
       COALESCE(facts.fact_high_water_commit_seq, 0)::bigint
           AS fact_high_water_commit_seq,
       COALESCE(facts.fact_count, 0)::bigint AS fact_count
  FROM l2_series AS scope
  LEFT JOIN fact_counts AS facts USING (series_id)
  LEFT JOIN checkpoint_counts USING (series_id)
  LEFT JOIN checkpoint_latest USING (series_id);

DO $validate_events$
DECLARE
    invalid_series text;
BEGIN
    SELECT string_agg(
               format(
                   'series_id=%s facts=%s snapshots=%s batches=%s',
                   series_id,
                   fact_count,
                   snapshot_count,
                   batch_count
               ),
               '; ' ORDER BY series_id
           )
      INTO invalid_series
      FROM book_operational_rollup_seed_v1
     WHERE fact_count <> snapshot_count + batch_count;

    IF invalid_series IS NOT NULL THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_blocked: unsupported L2 event types: %',
            invalid_series;
    END IF;
END
$validate_events$;

INSERT INTO market.book_operational_rollups (
    series_id,
    snapshot_count,
    batch_count,
    mutation_count,
    checkpoint_count,
    checkpoint_high_water_acknowledged_at,
    checkpoint_high_water_id,
    fact_high_water_commit_seq,
    updated_at
)
SELECT series_id,
       snapshot_count,
       batch_count,
       mutation_count,
       checkpoint_count,
       checkpoint_high_water_acknowledged_at,
       checkpoint_high_water_id,
       fact_high_water_commit_seq,
       now()
  FROM book_operational_rollup_seed_v1
ON CONFLICT (series_id) DO UPDATE SET
    snapshot_count = EXCLUDED.snapshot_count,
    batch_count = EXCLUDED.batch_count,
    mutation_count = EXCLUDED.mutation_count,
    checkpoint_count = EXCLUDED.checkpoint_count,
    checkpoint_high_water_acknowledged_at =
        EXCLUDED.checkpoint_high_water_acknowledged_at,
    checkpoint_high_water_id = EXCLUDED.checkpoint_high_water_id,
    fact_high_water_commit_seq = EXCLUDED.fact_high_water_commit_seq,
    updated_at = EXCLUDED.updated_at;

DO $verify_seed$
DECLARE
    mismatch text;
BEGIN
    SELECT string_agg(
               format('series_id=%s', expected.series_id),
               ', ' ORDER BY expected.series_id
           )
      INTO mismatch
      FROM book_operational_rollup_seed_v1 AS expected
      LEFT JOIN market.book_operational_rollups AS actual
        USING (series_id)
     WHERE actual.series_id IS NULL
        OR actual.snapshot_count <> expected.snapshot_count
        OR actual.batch_count <> expected.batch_count
        OR actual.mutation_count <> expected.mutation_count
        OR actual.checkpoint_count <> expected.checkpoint_count
        OR actual.checkpoint_high_water_acknowledged_at IS DISTINCT FROM
              expected.checkpoint_high_water_acknowledged_at
        OR actual.checkpoint_high_water_id IS DISTINCT FROM
              expected.checkpoint_high_water_id
        OR actual.fact_high_water_commit_seq
              <> expected.fact_high_water_commit_seq;

    IF mismatch IS NOT NULL THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_failed: seeded counters do not reconcile for %',
            mismatch;
    END IF;
END
$verify_seed$;

CREATE OR REPLACE FUNCTION
    market.record_book_checkpoint_operational_rollup_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE market.book_operational_rollups
    SET checkpoint_count = checkpoint_count + 1,
        checkpoint_high_water_acknowledged_at = CASE
            WHEN checkpoint_count = 0
              OR (NEW.acknowledged_at, NEW.id) >
                 (checkpoint_high_water_acknowledged_at,
                  checkpoint_high_water_id)
            THEN NEW.acknowledged_at
            ELSE checkpoint_high_water_acknowledged_at
        END,
        checkpoint_high_water_id = CASE
            WHEN checkpoint_count = 0
              OR (NEW.acknowledged_at, NEW.id) >
                 (checkpoint_high_water_acknowledged_at,
                  checkpoint_high_water_id)
            THEN NEW.id
            ELSE checkpoint_high_water_id
        END,
        updated_at = now()
    WHERE series_id = NEW.series_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION
            'market_book_operational_rollup_missing: checkpoint insert has no seeded counters series_id=%',
            NEW.series_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS
    trg_record_book_checkpoint_operational_rollup_v1
    ON market.book_checkpoint_manifests;

CREATE TRIGGER trg_record_book_checkpoint_operational_rollup_v1
AFTER INSERT ON market.book_checkpoint_manifests
FOR EACH ROW
EXECUTE FUNCTION market.record_book_checkpoint_operational_rollup_v1();

ALTER TABLE market.book_checkpoint_manifests
ENABLE ALWAYS TRIGGER trg_record_book_checkpoint_operational_rollup_v1;

DO $validate_checkpoint_trigger$
DECLARE
    trigger_ready boolean;
BEGIN
    SELECT EXISTS (
               SELECT 1
                 FROM pg_trigger AS trigger
                 JOIN pg_proc AS procedure
                   ON procedure.oid = trigger.tgfoid
                 JOIN pg_namespace AS procedure_namespace
                   ON procedure_namespace.oid = procedure.pronamespace
                 JOIN pg_language AS language
                   ON language.oid = procedure.prolang
                WHERE trigger.tgname =
                    'trg_record_book_checkpoint_operational_rollup_v1'
                  AND trigger.tgrelid =
                    'market.book_checkpoint_manifests'::regclass
                  AND NOT trigger.tgisinternal
                  AND trigger.tgenabled = 'A'
                  AND trigger.tgtype = 5
                  AND trigger.tgconstraint = 0
                  AND trigger.tgnargs = 0
                  AND trigger.tgqual IS NULL
                  AND trigger.tgoldtable IS NULL
                  AND trigger.tgnewtable IS NULL
                  AND pg_get_triggerdef(trigger.oid, false) =
                      'CREATE TRIGGER trg_record_book_checkpoint_operational_rollup_v1 AFTER INSERT ON market.book_checkpoint_manifests FOR EACH ROW EXECUTE FUNCTION market.record_book_checkpoint_operational_rollup_v1()'
                  AND procedure_namespace.nspname = 'market'
                  AND procedure.proname =
                    'record_book_checkpoint_operational_rollup_v1'
                  AND procedure.prokind = 'f'
                  AND procedure.prorettype = 'trigger'::regtype
                  AND procedure.pronargs = 0
                  AND NOT procedure.proretset
                  AND language.lanname = 'plpgsql'
                  AND procedure.provolatile = 'v'
                  AND procedure.proparallel = 'u'
                  AND NOT procedure.proisstrict
                  AND NOT procedure.prosecdef
                  AND NOT procedure.proleakproof
                  AND procedure.proconfig IS NULL
                  AND md5(
                      btrim(
                          regexp_replace(
                              procedure.prosrc,
                              '[[:space:]]+',
                              ' ',
                              'g'
                          )
                      )
                  ) = '958715b15accfdedbc715d7827ba5ee4'
                  AND has_function_privilege(
                      current_user,
                      procedure.oid,
                      'EXECUTE'
                  )
           )
      INTO trigger_ready;

    IF NOT trigger_ready THEN
        RAISE EXCEPTION
            'book_operational_rollup_migration_failed: durable checkpoint trigger is missing or invalid';
    END IF;
END
$validate_checkpoint_trigger$;

COMMIT;
