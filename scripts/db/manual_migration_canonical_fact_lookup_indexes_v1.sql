\set ON_ERROR_STOP on

-- Reconnect before the first server-side statement so even an inherited
-- sub-millisecond statement_timeout cannot cancel the SET that disables it.
\connect -reuse-previous=on "options='-c statement_timeout=0'"

-- Add the two canonical Fact lookup indexes used by derived-lineage validation.
--
-- This is an out-of-band schema operation for an existing canonical Fact store.
-- CREATE INDEX CONCURRENTLY keeps canonical ingestion available, but each build
-- reads the full fact_versions relation. Run one node at a time, monitor database
-- I/O and free space, and do not deploy code that requires these indexes until
-- this script completes successfully:
--
--   make db-file file=scripts/db/manual_migration_canonical_fact_lookup_indexes_v1.sql
--
-- The migration is restart-safe after cancellation. It disables an inherited
-- statement_timeout for this dedicated psql session, serializes concurrent
-- invocations, drops only invalid/not-ready or definition-mismatched target
-- indexes, and preserves every valid target index with the required key order.

SET statement_timeout = 0;

SELECT pg_advisory_lock(9021012);

DO $$
BEGIN
    IF to_regclass('market.fact_versions') IS NULL THEN
        RAISE EXCEPTION
            'canonical fact lookup index migration requires market.fact_versions';
    END IF;
END $$;

SELECT format(
           'DROP INDEX CONCURRENTLY IF EXISTS %I.%I',
           index_namespace.nspname,
           index_class.relname
       )
FROM pg_index AS index_state
JOIN pg_class AS index_class ON index_class.oid = index_state.indexrelid
JOIN pg_namespace AS index_namespace
  ON index_namespace.oid = index_class.relnamespace
JOIN pg_class AS table_class ON table_class.oid = index_state.indrelid
JOIN pg_namespace AS table_namespace
  ON table_namespace.oid = table_class.relnamespace
JOIN pg_am AS access_method ON access_method.oid = index_class.relam
WHERE table_namespace.nspname = 'market'
  AND table_class.relname = 'fact_versions'
  AND index_namespace.nspname = 'market'
  AND index_class.relname IN (
      'ix_market_fact_series_material',
      'ix_market_fact_series_source'
  )
  AND (
      NOT index_state.indisvalid
      OR NOT index_state.indisready
      OR index_state.indisunique
      OR access_method.amname <> 'btree'
      OR index_state.indexprs IS NOT NULL
      OR index_state.indpred IS NOT NULL
      OR index_state.indnkeyatts <> 2
      OR index_state.indnatts <> 2
      OR ARRAY(
          SELECT COALESCE(attribute.attname::text, '<expression>')
          FROM unnest(index_state.indkey::smallint[]) WITH ORDINALITY
              AS index_key(attnum, position)
          LEFT JOIN pg_attribute AS attribute
            ON attribute.attrelid = index_state.indrelid
           AND attribute.attnum = index_key.attnum
          WHERE index_key.position <= index_state.indnkeyatts
          ORDER BY index_key.position
      ) IS DISTINCT FROM CASE index_class.relname
          WHEN 'ix_market_fact_series_material'
              THEN ARRAY['series_id', 'material_hash']::text[]
          WHEN 'ix_market_fact_series_source'
              THEN ARRAY['series_id', 'source_id']::text[]
      END
  )
ORDER BY index_class.relname
\gexec

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_fact_series_material
    ON market.fact_versions (series_id, material_hash);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_fact_series_source
    ON market.fact_versions (series_id, source_id);

DO $$
DECLARE
    mismatched_indexes text;
BEGIN
    WITH expected(index_name, key_columns) AS (
        VALUES
            (
                'ix_market_fact_series_material'::text,
                ARRAY['series_id', 'material_hash']::text[]
            ),
            (
                'ix_market_fact_series_source'::text,
                ARRAY['series_id', 'source_id']::text[]
            )
    ),
    actual AS (
        SELECT
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
                SELECT COALESCE(attribute.attname::text, '<expression>')
                FROM unnest(index_state.indkey::smallint[]) WITH ORDINALITY
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
        JOIN pg_class AS table_class
          ON table_class.oid = index_state.indrelid
        JOIN pg_namespace AS table_namespace
          ON table_namespace.oid = table_class.relnamespace
        JOIN pg_namespace AS index_namespace
          ON index_namespace.oid = index_class.relnamespace
        JOIN pg_am AS access_method
          ON access_method.oid = index_class.relam
        WHERE table_namespace.nspname = 'market'
          AND table_class.relname = 'fact_versions'
          AND index_namespace.nspname = 'market'
          AND index_class.relname IN (
              'ix_market_fact_series_material',
              'ix_market_fact_series_source'
          )
    )
    SELECT string_agg(
               format(
                   '%s expected_keys=%s actual=%s valid=%s ready=%s',
                   expected.index_name,
                   expected.key_columns,
                   COALESCE(actual.definition, '<missing>'),
                   COALESCE(actual.indisvalid::text, '<missing>'),
                   COALESCE(actual.indisready::text, '<missing>')
               ),
               '; ' ORDER BY expected.index_name
           )
    INTO mismatched_indexes
    FROM expected
    LEFT JOIN actual USING (index_name)
    WHERE actual.index_name IS NULL
       OR NOT actual.indisvalid
       OR NOT actual.indisready
       OR actual.indisunique
       OR actual.access_method <> 'btree'
       OR NOT actual.has_plain_columns
       OR NOT actual.is_unfiltered
       OR actual.indnkeyatts <> cardinality(expected.key_columns)
       OR actual.indnatts <> cardinality(expected.key_columns)
       OR actual.key_columns IS DISTINCT FROM expected.key_columns;

    IF mismatched_indexes IS NOT NULL THEN
        RAISE EXCEPTION
            'canonical fact lookup indexes do not match the required definitions: %',
            mismatched_indexes;
    END IF;
END $$;

SELECT pg_advisory_unlock(9021012);
