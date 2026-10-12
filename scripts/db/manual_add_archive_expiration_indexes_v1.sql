-- Operator-only additive indexes for the existing expiration query.
-- Run using PG_DSN with psql ON_ERROR_STOP=1, outside a transaction, only after
-- resource/ownership admission. These session caps do not grant admission.
-- No fact conversion, data deletion, retention-policy change or runtime hook.
-- A canceled concurrent build may leave an invalid index. Inspect it and any
-- active builder before separately authorizing cleanup/retry; never auto-drop.
-- Compatible completed indexes can remain during code rollback.
\set ON_ERROR_STOP on

SET lock_timeout = '1s';
SET statement_timeout = '5min';
SET maintenance_work_mem = '64MB';

-- Refuse an incompatible existing name before creating either index.
DO $qt$
DECLARE expected record; compatible boolean; index_oid regclass;
BEGIN
    FOR expected IN
        SELECT * FROM (VALUES
                ('ix_market_dataset_archive_ref_manifest', 'market.dataset_archive_refs',
                 ARRAY['raw_archive_manifest_id']::text[]),
                ('ix_market_raw_archive_expiration_order', 'market.raw_archive_manifests',
                 ARRAY['last_received_at', 'id']::text[])
        ) AS specification(index_name, table_name, columns)
    LOOP
        index_oid := to_regclass('market.' || expected.index_name);
        IF index_oid IS NULL THEN
            CONTINUE;
        END IF;
        SELECT i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
               AND i.indpred IS NULL AND i.indexprs IS NULL
               AND i.indnkeyatts = cardinality(expected.columns)
               AND i.indnatts = cardinality(expected.columns)
               AND ARRAY(SELECT pg_get_indexdef(i.indexrelid, n, true)
                         FROM generate_series(1, i.indnkeyatts) AS n) = expected.columns
               AND NOT EXISTS (SELECT 1 FROM unnest(i.indoption) AS option WHERE option <> 0)
               AND am.amname = 'btree'
        INTO compatible
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_am am ON am.oid = idx.relam
        WHERE i.indexrelid = index_oid AND i.indrelid = to_regclass(expected.table_name);
        IF compatible IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'archive_expiration_index_incompatible: %; inspect definition and active build; no replacement performed', expected.index_name;
        END IF;
    END LOOP;
END
$qt$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_dataset_archive_ref_manifest
    ON market.dataset_archive_refs (raw_archive_manifest_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_raw_archive_expiration_order
    ON market.raw_archive_manifests (last_received_at, id);

-- Verify both exact usable definitions before declaring the operation complete.
DO $qt$
DECLARE expected record; compatible boolean; index_oid regclass;
BEGIN
    FOR expected IN
        SELECT * FROM (VALUES
                ('ix_market_dataset_archive_ref_manifest', 'market.dataset_archive_refs',
                 ARRAY['raw_archive_manifest_id']::text[]),
                ('ix_market_raw_archive_expiration_order', 'market.raw_archive_manifests',
                 ARRAY['last_received_at', 'id']::text[])
        ) AS specification(index_name, table_name, columns)
    LOOP
        index_oid := to_regclass('market.' || expected.index_name);
        IF index_oid IS NULL THEN
            RAISE EXCEPTION 'archive_expiration_index_missing: %', expected.index_name;
        END IF;
        SELECT i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
               AND i.indpred IS NULL AND i.indexprs IS NULL
               AND i.indnkeyatts = cardinality(expected.columns)
               AND i.indnatts = cardinality(expected.columns)
               AND ARRAY(SELECT pg_get_indexdef(i.indexrelid, n, true)
                         FROM generate_series(1, i.indnkeyatts) AS n) = expected.columns
               AND NOT EXISTS (SELECT 1 FROM unnest(i.indoption) AS option WHERE option <> 0)
               AND am.amname = 'btree'
        INTO compatible
        FROM pg_index i
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN pg_am am ON am.oid = idx.relam
        WHERE i.indexrelid = index_oid AND i.indrelid = to_regclass(expected.table_name);
        IF compatible IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'archive_expiration_index_incompatible: %; inspect definition and active build; no replacement performed', expected.index_name;
        END IF;
    END LOOP;
END
$qt$;
