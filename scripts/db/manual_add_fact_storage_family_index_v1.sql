-- Operator-only additive index; run with psql ON_ERROR_STOP=1 outside a
-- transaction after reviewing disk headroom. Concurrent creation leaves writers
-- running. An interrupted/invalid same-name index must be investigated explicitly.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_fact_storage_family
    ON market.fact_versions (storage_day, fact_type);

DO $qt$
DECLARE compatible boolean;
BEGIN
    SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
           AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 2
           AND pg_get_indexdef(i.indexrelid, 1, true) = 'storage_day'
           AND pg_get_indexdef(i.indexrelid, 2, true) = 'fact_type'
           AND i.indoption::text = '0 0'
           AND am.amname = 'btree'
    INTO compatible
    FROM pg_index i
    JOIN pg_class idx ON idx.oid = i.indexrelid
    JOIN pg_am am ON am.oid = idx.relam
    WHERE i.indexrelid = to_regclass('market.ix_market_fact_storage_family')
      AND i.indrelid = 'market.fact_versions'::regclass;
    IF compatible IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'canonical_storage_family_index_invalid: inspect existing index; no automatic replacement performed';
    END IF;
END
$qt$;
