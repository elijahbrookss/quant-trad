-- Operator-only additive index. Run with psql ON_ERROR_STOP=1 outside a
-- transaction after checking disk/WAL headroom. Rollback can leave it in place.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_fact_series_accepted
    ON market.fact_versions (series_id, accepted_at, market_commit_seq);

DO $qt$
DECLARE compatible boolean;
BEGIN
    SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
           AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 3
           AND pg_get_indexdef(i.indexrelid, 1, true) = 'series_id'
           AND pg_get_indexdef(i.indexrelid, 2, true) = 'accepted_at'
           AND pg_get_indexdef(i.indexrelid, 3, true) = 'market_commit_seq'
           AND i.indoption::text = '0 0 0'
           AND am.amname = 'btree'
    INTO compatible
    FROM pg_index i
    JOIN pg_class idx ON idx.oid = i.indexrelid
    JOIN pg_am am ON am.oid = idx.relam
    WHERE i.indexrelid = to_regclass('market.ix_market_fact_series_accepted')
      AND i.indrelid = 'market.fact_versions'::regclass;
    IF compatible IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'canonical_series_accepted_index_invalid: inspect existing index; no automatic replacement performed';
    END IF;
END
$qt$;
