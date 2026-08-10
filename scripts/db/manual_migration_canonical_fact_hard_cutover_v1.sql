\set ON_ERROR_STOP on

-- Destructive final cutover after migrate_canonical_fact_data_v1.py --execute.
-- Stop every backend, collector, worker, and paper runtime before applying.
-- A verified pre-migration backup is mandatory; see the operator runbook.

BEGIN;

SELECT pg_advisory_xact_lock(9021012);

DO $$
DECLARE
    other_clients bigint;
    legacy_relation text;
    legacy_count bigint;
    canonical_count bigint;
BEGIN
    SELECT count(*)
    INTO other_clients
    FROM pg_stat_activity
    WHERE datname = current_database()
      AND pid <> pg_backend_pid()
      AND backend_type = 'client backend';
    IF other_clients > 0 THEN
        RAISE EXCEPTION
            'canonical Fact hard cutover requires exclusive client access; found % other client(s)',
            other_clients;
    END IF;

    IF to_regclass('market.fact_versions') IS NULL THEN
        RAISE EXCEPTION 'canonical Fact hard cutover requires market.fact_versions';
    END IF;

    FOREACH legacy_relation IN ARRAY ARRAY[
        'market.candle_versions',
        'market.open_interest_versions',
        'market.funding_rate_versions',
        'market.numeric_fact_versions',
        'market.market_trade_versions',
        'market.trade_flow_aggregate_versions',
        'market.l2_snapshot_versions',
        'market.l2_mutation_batches',
        'market.bbo_feature_versions',
        'market.depth_feature_versions',
        'market.trade_flow_feature_versions',
        'market.futures_spot_relationship_versions',
        'market.derivative_state_versions',
        'market.market_response_feature_versions',
        'market.normalized_feature_versions'
    ]
    LOOP
        IF to_regclass(legacy_relation) IS NULL THEN
            RAISE EXCEPTION
                'canonical Fact hard cutover boundary is incomplete: % is missing',
                legacy_relation;
        END IF;
        EXECUTE format('SELECT count(*) FROM %s', legacy_relation)
        INTO legacy_count;
        SELECT count(*)
        INTO canonical_count
        FROM market.fact_versions
        WHERE provenance -> '_qt_migration' ->> 'source_table' = legacy_relation;
        IF canonical_count <> legacy_count THEN
            RAISE EXCEPTION
                'canonical Fact hard cutover validation failed for %: legacy=% canonical_migrated=%',
                legacy_relation,
                legacy_count,
                canonical_count;
        END IF;
    END LOOP;

    SELECT count(*)
    INTO legacy_count
    FROM market.l2_snapshot_levels;
    SELECT COALESCE(sum((payload ->> 'entry_count')::bigint), 0)
    INTO canonical_count
    FROM market.fact_versions
    WHERE provenance -> '_qt_migration' ->> 'source_table' =
          'market.l2_snapshot_versions';
    IF canonical_count <> legacy_count THEN
        RAISE EXCEPTION
            'canonical L2 snapshot atomic-entry validation failed: legacy=% canonical=%',
            legacy_count,
            canonical_count;
    END IF;

    SELECT count(*)
    INTO legacy_count
    FROM market.l2_mutations;
    SELECT COALESCE(sum((payload ->> 'entry_count')::bigint), 0)
    INTO canonical_count
    FROM market.fact_versions
    WHERE provenance -> '_qt_migration' ->> 'source_table' =
          'market.l2_mutation_batches';
    IF canonical_count <> legacy_count THEN
        RAISE EXCEPTION
            'canonical L2 mutation atomic-entry validation failed: legacy=% canonical=%',
            legacy_count,
            canonical_count;
    END IF;
END
$$;

DROP TABLE market.l2_snapshot_levels;
DROP TABLE market.l2_mutations;
DROP TABLE market.candle_versions;
DROP TABLE market.open_interest_versions;
DROP TABLE market.funding_rate_versions;
DROP TABLE market.numeric_fact_versions;
DROP TABLE market.market_trade_versions;
DROP TABLE market.trade_flow_aggregate_versions;
DROP TABLE market.l2_snapshot_versions;
DROP TABLE market.l2_mutation_batches;
DROP TABLE market.bbo_feature_versions;
DROP TABLE market.depth_feature_versions;
DROP TABLE market.trade_flow_feature_versions;
DROP TABLE market.futures_spot_relationship_versions;
DROP TABLE market.derivative_state_versions;
DROP TABLE market.market_response_feature_versions;
DROP TABLE market.normalized_feature_versions;

DO $$
DECLARE
    legacy_relation text;
BEGIN
    FOREACH legacy_relation IN ARRAY ARRAY[
        'market.candle_versions',
        'market.open_interest_versions',
        'market.funding_rate_versions',
        'market.numeric_fact_versions',
        'market.market_trade_versions',
        'market.trade_flow_aggregate_versions',
        'market.l2_snapshot_versions',
        'market.l2_snapshot_levels',
        'market.l2_mutation_batches',
        'market.l2_mutations',
        'market.bbo_feature_versions',
        'market.depth_feature_versions',
        'market.trade_flow_feature_versions',
        'market.futures_spot_relationship_versions',
        'market.derivative_state_versions',
        'market.market_response_feature_versions',
        'market.normalized_feature_versions'
    ]
    LOOP
        IF to_regclass(legacy_relation) IS NOT NULL THEN
            RAISE EXCEPTION
                'canonical Fact hard cutover failed to remove %',
                legacy_relation;
        END IF;
    END LOOP;
END
$$;

COMMIT;
