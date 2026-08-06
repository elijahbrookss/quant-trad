\set ON_ERROR_STOP on

-- Market storage lifecycle v1 activation.
--
-- Run only after stopping all collectors. This script configures Timescale
-- compression and converts the two L2 child tables needed for atomic parent /
-- child chunk lifecycle. It intentionally does not install add_retention_policy:
-- frozen datasets and explicit archive pins are enforced by the application
-- lifecycle worker before any chunk or object is removed.

BEGIN;

SELECT pg_advisory_xact_lock(
    hashtextextended('quant-trad:market-storage-lifecycle:v1', 0)
);

DO $guard$
DECLARE
    active_lease_count bigint;
BEGIN
    SELECT count(*)
      INTO active_lease_count
      FROM market.stream_lease_state
     WHERE lease_expires_at > now();

    IF active_lease_count > 0 THEN
        RAISE EXCEPTION
            'market_storage_lifecycle_activation_blocked: % active stream leases; stop collectors first',
            active_lease_count;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) THEN
        RAISE EXCEPTION
            'market_storage_lifecycle_activation_blocked: TimescaleDB is required';
    END IF;
END
$guard$;

SELECT create_hypertable(
    'market.l2_snapshot_levels',
    by_range('snapshot_effective_at'),
    if_not_exists => TRUE,
    migrate_data => TRUE
);

SELECT create_hypertable(
    'market.l2_mutations',
    by_range('batch_effective_at'),
    if_not_exists => TRUE,
    migrate_data => TRUE
);

ALTER TABLE market.candle_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'candle_open_time DESC, market_commit_seq DESC'
);
ALTER TABLE market.open_interest_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'sample_time DESC, market_commit_seq DESC'
);
ALTER TABLE market.funding_rate_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'sample_time DESC, market_commit_seq DESC'
);
ALTER TABLE market.market_trade_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'provider_event_time DESC, market_commit_seq DESC'
);
ALTER TABLE market.trade_flow_aggregate_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'bucket_start DESC, market_commit_seq DESC'
);
ALTER TABLE market.l2_snapshot_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'effective_at DESC, market_commit_seq DESC'
);
ALTER TABLE market.l2_snapshot_levels SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'snapshot_version_id',
    timescaledb.compress_orderby = 'snapshot_effective_at DESC, side, price'
);
ALTER TABLE market.l2_mutation_batches SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'effective_at DESC, market_commit_seq DESC'
);
ALTER TABLE market.l2_mutations SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'batch_id',
    timescaledb.compress_orderby = 'batch_effective_at DESC, mutation_ordinal'
);
ALTER TABLE market.bbo_feature_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'bucket_start DESC, market_commit_seq DESC'
);
ALTER TABLE market.depth_feature_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'bucket_start DESC, market_commit_seq DESC'
);
ALTER TABLE market.trade_flow_feature_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'bucket_start DESC, market_commit_seq DESC'
);
ALTER TABLE market.futures_spot_relationship_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'effective_at DESC, market_commit_seq DESC'
);
ALTER TABLE market.derivative_state_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'effective_at DESC, market_commit_seq DESC'
);
ALTER TABLE market.normalized_feature_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'effective_at DESC, market_commit_seq DESC'
);
ALTER TABLE market.market_response_feature_versions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'bucket_start DESC, market_commit_seq DESC'
);

DO $verify$
DECLARE
    missing text;
BEGIN
    SELECT string_agg(format('%I.%I', expected.schema_name, expected.table_name), ', ')
      INTO missing
      FROM (
          VALUES
              ('market', 'candle_versions'),
              ('market', 'open_interest_versions'),
              ('market', 'funding_rate_versions'),
              ('market', 'market_trade_versions'),
              ('market', 'trade_flow_aggregate_versions'),
              ('market', 'l2_snapshot_versions'),
              ('market', 'l2_snapshot_levels'),
              ('market', 'l2_mutation_batches'),
              ('market', 'l2_mutations'),
              ('market', 'bbo_feature_versions'),
              ('market', 'depth_feature_versions'),
              ('market', 'trade_flow_feature_versions'),
              ('market', 'futures_spot_relationship_versions'),
              ('market', 'derivative_state_versions'),
              ('market', 'normalized_feature_versions'),
              ('market', 'market_response_feature_versions')
      ) AS expected(schema_name, table_name)
      LEFT JOIN timescaledb_information.hypertables AS actual
        ON actual.hypertable_schema = expected.schema_name
       AND actual.hypertable_name = expected.table_name
       AND actual.compression_enabled
     WHERE actual.hypertable_name IS NULL;

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION
            'market_storage_lifecycle_activation_failed: compression missing for %',
            missing;
    END IF;
END
$verify$;

COMMIT;
