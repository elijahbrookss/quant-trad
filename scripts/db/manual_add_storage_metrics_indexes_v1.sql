-- Approved operator action only. Run OUTSIDE a transaction; retains writers.
-- Check disk/WAL headroom and active maintenance first. Investigate invalid indexes;
-- IF NOT EXISTS does not prove a same-name index has the required definition.
\set ON_ERROR_STOP on
SET lock_timeout = '250ms';
SET statement_timeout = '5min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_live_partition_metrics_v1 ON market.fact_retention_partitions (storage_day) WHERE state<>'reclaimed';
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_archive_ack_metrics_v1 ON market.fact_archive_manifests (acknowledged_at,id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_archive_verify_time_metrics_v1 ON market.fact_archive_verifications (verified_at,manifest_id,verifier_version);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_market_reclaimed_time_metrics_v1 ON market.fact_retention_partitions (reclaimed_at,storage_day) WHERE state='reclaimed';
SELECT c.relname,i.indisvalid,i.indisready,pg_catalog.pg_get_indexdef(i.indexrelid)
FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='market' AND c.relname IN (
'ix_market_live_partition_metrics_v1','ix_market_archive_ack_metrics_v1',
'ix_market_archive_verify_time_metrics_v1','ix_market_reclaimed_time_metrics_v1');
