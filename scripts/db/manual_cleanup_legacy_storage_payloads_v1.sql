-- Destructive local/dev cleanup for legacy payload-heavy storage.
--
-- This removes pre-retention raw Tier 2/3/4 event rows and stale oversized
-- async result blobs. It does not delete canonical decisions, trades, wallet
-- ledger facts, lifecycle facts, material diagnostics, or market data.
--
-- Apply manually only when no bot run is active.

\echo 'Deleting legacy nonmaterial runtime event rows...'
WITH deleted AS (
    DELETE FROM public.portal_bot_run_events
    WHERE event_name IN (
        'CANDLE_OBSERVED',
        'CANDLE_UPSERTED',
        'HEALTH_STATUS_REPORTED',
        'OVERLAY_STATE_CHANGED',
        'SERIES_STATS_REPORTED'
    )
    RETURNING event_name, pg_column_size(payload) AS payload_bytes
)
SELECT
    COALESCE(event_name, 'unknown') AS event_name,
    count(*) AS deleted_rows,
    pg_size_pretty(COALESCE(sum(payload_bytes), 0)::bigint) AS deleted_payload
FROM deleted
GROUP BY event_name
ORDER BY COALESCE(sum(payload_bytes), 0) DESC;

\echo 'Deleting legacy raw observability event chatter now represented by rollups/live state...'
WITH deleted AS (
    DELETE FROM observability_events.botlens_backend_events_v1
    WHERE event_name IN (
        'db_write_observed',
        'db_write_slow',
        'telemetry_transport_recovered',
        'telemetry_transport_retry_scheduled',
        'telemetry_transport_send_failed',
        'telemetry_transport_connection_lost',
        'telemetry_transport_connection_restored',
        'telemetry_transport_connection_established',
        'telemetry_backpressure_entered',
        'telemetry_enqueue_timeout',
        'viewer_added',
        'viewer_removed',
        'viewer_send_failed',
        'runtime_state_transition',
        'symbol_projector_created',
        'run_projector_created',
        'ledger_tail_start_cursor_resolved',
        'ledger_tail_started',
        'selected_symbol_replay_sent',
        'run_evicted'
    )
    RETURNING event_name, pg_column_size(details) AS details_bytes
)
SELECT
    COALESCE(event_name, 'unknown') AS event_name,
    count(*) AS deleted_rows,
    pg_size_pretty(COALESCE(sum(details_bytes), 0)::bigint) AS deleted_details
FROM deleted
GROUP BY event_name
ORDER BY COALESCE(sum(details_bytes), 0) DESC;

\echo 'Replacing stale oversized async job results with bounded retention markers...'
WITH updated AS (
    UPDATE public.portal_async_jobs
    SET result = jsonb_build_object(
            'retention_status', 'dropped_legacy_payload',
            'reason', 'stale async result exceeded durable storage budget',
            'original_result_bytes', pg_column_size(result),
            'dropped_at', NOW()
        ),
        updated_at = NOW()
    WHERE result IS NOT NULL
      AND pg_column_size(result) > 262144
      AND updated_at < NOW() - INTERVAL '1 hour'
    RETURNING job_type, pg_column_size(result) AS retained_marker_bytes
)
SELECT
    job_type,
    count(*) AS trimmed_rows,
    pg_size_pretty(COALESCE(sum(retained_marker_bytes), 0)::bigint) AS marker_size
FROM updated
GROUP BY job_type
ORDER BY count(*) DESC;

\echo 'Reclaiming storage with VACUUM FULL. This locks rewritten tables.'
VACUUM (FULL, ANALYZE) public.portal_bot_run_events;
VACUUM (FULL, ANALYZE) observability_events.botlens_backend_events_v1;
VACUUM (FULL, ANALYZE) public.portal_async_jobs;
