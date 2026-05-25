-- Runtime pressure cleanup for bot fleet projections and step trace rollups.
-- Safe to run while bots are live; avoids table rewrites and only prunes diagnostic rows.

ALTER TABLE IF EXISTS public.portal_bot_run_step_rollups_v1
    ALTER COLUMN bucket_seconds SET DEFAULT 60;

DELETE FROM public.portal_bot_run_step_rollups_v1
WHERE metric_name <> 'duration_ms'
  AND metric_name NOT IN (
    'build_state_ms',
    'canonical_append_ms',
    'canonical_fact_overflow_count',
    'canonical_fact_persist_batch_ms',
    'canonical_fact_persist_error_count',
    'canonical_fact_persist_lag_ms',
    'canonical_fact_queue_depth',
    'canonical_fact_queued_count',
    'candle_update_ms',
    'db_commit_ms',
    'delta_build_ms',
    'delta_serialize_ms',
    'dispatch_ms',
    'enqueue_ms',
    'execution_decision_flow_ms',
    'execution_ms',
    'execution_prime_ms',
    'execution_settlement_ms',
    'execution_trade_event_processing_ms',
    'finalize_residual_ms',
    'indicator_eval_ms',
    'indicator_state_update_ms',
    'max_overlay_payload_bytes',
    'overlay_payload_bytes',
    'overlay_projection_delta_ms',
    'overlay_projection_entries_total',
    'overlay_projection_ms',
    'overlay_projection_ops_count',
    'overlays_update_ms',
    'payload_bytes',
    'pending_signals_ops_ms',
    'persist_ms',
    'persistence_ms',
    'rule_eval_ms',
    'serialize_ms',
    'series_overlay_entries_ms',
    'signal_eval_ms',
    'stats_update_ms',
    'step_trace_dropped_count',
    'step_trace_persist_batch_ms',
    'step_trace_persist_error_count',
    'step_trace_persist_lag_ms',
    'step_trace_queue_depth',
    'strategy_eval_ms',
    'stream_emit_ms',
    'trace_persist_ms',
    'trade_lock_hold_ms',
    'trade_lock_wait_ms',
    'worker_count'
  );

DELETE FROM public.portal_bot_run_step_rollups_v1
WHERE bucket_start < now() - interval '24 hours';

UPDATE public.portal_bots
SET last_run_artifact = jsonb_strip_nulls(
        jsonb_build_object(
            'compacted', true,
            'compacted_at', now(),
            'previous_payload_bytes', pg_column_size(last_run_artifact),
            'run_id', COALESCE(
                last_run_artifact::jsonb ->> 'run_id',
                last_run_artifact::jsonb #>> '{startup,run_id}'
            ),
            'startup', last_run_artifact::jsonb -> 'startup',
            'error', last_run_artifact::jsonb -> 'error',
            'execution_mode', last_run_artifact::jsonb -> 'execution_mode'
        )
    )::json,
    updated_at = now()
WHERE last_run_artifact IS NOT NULL
  AND pg_column_size(last_run_artifact) > 65536
  AND COALESCE(status, '') NOT IN (
    'starting',
    'running',
    'paused',
    'degraded',
    'telemetry_degraded'
  );

SET maintenance_work_mem = '64MB';

VACUUM (ANALYZE, PARALLEL 0) public.portal_bot_run_step_rollups_v1;
VACUUM (ANALYZE, PARALLEL 0) public.portal_bots;
