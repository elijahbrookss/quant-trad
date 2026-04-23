-- Manual migration artifact for BotLens runtime-event storage efficiency.
-- Apply manually; do not execute from application code.
--
-- This pass intentionally does not backfill historical rows in-place.
-- New writes populate the typed hot columns at insert time, and hot reads no
-- longer fall back to payload JSON for those dimensions.

BEGIN;

CREATE SCHEMA IF NOT EXISTS runtime_state;

ALTER TABLE public.portal_bot_run_events
    ADD COLUMN IF NOT EXISTS event_name VARCHAR(128),
    ADD COLUMN IF NOT EXISTS series_key VARCHAR(255),
    ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS root_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS bar_time TIMESTAMP,
    ADD COLUMN IF NOT EXISTS instrument_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS symbol VARCHAR(64),
    ADD COLUMN IF NOT EXISTS timeframe VARCHAR(32),
    ADD COLUMN IF NOT EXISTS signal_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS decision_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS trade_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS reason_code VARCHAR(128);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, seq, id);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_series_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, series_key, seq, id);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_event_name_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, event_name, seq, id);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_candle_series_bar_time_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, series_key, bar_time, seq, id)
    WHERE event_name = 'CANDLE_OBSERVED'
      AND series_key IS NOT NULL
      AND bar_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_correlation_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, correlation_id, seq, id)
    WHERE correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_root_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, root_id, seq, id)
    WHERE root_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_bar_time_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, bar_time, seq, id)
    WHERE bar_time IS NOT NULL;

DROP INDEX IF EXISTS public.ix_portal_bot_run_events_payload_series_key;

CREATE OR REPLACE VIEW runtime_state.bot_runtime_events_v1 AS
SELECT
    e.id,
    e.event_id,
    e.bot_id,
    e.run_id,
    e.seq,
    e.event_type,
    e.critical,
    e.schema_version,
    e.event_time,
    e.known_at,
    e.created_at,
    e.payload,
    e.series_key,
    NULLIF(e.payload #>> '{context,bridge_session_id}', '') AS bridge_session_id,
    CASE
        WHEN NULLIF(e.payload #>> '{context,bridge_seq}', '') ~ '^-?[0-9]+$'
            THEN (e.payload #>> '{context,bridge_seq}')::INTEGER
        ELSE NULL
    END AS bridge_seq,
    CASE
        WHEN NULLIF(e.payload #>> '{context,run_seq}', '') ~ '^-?[0-9]+$'
            THEN (e.payload #>> '{context,run_seq}')::INTEGER
        ELSE NULL
    END AS run_seq,
    e.instrument_id,
    e.symbol,
    e.timeframe,
    e.event_name AS runtime_event_name,
    NULLIF(e.payload #>> '{context,category}', '') AS runtime_event_category,
    e.root_id,
    e.correlation_id,
    e.bar_time,
    e.signal_id,
    e.decision_id,
    e.trade_id,
    e.reason_code
FROM public.portal_bot_run_events e;

COMMIT;
