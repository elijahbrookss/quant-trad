CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_portal_bot_trades_run_updated_id
    ON public.portal_bot_trades (run_id, updated_at, id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_portal_bot_trades_bot_run_status
    ON public.portal_bot_trades (bot_id, run_id, status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_portal_bot_run_events_bot_run_series_event_run_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, series_key, event_name, run_seq, id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_portal_bot_run_events_bot_run_series_trade_run_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, series_key, trade_id, run_seq, id);
