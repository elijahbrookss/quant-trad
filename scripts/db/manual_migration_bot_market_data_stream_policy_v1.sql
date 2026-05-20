-- Add first-class bot-owned market data stream reconnect policy storage.
--
-- This is an operator-run migration. Runtime startup still fails loud if the
-- schema does not match the SQLAlchemy model.

ALTER TABLE public.portal_bots
    ADD COLUMN IF NOT EXISTS market_data_stream_policy JSON NOT NULL DEFAULT '{}'::json;
