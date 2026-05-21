-- Runtime event per-run replay ordering.
--
-- Runtime writes allocate run_seq from portal_bot_run_event_seq_allocators.
-- The typed run_seq columns are the hot-path/order source for new rows. The
-- payload context read below is a one-time historical backfill only.

ALTER TABLE public.portal_bot_run_events
    ADD COLUMN IF NOT EXISTS run_seq INTEGER,
    ADD COLUMN IF NOT EXISTS run_seq_status TEXT;

UPDATE public.portal_bot_run_events
SET
    run_seq = (payload #>> '{context,run_seq}')::INTEGER,
    run_seq_status = NULLIF(payload #>> '{context,run_seq_status}', '')
WHERE run_seq IS NULL
  AND NULLIF(payload #>> '{context,run_seq}', '') ~ '^[0-9]+$';

CREATE TABLE IF NOT EXISTS public.portal_bot_run_event_seq_allocators (
    run_id TEXT PRIMARY KEY,
    next_run_seq INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.portal_bot_run_event_seq_allocators (run_id, next_run_seq, updated_at)
SELECT
    run_id,
    COALESCE(MAX(run_seq), 0) + 1 AS next_run_seq,
    NOW() AS updated_at
FROM public.portal_bot_run_events
WHERE run_seq IS NOT NULL
GROUP BY run_id
ON CONFLICT (run_id) DO UPDATE
SET
    next_run_seq = GREATEST(
        public.portal_bot_run_event_seq_allocators.next_run_seq,
        EXCLUDED.next_run_seq
    ),
    updated_at = NOW();

CREATE UNIQUE INDEX IF NOT EXISTS uq_portal_bot_run_events_run_seq
    ON public.portal_bot_run_events (run_id, run_seq)
    WHERE run_seq IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_events_bot_run_run_seq_id
    ON public.portal_bot_run_events (bot_id, run_id, run_seq, id)
    WHERE run_seq IS NOT NULL;

DROP INDEX IF EXISTS public.uq_portal_bot_run_events_run_seq_payload;
DROP INDEX IF EXISTS public.ix_portal_bot_run_events_bot_run_payload_run_seq_id;
