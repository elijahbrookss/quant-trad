-- Runtime ownership/read-model indexes for the definition-only portal_bots shape.
--
-- Safe to apply while the stack is running: this only adds missing indexes on
-- run-owned tables that now carry liveness and lifecycle reads.

BEGIN;

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_leases_bot_status_expires
    ON public.portal_bot_run_leases (bot_id, status, expires_at);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_leases_runner_status
    ON public.portal_bot_run_leases (runner_id, status);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_leases_runner_status_expires
    ON public.portal_bot_run_leases (runner_id, status, expires_at);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_leases_status_expires
    ON public.portal_bot_run_leases (status, expires_at);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_lifecycle_bot_checkpoint_updated
    ON public.portal_bot_run_lifecycle (bot_id, checkpoint_at DESC, updated_at DESC);

ANALYZE public.portal_bot_run_leases;
ANALYZE public.portal_bot_run_lifecycle;

COMMIT;
