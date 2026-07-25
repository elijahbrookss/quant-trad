-- Cut portal_bots to bot definitions only.
--
-- Run this only with backend/frontend code that reads runtime state from
-- portal_bot_runs, portal_bot_run_lifecycle, portal_bot_run_leases, and report
-- materialization tables. Existing live processes that still read/write these
-- columns must be stopped before this migration is applied.

BEGIN;

ALTER TABLE public.portal_bots
    DROP COLUMN status,
    DROP COLUMN last_run_at,
    DROP COLUMN last_stats,
    DROP COLUMN last_run_artifact,
    DROP COLUMN runner_id,
    DROP COLUMN heartbeat_at;

COMMIT;

VACUUM (ANALYZE) public.portal_bots;
