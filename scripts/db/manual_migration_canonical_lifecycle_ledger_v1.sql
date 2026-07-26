-- Hard cutover to portal_bot_run_events as the sole lifecycle event ledger.
--
-- This migration refuses to drop the retired lifecycle tables when they
-- contain event/current-state rows that are not represented field-for-field in
-- the canonical runtime-event ledger. Resolve or intentionally discard those
-- rows before rerunning the migration.

BEGIN;

DROP VIEW IF EXISTS runtime_state.bot_run_lifecycle;
DROP VIEW IF EXISTS runtime_state.bot_run_lifecycle_events;
DROP VIEW IF EXISTS runtime_state.bot_run_lifecycle_v1;
DROP VIEW IF EXISTS runtime_state.bot_run_lifecycle_events_v1;

DO $$
DECLARE
    missing_event_rows bigint := 0;
    missing_current_rows bigint := 0;
BEGIN
    IF to_regclass('public.portal_bot_run_lifecycle_events') IS NOT NULL THEN
        SELECT count(*)
        INTO missing_event_rows
        FROM public.portal_bot_run_lifecycle_events legacy
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.portal_bot_run_events canonical
            WHERE canonical.event_id = legacy.event_id
              AND canonical.run_id = legacy.run_id
              AND canonical.bot_id = legacy.bot_id
              AND canonical.event_time = legacy.checkpoint_at
              AND canonical.event_name IN (
                  'RUN_PHASE_REPORTED',
                  'RUN_STARTED',
                  'RUN_READY',
                  'RUN_DEGRADED',
                  'RUN_COMPLETED',
                  'RUN_FAILED',
                  'RUN_STOPPED',
                  'RUN_CANCELLED'
              )
              AND canonical.payload #>> '{context,phase}' = legacy.phase
              AND canonical.payload #>> '{context,status}' = legacy.status
              AND canonical.payload #>> '{context,component}' IS NOT DISTINCT FROM legacy.owner
              AND canonical.payload #>> '{context,message}' IS NOT DISTINCT FROM legacy.message
              AND COALESCE(canonical.payload #> '{context,metadata}', '{}'::jsonb)
                  = COALESCE(legacy.metadata, '{}'::jsonb)
              AND COALESCE(canonical.payload #> '{context,failure}', '{}'::jsonb)
                  = COALESCE(legacy.failure, '{}'::jsonb)
        );
    END IF;

    IF missing_event_rows > 0 THEN
        RAISE EXCEPTION
            'refusing lifecycle hard cutover: % legacy lifecycle event rows lack field-equivalent canonical events',
            missing_event_rows;
    END IF;

    IF to_regclass('public.portal_bot_run_lifecycle') IS NOT NULL THEN
        SELECT count(*)
        INTO missing_current_rows
        FROM public.portal_bot_run_lifecycle legacy
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.portal_bot_run_events canonical
            WHERE canonical.run_id = legacy.run_id
              AND canonical.bot_id = legacy.bot_id
              AND canonical.event_time = legacy.checkpoint_at
              AND canonical.event_name IN (
                  'RUN_PHASE_REPORTED',
                  'RUN_STARTED',
                  'RUN_READY',
                  'RUN_DEGRADED',
                  'RUN_COMPLETED',
                  'RUN_FAILED',
                  'RUN_STOPPED',
                  'RUN_CANCELLED'
              )
              AND canonical.payload #>> '{context,phase}' = legacy.phase
              AND canonical.payload #>> '{context,status}' = legacy.status
              AND canonical.payload #>> '{context,component}' IS NOT DISTINCT FROM legacy.owner
              AND canonical.payload #>> '{context,message}' IS NOT DISTINCT FROM legacy.message
              AND COALESCE(canonical.payload #> '{context,metadata}', '{}'::jsonb)
                  = COALESCE(legacy.metadata, '{}'::jsonb)
              AND COALESCE(canonical.payload #> '{context,failure}', '{}'::jsonb)
                  = COALESCE(legacy.failure, '{}'::jsonb)
        );
    END IF;

    IF missing_current_rows > 0 THEN
        RAISE EXCEPTION
            'refusing lifecycle hard cutover: % legacy current lifecycle rows lack field-equivalent canonical events',
            missing_current_rows;
    END IF;
END $$;

DROP TABLE IF EXISTS public.portal_bot_run_lifecycle_events;
DROP TABLE IF EXISTS public.portal_bot_run_lifecycle;

COMMIT;
