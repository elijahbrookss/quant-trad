DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EXCEPTION
  WHEN object_not_in_prerequisite_state THEN
    RAISE WARNING 'pg_stat_statements requires shared_preload_libraries; enable it and restart the server.';
END;
$$;

CREATE EXTENSION IF NOT EXISTS pg_buffercache;

DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS system_stats;
EXCEPTION
  WHEN undefined_file THEN
    RAISE WARNING 'system_stats extension not available in this Postgres image.';
END;
$$;
