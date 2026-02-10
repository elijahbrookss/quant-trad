DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS timescaledb;
EXCEPTION
  WHEN object_not_in_prerequisite_state THEN
    RAISE WARNING 'timescaledb requires shared_preload_libraries=timescaledb; enable it and restart the server.';
END;
$$;

DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EXCEPTION
  WHEN object_not_in_prerequisite_state THEN
    RAISE WARNING 'pg_stat_statements requires shared_preload_libraries; enable it and restart the server.';
END;
$$;

CREATE EXTENSION IF NOT EXISTS pg_buffercache;
