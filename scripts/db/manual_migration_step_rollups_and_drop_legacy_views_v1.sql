-- Replace raw runtime step rows and legacy BotLens snapshot payload stores with
-- typed profiler rollups.

CREATE OR REPLACE FUNCTION public.quanttrad_jsonb_histogram_counts_add(left_counts JSONB, right_counts JSONB)
RETURNS JSONB
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(
        jsonb_agg(
            COALESCE((left_counts ->> (idx - 1))::INTEGER, 0)
            + COALESCE((right_counts ->> (idx - 1))::INTEGER, 0)
            ORDER BY idx
        ),
        '[]'::jsonb
    )
    FROM generate_series(
        1,
        GREATEST(
            COALESCE(jsonb_array_length(left_counts), 0),
            COALESCE(jsonb_array_length(right_counts), 0)
        )
    ) AS idx;
$$;

CREATE OR REPLACE FUNCTION public.quanttrad_jsonb_histogram_quantile(
    bounds JSONB,
    counts JSONB,
    quantile DOUBLE PRECISION,
    fallback DOUBLE PRECISION DEFAULT 0.0
)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    total_count BIGINT;
    threshold_count BIGINT;
    cumulative_count BIGINT := 0;
    idx INTEGER;
    count_value BIGINT;
    bound_value DOUBLE PRECISION;
BEGIN
    IF bounds IS NULL OR counts IS NULL OR jsonb_array_length(bounds) = 0 THEN
        RETURN COALESCE(fallback, 0.0);
    END IF;

    SELECT COALESCE(SUM(value::BIGINT), 0)
    INTO total_count
    FROM jsonb_array_elements_text(counts) AS value;

    IF total_count <= 0 THEN
        RETURN COALESCE(fallback, 0.0);
    END IF;

    threshold_count := GREATEST(
        CEIL(total_count * LEAST(GREATEST(COALESCE(quantile, 0.0), 0.0), 1.0))::BIGINT,
        1
    );

    FOR idx IN 0..(jsonb_array_length(bounds) - 1) LOOP
        count_value := COALESCE((counts ->> idx)::BIGINT, 0);
        cumulative_count := cumulative_count + count_value;
        IF cumulative_count >= threshold_count THEN
            bound_value := COALESCE((bounds ->> idx)::DOUBLE PRECISION, COALESCE(fallback, 0.0));
            IF fallback IS NULL THEN
                RETURN bound_value;
            END IF;
            RETURN LEAST(bound_value, fallback);
        END IF;
    END LOOP;

    RETURN COALESCE(fallback, 0.0);
END;
$$;

CREATE TABLE IF NOT EXISTS public.portal_bot_run_step_rollups_v1 (
    id SERIAL PRIMARY KEY,
    bucket_start TIMESTAMP NOT NULL,
    bucket_seconds INTEGER NOT NULL DEFAULT 10,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    bot_id VARCHAR(64) NOT NULL DEFAULT '',
    step_name VARCHAR(64) NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    strategy_id VARCHAR(64) NOT NULL DEFAULT '',
    symbol VARCHAR(64) NOT NULL DEFAULT '',
    timeframe VARCHAR(32) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'ok',
    sample_count INTEGER NOT NULL DEFAULT 0,
    value_sum DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    value_min DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    value_max DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    latest_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p95_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    p99_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    histogram_bounds JSONB NOT NULL DEFAULT '[]'::jsonb,
    histogram_counts JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_sample_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_portal_bot_run_step_rollups_v1_bucket_identity UNIQUE (
        bucket_start,
        bucket_seconds,
        run_id,
        bot_id,
        step_name,
        metric_name,
        strategy_id,
        symbol,
        timeframe,
        status
    )
);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_step_rollups_v1_run_bucket
    ON public.portal_bot_run_step_rollups_v1 (run_id, bucket_start);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_step_rollups_v1_run_step_metric_bucket
    ON public.portal_bot_run_step_rollups_v1 (run_id, step_name, metric_name, bucket_start);

CREATE INDEX IF NOT EXISTS ix_portal_bot_run_step_rollups_v1_bot_bucket
    ON public.portal_bot_run_step_rollups_v1 (bot_id, bucket_start);

DROP TABLE IF EXISTS public.portal_bot_run_steps CASCADE;
DROP TABLE IF EXISTS public.portal_bot_run_snapshots CASCADE;
DROP TABLE IF EXISTS public.portal_bot_run_view_state CASCADE;
