-- Add mergeable histograms to runtime step profiler rollups.
-- Apply manually; do not execute from application code.

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

ALTER TABLE public.portal_bot_run_step_rollups_v1
    ADD COLUMN IF NOT EXISTS histogram_bounds JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS histogram_counts JSONB NOT NULL DEFAULT '[]'::jsonb;
