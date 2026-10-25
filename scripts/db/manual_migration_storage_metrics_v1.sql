-- Explicit, additive function install. Review exact source and prerequisites first.
-- Existing application startup does NOT run this script. No roles/credentials/grants created.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '250ms';
SET LOCAL statement_timeout = '5s';





CREATE OR REPLACE FUNCTION observability_metrics.storage_metrics_admit_v1()
RETURNS boolean LANGUAGE plpgsql STABLE SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $body$
BEGIN
 IF current_setting('transaction_read_only')<>'on'
    OR current_setting('statement_timeout')::interval <= interval '0 seconds'
    OR current_setting('statement_timeout')::interval > interval '5 seconds'
    OR current_setting('lock_timeout')::interval <= interval '0 seconds'
    OR current_setting('lock_timeout')::interval > interval '250 milliseconds' THEN
   RAISE EXCEPTION 'storage_metrics_session_bounds_required' USING ERRCODE='55000';
 END IF;
 RETURN true;
END
$body$;


CREATE OR REPLACE FUNCTION observability_metrics.storage_series_v1(
    p_metric text, p_from timestamptz, p_to timestamptz, p_bucket_seconds integer
) RETURNS TABLE(time timestamptz,value bigint,state text,reason text,
                complete boolean,truncated boolean,source_count integer,
                observed_at timestamptz,max_gap_seconds double precision)
LANGUAGE plpgsql STABLE SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $body$
BEGIN
    PERFORM observability_metrics.storage_metrics_admit_v1();
    IF p_metric IS NULL OR p_metric NOT IN (
        'database_allocated_bytes','market_allocated_bytes','archive_compressed_bytes',
        'verified_first_compressed_bytes','reclaimed_physical_hot_bytes'
    ) OR p_from IS NULL OR p_to IS NULL OR p_to <= p_from
      OR p_to-p_from > interval '30 days' OR p_bucket_seconds IS NULL
      OR p_bucket_seconds NOT IN (300,1800,7200)
      OR ceil(extract(epoch FROM (p_to-p_from))/p_bucket_seconds) > 720 THEN
        RAISE EXCEPTION 'storage_metrics_range_or_metric_invalid' USING ERRCODE='22023';
    END IF;
    IF p_metric IN ('archive_compressed_bytes','verified_first_compressed_bytes','reclaimed_physical_hot_bytes')
       AND NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
           JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname='market' AND i.indisvalid AND i.indisready
             AND c.relname=CASE p_metric WHEN 'archive_compressed_bytes' THEN 'ix_market_archive_ack_metrics_v1'
                WHEN 'verified_first_compressed_bytes' THEN 'ix_market_archive_verify_time_metrics_v1'
                ELSE 'ix_market_reclaimed_time_metrics_v1' END
             AND i.indrelid=CASE p_metric WHEN 'archive_compressed_bytes' THEN 'market.fact_archive_manifests'::regclass
                WHEN 'verified_first_compressed_bytes' THEN 'market.fact_archive_verifications'::regclass
                ELSE 'market.fact_retention_partitions'::regclass END
             AND pg_catalog.pg_get_indexdef(i.indexrelid,1,true)=CASE p_metric
                WHEN 'archive_compressed_bytes' THEN 'acknowledged_at'
                WHEN 'verified_first_compressed_bytes' THEN 'verified_at' ELSE 'reclaimed_at' END
             AND ((p_metric<>'reclaimed_physical_hot_bytes' AND i.indpred IS NULL)
               OR (p_metric='reclaimed_physical_hot_bytes' AND
                   regexp_replace(pg_catalog.pg_get_expr(i.indpred,i.indrelid),
                      '[[:space:]()]|::text|::character varying','','g')='state=''reclaimed'''))
       ) THEN
       RETURN QUERY SELECT g,NULL::bigint,'unavailable'::text,'metrics_index_unavailable'::text,
          false,false,0,NULL::timestamptz,NULL::double precision FROM generate_series(p_from,p_to-interval '1 microsecond',
          make_interval(secs=>p_bucket_seconds)) g;
       RETURN;
    END IF;
    RETURN QUERY
    WITH relation_page AS MATERIALIZED (
        SELECT sampled_at,schema_name,relation_name,total_bytes
        FROM observability_metrics.database_relation_capacity_samples
        WHERE p_metric='market_allocated_bytes'
          AND sampled_at >= p_from AT TIME ZONE 'UTC' AND sampled_at < p_to AT TIME ZONE 'UTC'
        ORDER BY sampled_at,schema_name,relation_name LIMIT 1000001
    ), verification_page AS MATERIALIZED (
        SELECT manifest_id,manifest_hash,verified_at,verifier_version
        FROM market.fact_archive_verifications
        WHERE p_metric='verified_first_compressed_bytes'
          AND verified_at >= p_from AND verified_at < p_to
        ORDER BY verified_at,manifest_id,verifier_version LIMIT 10001
    ), source AS MATERIALIZED (
        -- Each branch is range/index bounded, then sentinel-capped before aggregation.
        SELECT d.sampled_at AT TIME ZONE 'UTC' AS happened_at,d.database_size_bytes AS bytes
        FROM (SELECT sampled_at,database_size_bytes
              FROM observability_metrics.database_capacity_samples
              WHERE p_metric='database_allocated_bytes'
                AND sampled_at >= p_from AT TIME ZONE 'UTC'
                AND sampled_at < p_to AT TIME ZONE 'UTC'
              ORDER BY sampled_at LIMIT 10001) d
        UNION ALL
        SELECT r.sampled_at AT TIME ZONE 'UTC',
               CASE WHEN count(*)=max(d.relation_count) AND bool_and(d.sampled_at IS NOT NULL)
                    THEN sum(r.total_bytes) FILTER (WHERE r.schema_name='market')::bigint END
        FROM relation_page r LEFT JOIN observability_metrics.database_capacity_samples d ON d.sampled_at=r.sampled_at
        GROUP BY r.sampled_at
        UNION ALL
        SELECT a.acknowledged_at,a.byte_count
        FROM (SELECT acknowledged_at,byte_count FROM market.fact_archive_manifests
              WHERE p_metric='archive_compressed_bytes'
                AND acknowledged_at >= p_from AND acknowledged_at < p_to
              ORDER BY acknowledged_at,id LIMIT 10001) a
        UNION ALL
        SELECT v.verified_at,m.byte_count
        FROM verification_page v
        JOIN market.fact_archive_manifests m ON m.id=v.manifest_id
        WHERE v.manifest_hash=m.manifest_hash AND v.verified_at >= m.acknowledged_at
          AND v.verifier_version = ANY(ARRAY[
            'market.canonical_archive_verification.v1','market.canonical_archive_verification.v2',
            'market.canonical_archive_verification.v3','market.canonical_archive_verification.v4',
            'market.canonical_archive_verification.v5','market.canonical_archive_verification.v6',
            'market.canonical_archive_verification.v7','market.canonical_archive_verification.v8',
            'market.canonical_archive_verification.v9','market.canonical_archive_verification.v10',
            'market.canonical_archive_verification.v11','market.canonical_archive_verification.v12'])
          AND NOT EXISTS (
            SELECT 1 FROM market.fact_archive_verifications older
            WHERE older.manifest_id=m.id AND older.manifest_hash=m.manifest_hash
              AND older.verified_at >= m.acknowledged_at
              AND older.verifier_version = ANY(ARRAY[
                'market.canonical_archive_verification.v1','market.canonical_archive_verification.v2',
                'market.canonical_archive_verification.v3','market.canonical_archive_verification.v4',
                'market.canonical_archive_verification.v5','market.canonical_archive_verification.v6',
                'market.canonical_archive_verification.v7','market.canonical_archive_verification.v8',
                'market.canonical_archive_verification.v9','market.canonical_archive_verification.v10',
                'market.canonical_archive_verification.v11','market.canonical_archive_verification.v12'])
              AND (older.verified_at,older.verifier_version) < (v.verified_at,v.verifier_version))
        UNION ALL
        SELECT r.reclaimed_at,r.reclaimed_bytes
        FROM (SELECT reclaimed_at,reclaimed_bytes FROM market.fact_retention_partitions
              WHERE p_metric='reclaimed_physical_hot_bytes' AND fact_retention_partitions.state='reclaimed'
                AND reclaimed_at >= p_from AND reclaimed_at < p_to
              ORDER BY reclaimed_at,storage_day LIMIT 2049) r
    ), counts AS (
        SELECT CASE WHEN p_metric='market_allocated_bytes' THEN (SELECT count(*)::integer FROM relation_page)
                    WHEN p_metric='verified_first_compressed_bytes'
                    THEN (SELECT count(*)::integer FROM verification_page)
                    ELSE count(*)::integer END AS n FROM source
    ), observations AS (
        SELECT happened_at,CASE WHEN bool_and(bytes IS NOT NULL) THEN sum(bytes)::bigint END AS bytes
        FROM source GROUP BY happened_at
    ), spaced AS (
        SELECT *,extract(epoch FROM (happened_at-coalesce(lag(happened_at) OVER(ORDER BY happened_at),p_from)))::double precision AS gap
        FROM observations
    ), binned AS (
        SELECT date_bin(make_interval(secs=>p_bucket_seconds),happened_at,p_from) AS bucket,
               happened_at,bytes,gap,
               row_number() OVER (PARTITION BY date_bin(make_interval(secs=>p_bucket_seconds),happened_at,p_from)
                                  ORDER BY happened_at DESC) AS newest
        FROM spaced
    ), values_by_bucket AS (
        SELECT bucket,max(happened_at) AS last_observed_at,max(gap) AS largest_gap,bool_and(bytes IS NOT NULL) AS samples_complete,CASE WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes')
                           THEN max(bytes) FILTER (WHERE newest=1) ELSE sum(bytes)::bigint END AS bytes
        FROM binned GROUP BY bucket
    ), grid AS (
        SELECT generate_series(p_from,p_to-interval '1 microsecond',make_interval(secs=>p_bucket_seconds)) AS bucket
    ), bounds AS (
        SELECT n,n > CASE p_metric WHEN 'market_allocated_bytes' THEN 1000000
                       WHEN 'reclaimed_physical_hot_bytes' THEN 2048 ELSE 10000 END AS clipped
        FROM counts
    )
    SELECT g.bucket,CASE WHEN b.clipped THEN NULL ELSE v.bytes END,
           CASE WHEN b.clipped THEN 'partial' WHEN v.bytes IS NULL THEN 'unavailable'
                WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes') AND (v.largest_gap>600 OR NOT v.samples_complete) THEN 'partial'
                WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes')
                  AND p_to >= statement_timestamp()-interval '15 minutes'
                  AND p_to <= statement_timestamp()
                  AND g.bucket+make_interval(secs=>p_bucket_seconds)>=p_to
                  AND p_to-v.last_observed_at>interval '15 minutes' THEN 'stale' ELSE 'ready' END,
           CASE WHEN b.clipped THEN 'source_limit' WHEN v.bytes IS NULL THEN
             CASE WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes')
                  THEN CASE WHEN p_metric='market_allocated_bytes' AND v.last_observed_at IS NOT NULL THEN 'relation_sample_incomplete' ELSE 'no_sample' END ELSE 'no_completion_record' END
                WHEN p_metric='market_allocated_bytes' AND NOT v.samples_complete THEN 'relation_sample_incomplete'
                WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes') AND v.largest_gap>600 THEN 'raw_sample_gap'
                WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes')
                  AND p_to >= statement_timestamp()-interval '15 minutes'
                  AND p_to <= statement_timestamp()
                  AND g.bucket+make_interval(secs=>p_bucket_seconds)>=p_to
                  AND p_to-v.last_observed_at>interval '15 minutes' THEN 'capacity_sample_stale' ELSE NULL END,
           NOT b.clipped AND (p_metric NOT IN ('database_allocated_bytes','market_allocated_bytes')
                  OR (v.bytes IS NOT NULL AND v.samples_complete AND coalesce(v.largest_gap,0)<=600)),b.clipped,b.n,
           v.last_observed_at,CASE WHEN p_metric IN ('database_allocated_bytes','market_allocated_bytes') THEN v.largest_gap END
    FROM grid g CROSS JOIN bounds b LEFT JOIN values_by_bucket v ON v.bucket=g.bucket
    ORDER BY g.bucket;
END
$body$;


CREATE OR REPLACE FUNCTION observability_metrics.storage_metric_status_v1()
RETURNS TABLE(metric_id text,value bigint,unit text,basis text,state text,reason text,
              complete boolean,truncated boolean,oldest_day date)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp
AS $body$
BEGIN
PERFORM observability_metrics.storage_metrics_admit_v1();
IF NOT EXISTS (
 SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='market' AND c.relname='ix_market_live_partition_metrics_v1'
 AND i.indisvalid AND i.indisready AND i.indrelid='market.fact_retention_partitions'::regclass
 AND pg_catalog.pg_get_indexdef(i.indexrelid,1,true)='storage_day'
 AND regexp_replace(pg_catalog.pg_get_expr(i.indpred,i.indrelid),
       '[[:space:]()]|::text|::character varying','','g')='state<>''reclaimed'''
) THEN
 RETURN QUERY SELECT m.id,NULL::bigint,'bytes'::text,'current_eligible_sealed_partition_catalog'::text,
     'unavailable'::text,'metrics_index_unavailable'::text,false,false,NULL::date
 FROM (VALUES ('eligible_backlog_physical_bytes'),('verified_guarded_physical_bytes')) m(id);
 RETURN;
END IF;
RETURN QUERY
WITH policy AS (
    SELECT CASE WHEN jsonb_typeof(policy->'recent_days')='number'
                 AND (policy->>'recent_days')::numeric BETWEEN 1 AND 3650
                THEN (policy->>'recent_days')::integer END AS days
    FROM public.portal_storage_policy WHERE id=1 AND revision>0
), page AS MATERIALIZED (
    SELECT r.storage_day,r.state,r.source_bytes FROM market.fact_retention_partitions r
    WHERE r.state<>'reclaimed' ORDER BY r.storage_day LIMIT 2049
), inventory AS (
    SELECT count(*)>2048 AS clipped FROM page
), eligible AS (
    SELECT p.* FROM page p CROSS JOIN policy
    WHERE storage_day < (statement_timestamp() AT TIME ZONE 'UTC')::date-policy.days
), totals AS (
    SELECT count(*) FILTER (WHERE eligible.source_bytes IS NULL OR eligible.state='open') AS unknown,
           sum(source_bytes)::bigint AS backlog,
           sum(eligible.source_bytes) FILTER (WHERE eligible.state='verified')::bigint AS guarded,
           min(storage_day) AS oldest_day FROM eligible
)
SELECT m.metric_id,
       CASE WHEN i.clipped OR NOT EXISTS(SELECT 1 FROM policy WHERE days IS NOT NULL)
                      OR t.unknown>0 THEN NULL
            WHEN m.metric_id='eligible_backlog_physical_bytes' THEN coalesce(t.backlog,0)
            ELSE coalesce(t.guarded,0) END,
       'bytes','current_eligible_sealed_partition_catalog',
       CASE WHEN i.clipped THEN 'partial'
            WHEN NOT EXISTS(SELECT 1 FROM policy WHERE days IS NOT NULL) OR t.unknown>0
                 THEN 'unavailable' ELSE 'ready' END,
       CASE WHEN i.clipped THEN 'partition_limit'
            WHEN NOT EXISTS(SELECT 1 FROM policy WHERE days IS NOT NULL) THEN 'applied_policy_unavailable'
            WHEN t.unknown>0 THEN 'eligible_unsealed_size_unknown' ELSE NULL END,
       NOT i.clipped AND EXISTS(SELECT 1 FROM policy WHERE days IS NOT NULL) AND t.unknown=0,
       i.clipped,t.oldest_day
FROM totals t CROSS JOIN inventory i CROSS JOIN (VALUES
    ('eligible_backlog_physical_bytes'),('verified_guarded_physical_bytes')) m(metric_id);
END
$body$;


CREATE OR REPLACE FUNCTION observability_metrics.storage_growth_v1(
 p_metric text,p_from timestamptz,p_to timestamptz,p_bucket_seconds integer)
RETURNS TABLE(time timestamptz,value double precision,state text,reason text,
 complete boolean,truncated boolean,source_count integer,
 observed_at timestamptz,max_gap_seconds double precision)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp
AS $body$
BEGIN
 IF p_metric NOT IN ('database_allocated_bytes','market_allocated_bytes') OR p_metric IS NULL THEN
   RAISE EXCEPTION 'storage_growth_metric_invalid' USING ERRCODE='22023';
 END IF;
 RETURN QUERY
 WITH s AS (SELECT * FROM observability_metrics.storage_series_v1(p_metric,p_from,p_to,p_bucket_seconds)),
 previous AS (SELECT s.*,lag(s.value) OVER(ORDER BY s.time) AS prior,
                  lag(s.observed_at) OVER(ORDER BY s.time) AS prior_at,
                  lag(s.state) OVER(ORDER BY s.time) AS prior_state FROM s)
 SELECT p.time,CASE WHEN p.state='ready' AND p.prior IS NOT NULL AND p.prior_state='ready' AND p.observed_at>p.prior_at
                   THEN (p.value-p.prior)::double precision*86400/extract(epoch FROM(p.observed_at-p.prior_at)) END,
 CASE WHEN p.state='ready' AND (p.prior IS NULL OR p.prior_state<>'ready' OR p.observed_at<=p.prior_at) THEN 'unavailable' ELSE p.state END,
 CASE WHEN p.state='ready' AND (p.prior IS NULL OR p.prior_state<>'ready' OR p.observed_at<=p.prior_at) THEN 'previous_bucket_unavailable' ELSE p.reason END,
 p.complete,p.truncated,p.source_count,p.observed_at,p.max_gap_seconds FROM previous p ORDER BY p.time;
END
$body$;

REVOKE ALL ON FUNCTION observability_metrics.storage_metrics_admit_v1() FROM PUBLIC;
REVOKE ALL ON FUNCTION observability_metrics.storage_series_v1(text,timestamptz,timestamptz,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION observability_metrics.storage_growth_v1(text,timestamptz,timestamptz,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION observability_metrics.storage_metric_status_v1() FROM PUBLIC;
COMMIT;
