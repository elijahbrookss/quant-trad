-- Drop legacy raw observability metric samples.
--
-- The active observability contract persists bucketed rollups in
-- observability_metrics.botlens_backend_metric_rollups. Raw samples were
-- an unbounded legacy/debug surface and should not be recreated by the ORM.

DROP TABLE IF EXISTS observability_metrics.botlens_backend_metric_samples_v1 CASCADE;
