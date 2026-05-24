#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text

os.environ.setdefault("QT_LOGGING_LOKI_URL", "")
os.environ.setdefault("QT_LOGGING_DEBUG", "false")
os.environ.setdefault("QT_LOGGING_LEVEL", "WARNING")

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portal.backend.db import db  # noqa: E402
from portal.backend.service.bots.botlens_event_retention import retention_policy_for_event_name, tier_map  # noqa: E402


RUN_SEQ_EXPR = "run_seq"


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _query(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    with db.session() as session:
        return [dict(row) for row in session.execute(text(sql), params).mappings().all()]


def _query_one(sql: str, params: dict[str, Any]) -> dict[str, Any]:
    rows = _query(sql, params)
    return rows[0] if rows else {}


def _cmd_throughput(args: argparse.Namespace) -> int:
    run_id = str(args.run_id)
    summary = _query_one(
        f"""
        SELECT
            COUNT(*) AS event_count,
            MIN(created_at) AS first_event_at,
            MAX(created_at) AS latest_event_at,
            MIN({RUN_SEQ_EXPR}) AS min_run_seq,
            MAX({RUN_SEQ_EXPR}) AS max_run_seq
        FROM public.portal_bot_run_events
        WHERE run_id = :run_id
        """,
        {"run_id": run_id},
    )
    per_minute = _query(
        f"""
        SELECT
            date_trunc('minute', created_at) AS bucket,
            COUNT(*) AS event_count,
            MIN({RUN_SEQ_EXPR}) AS min_run_seq,
            MAX({RUN_SEQ_EXPR}) AS max_run_seq
        FROM public.portal_bot_run_events
        WHERE run_id = :run_id
        GROUP BY bucket
        ORDER BY bucket
        """,
        {"run_id": run_id},
    )
    _print_json({"run_id": run_id, "summary": summary, "per_minute": per_minute})
    return 0


def _cmd_event_summary(args: argparse.Namespace) -> int:
    run_id = str(args.run_id)
    rows = _query(
        f"""
        SELECT
            COALESCE(NULLIF(event_name, ''), payload ->> 'event_name', '<unknown>') AS event_name,
            event_type,
            COUNT(*) AS event_count,
            COUNT(DISTINCT event_id) AS distinct_event_ids,
            MIN({RUN_SEQ_EXPR}) AS min_run_seq,
            MAX({RUN_SEQ_EXPR}) AS max_run_seq
        FROM public.portal_bot_run_events
        WHERE run_id = :run_id
        GROUP BY 1, 2
        ORDER BY event_count DESC, event_name ASC, event_type ASC
        """,
        {"run_id": run_id},
    )
    _print_json({"run_id": run_id, "events": rows})
    return 0


def _storage_budget_scope(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    run_id = str(getattr(args, "run_id", "") or "").strip()
    if run_id:
        return "WHERE run_id = :run_id", {"run_id": run_id}
    return "", {}


def _cmd_storage_budget(args: argparse.Namespace) -> int:
    scope_sql, params = _storage_budget_scope(args)
    rows = _query(
        f"""
        SELECT
            COALESCE(NULLIF(event_name, ''), payload ->> 'event_name', '<unknown>') AS event_name,
            event_type,
            payload #>> '{{context,level}}' AS context_level,
            payload #>> '{{context,status}}' AS context_status,
            payload #>> '{{context,failure_mode}}' AS context_failure_mode,
            payload #>> '{{context,diagnostic_code}}' AS context_diagnostic_code,
            payload #>> '{{context,diagnostic_event}}' AS context_diagnostic_event,
            payload #>> '{{context,reason_code}}' AS context_reason_code,
            COUNT(*) AS row_count,
            COUNT(DISTINCT event_id) AS distinct_event_ids,
            COALESCE(SUM(pg_column_size(payload)), 0) AS payload_bytes,
            COALESCE(ROUND(AVG(pg_column_size(payload))::numeric, 1), 0) AS avg_payload_bytes
        FROM public.portal_bot_run_events
        {scope_sql}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ORDER BY row_count DESC, event_name ASC, event_type ASC
        """,
        params,
    )
    details: list[dict[str, Any]] = []
    totals = {
        "current_rows": 0,
        "current_payload_bytes": 0,
        "permanent_rows_after_policy_estimate": 0,
        "permanent_payload_bytes_after_policy_estimate": 0,
        "raw_rows_removed_or_summarized_estimate": 0,
        "raw_payload_bytes_removed_or_summarized_estimate": 0,
    }
    by_tier: dict[str, dict[str, int]] = {}
    by_action: dict[str, dict[str, int]] = {}
    for row in rows:
        event_name = str(row.get("event_name") or "").strip().upper()
        context = {
            key.removeprefix("context_"): row.get(key)
            for key in (
                "context_level",
                "context_status",
                "context_failure_mode",
                "context_diagnostic_code",
                "context_diagnostic_event",
                "context_reason_code",
            )
            if row.get(key) not in (None, "")
        }
        policy = retention_policy_for_event_name(event_name, context=context)
        row_count = int(row.get("row_count") or 0)
        payload_bytes = int(row.get("payload_bytes") or 0)
        retained = row_count if policy.persist_raw else 0
        retained_bytes = payload_bytes if policy.persist_raw else 0
        removed = row_count - retained
        removed_bytes = payload_bytes - retained_bytes
        totals["current_rows"] += row_count
        totals["current_payload_bytes"] += payload_bytes
        totals["permanent_rows_after_policy_estimate"] += retained
        totals["permanent_payload_bytes_after_policy_estimate"] += retained_bytes
        totals["raw_rows_removed_or_summarized_estimate"] += removed
        totals["raw_payload_bytes_removed_or_summarized_estimate"] += removed_bytes
        tier_bucket = by_tier.setdefault(policy.tier.value, {"rows": 0, "payload_bytes": 0})
        tier_bucket["rows"] += row_count
        tier_bucket["payload_bytes"] += payload_bytes
        action_bucket = by_action.setdefault(policy.action.value, {"rows": 0, "payload_bytes": 0})
        action_bucket["rows"] += row_count
        action_bucket["payload_bytes"] += payload_bytes
        details.append(
            {
                **dict(row),
                "policy_context": context,
                "tier": policy.tier.value,
                "action": policy.action.value,
                "reason": policy.reason,
                "estimated_permanent_rows": retained,
                "estimated_removed_or_summarized_rows": removed,
                "estimated_permanent_payload_bytes": retained_bytes,
                "estimated_removed_or_summarized_payload_bytes": removed_bytes,
            }
        )
    current_rows = max(int(totals["current_rows"]), 1)
    permanent_rows = int(totals["permanent_rows_after_policy_estimate"])
    reduction_pct = 1.0 - (float(permanent_rows) / float(current_rows))
    _print_json(
        {
            "run_id": str(getattr(args, "run_id", "") or "").strip() or None,
            "summary": {
                **totals,
                "estimated_row_reduction_pct": round(reduction_pct, 6),
            },
            "by_tier": by_tier,
            "by_action": by_action,
            "events": details,
            "tier_map": list(tier_map()),
        }
    )
    return 0


def _cmd_seq_gaps(args: argparse.Namespace) -> int:
    run_id = str(args.run_id)
    summary = _query_one(
        f"""
        WITH present AS (
            SELECT DISTINCT {RUN_SEQ_EXPR} AS run_seq
            FROM public.portal_bot_run_events
            WHERE run_id = :run_id
              AND {RUN_SEQ_EXPR} IS NOT NULL
        )
        SELECT
            COUNT(*) AS distinct_run_seq,
            MIN(run_seq) AS min_run_seq,
            MAX(run_seq) AS max_run_seq,
            CASE
                WHEN COUNT(*) = 0 THEN 0
                ELSE MAX(run_seq) - MIN(run_seq) + 1 - COUNT(*)
            END AS gap_count
        FROM present
        """,
        {"run_id": run_id},
    )
    first_gaps = _query(
        f"""
        WITH present AS (
            SELECT DISTINCT {RUN_SEQ_EXPR} AS run_seq
            FROM public.portal_bot_run_events
            WHERE run_id = :run_id
              AND {RUN_SEQ_EXPR} IS NOT NULL
        ),
        bounds AS (
            SELECT MIN(run_seq) AS min_run_seq, MAX(run_seq) AS max_run_seq
            FROM present
        )
        SELECT expected.run_seq AS missing_run_seq
        FROM bounds
        CROSS JOIN LATERAL generate_series(bounds.min_run_seq, bounds.max_run_seq) AS expected(run_seq)
        LEFT JOIN present ON present.run_seq = expected.run_seq
        WHERE present.run_seq IS NULL
        ORDER BY expected.run_seq
        LIMIT :limit
        """,
        {"run_id": run_id, "limit": max(int(args.limit or 20), 1)},
    )
    duplicates = _query(
        f"""
        SELECT {RUN_SEQ_EXPR} AS run_seq, COUNT(*) AS row_count
        FROM public.portal_bot_run_events
        WHERE run_id = :run_id
          AND {RUN_SEQ_EXPR} IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) > 1
        ORDER BY run_seq
        LIMIT :limit
        """,
        {"run_id": run_id, "limit": max(int(args.limit or 20), 1)},
    )
    _print_json(
        {
            "run_id": run_id,
            **summary,
            "first_gaps": first_gaps,
            "duplicates": duplicates,
            "status": "ready" if int(summary.get("gap_count") or 0) == 0 and not duplicates else "blocked",
        }
    )
    return 0 if int(summary.get("gap_count") or 0) == 0 and not duplicates else 1


def _cmd_write_latency(args: argparse.Namespace) -> int:
    run_id = str(args.run_id)
    rows = _query(
        """
        SELECT
            metric_name,
            pipeline_stage,
            message_kind,
            SUM(sample_count) AS sample_count,
            ROUND((SUM(value_sum) / NULLIF(SUM(sample_count), 0))::numeric, 3) AS avg_value,
            ROUND(MAX(value_max)::numeric, 3) AS max_value,
            ROUND(MAX(p95_value)::numeric, 3) AS p95_value
        FROM observability_metrics.botlens_backend_metric_rollups_v1
        WHERE run_id = :run_id
          AND storage_target IN ('bot_runtime_events', 'observability_metric_rollups', 'observability_events')
          AND metric_name IN (
              'db_write_ms',
              'db_write_round_trip_ms',
              'db_write_payload_build_ms',
              'db_write_attempted_rows_total',
              'db_write_duplicate_rows_total',
              'db_write_rows_total',
              'observability_raw_samples_seen',
              'observability_metric_records_seen',
              'observability_live_only_metric_records_skipped',
              'observability_live_only_raw_samples_skipped',
              'observability_rollup_rows_written',
              'observability_rollup_reduction_ratio',
              'observability_source_budget_reduction_ratio',
              'observability_export_db_ms',
              'observability_export_errors'
          )
        GROUP BY metric_name, pipeline_stage, message_kind
        ORDER BY metric_name, sample_count DESC, pipeline_stage, message_kind
        """,
        {"run_id": run_id},
    )
    _print_json({"run_id": run_id, "latency": rows})
    return 0


def _cmd_observability_storage_budget(args: argparse.Namespace) -> int:
    run_id = str(getattr(args, "run_id", "") or "").strip()
    scope = "WHERE run_id = :run_id" if run_id else ""
    params = {"run_id": run_id} if run_id else {}
    summary = _query_one(
        f"""
        SELECT
            COUNT(*) AS rollup_rows,
            COALESCE(SUM(raw_sample_count), 0) AS raw_samples_seen,
            COALESCE(SUM(source_metric_record_count), 0) AS source_metric_records_seen,
            GREATEST(COALESCE(SUM(raw_sample_count), 0) - COUNT(*), 0) AS estimated_rows_avoided,
            GREATEST(COALESCE(SUM(raw_sample_count), 0) - COALESCE(SUM(source_metric_record_count), 0), 0) AS source_records_avoided,
            ROUND((COALESCE(SUM(raw_sample_count), 0)::numeric / NULLIF(COUNT(*), 0)), 3) AS reduction_ratio,
            ROUND((COALESCE(SUM(raw_sample_count), 0)::numeric / NULLIF(SUM(source_metric_record_count), 0)), 3) AS source_budget_reduction_ratio,
            COALESCE(SUM(pg_column_size(to_jsonb(r))), 0) AS approx_rollup_bytes,
            MIN(bucket_start) AS first_bucket,
            MAX(bucket_start) AS latest_bucket
        FROM observability_metrics.botlens_backend_metric_rollups_v1 r
        {scope}
        """,
        params,
    )
    top_metrics = _query(
        f"""
        SELECT
            metric_name,
            component,
            storage_target,
            COUNT(*) AS rollup_rows,
            COALESCE(SUM(raw_sample_count), 0) AS raw_samples_seen,
            COALESCE(SUM(source_metric_record_count), 0) AS source_metric_records_seen,
            GREATEST(COALESCE(SUM(raw_sample_count), 0) - COUNT(*), 0) AS estimated_rows_avoided,
            GREATEST(COALESCE(SUM(raw_sample_count), 0) - COALESCE(SUM(source_metric_record_count), 0), 0) AS source_records_avoided,
            ROUND((COALESCE(SUM(raw_sample_count), 0)::numeric / NULLIF(COUNT(*), 0)), 3) AS reduction_ratio,
            ROUND((COALESCE(SUM(raw_sample_count), 0)::numeric / NULLIF(SUM(source_metric_record_count), 0)), 3) AS source_budget_reduction_ratio,
            COALESCE(SUM(pg_column_size(to_jsonb(r))), 0) AS approx_bytes
        FROM observability_metrics.botlens_backend_metric_rollups_v1 r
        {scope}
        GROUP BY metric_name, component, storage_target
        ORDER BY raw_samples_seen DESC, rollup_rows DESC, metric_name
        LIMIT :limit
        """,
        {**params, "limit": max(int(getattr(args, "limit", 20) or 20), 1)},
    )
    per_minute = _query(
        f"""
        SELECT
            date_trunc('minute', bucket_start) AS bucket,
            COUNT(*) AS rollup_rows,
            COALESCE(SUM(raw_sample_count), 0) AS raw_samples_seen,
            COALESCE(SUM(source_metric_record_count), 0) AS source_metric_records_seen,
            GREATEST(COALESCE(SUM(raw_sample_count), 0) - COUNT(*), 0) AS estimated_rows_avoided,
            GREATEST(COALESCE(SUM(raw_sample_count), 0) - COALESCE(SUM(source_metric_record_count), 0), 0) AS source_records_avoided,
            COALESCE(SUM(pg_column_size(to_jsonb(r))), 0) AS approx_bytes
        FROM observability_metrics.botlens_backend_metric_rollups_v1 r
        {scope}
        GROUP BY bucket
        ORDER BY bucket
        """,
        params,
    )
    policy_summary = _query_one(
        f"""
        SELECT
            COALESCE(SUM(value_sum) FILTER (WHERE metric_name = 'observability_live_only_metric_records_skipped'), 0) AS live_only_metric_records_skipped,
            COALESCE(SUM(value_sum) FILTER (WHERE metric_name = 'observability_live_only_raw_samples_skipped'), 0) AS live_only_raw_samples_skipped,
            COALESCE(SUM(value_sum) FILTER (WHERE metric_name = 'observability_metric_records_seen'), 0) AS exporter_metric_records_seen,
            COALESCE(SUM(value_sum) FILTER (WHERE metric_name = 'observability_raw_samples_seen'), 0) AS exporter_raw_samples_seen,
            ROUND(MAX(latest_value) FILTER (WHERE metric_name = 'observability_source_budget_reduction_ratio')::numeric, 3) AS max_source_budget_reduction_ratio
        FROM observability_metrics.botlens_backend_metric_rollups_v1 r
        {scope}
        """,
        params,
    )
    _print_json(
        {
            "run_id": run_id or None,
            "summary": summary,
            "policy_summary": policy_summary,
            "top_metrics": top_metrics,
            "per_minute": per_minute,
        }
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect runtime event persistence throughput and ordering.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_run_command(name: str, help_text: str) -> argparse.ArgumentParser:
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--run-id", required=True)
        return command

    throughput = add_run_command("throughput", "Print runtime event throughput by minute.")
    throughput.set_defaults(func=_cmd_throughput)

    event_summary = add_run_command("event-summary", "Print runtime event counts by type/name.")
    event_summary.set_defaults(func=_cmd_event_summary)

    storage_budget = subparsers.add_parser("storage-budget", help="Estimate runtime event storage budget by tier.")
    storage_budget.add_argument("--run-id", required=False)
    storage_budget.set_defaults(func=_cmd_storage_budget)

    seq_gaps = add_run_command("seq-gaps", "Check runtime run_seq gaps and duplicates.")
    seq_gaps.add_argument("--limit", type=int, default=20)
    seq_gaps.set_defaults(func=_cmd_seq_gaps)

    write_latency = add_run_command("write-latency", "Summarize runtime event DB write metrics.")
    write_latency.set_defaults(func=_cmd_write_latency)

    observability_storage = subparsers.add_parser(
        "observability-storage-budget",
        help="Estimate durable observability metric rollup storage budget.",
    )
    observability_storage.add_argument("--run-id", required=False)
    observability_storage.add_argument("--limit", type=int, default=20)
    observability_storage.set_defaults(func=_cmd_observability_storage_budget)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
