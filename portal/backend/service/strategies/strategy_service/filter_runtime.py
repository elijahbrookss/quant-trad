"""Runtime helpers for applying filter gates during strategy evaluation."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from strategies.evaluator import _extract_signal_epoch

from ...market.stats_repository import StatsSnapshot, build_stats_snapshot
from .filters import (
    FilterContext,
    FilterDefinition,
    collect_filter_versions,
    evaluate_filter_definitions,
    summarize_filter_results,
)


def build_filter_gate_snapshot(
    *,
    instrument_ids: Iterable[str],
    timeframe_seconds: int,
    start: datetime,
    end: datetime,
    global_filters: Sequence[FilterDefinition],
    rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]],
) -> StatsSnapshot:
    all_filters: List[FilterDefinition] = []
    all_filters.extend(global_filters)
    for filters in rule_filters_by_rule.values():
        all_filters.extend(filters)

    candle_versions, regime_versions, needs_latest_candle, needs_latest_regime = collect_filter_versions(
        all_filters
    )
    return build_stats_snapshot(
        instrument_ids=instrument_ids,
        timeframe_seconds=timeframe_seconds,
        start=start,
        end=end,
        candle_versions=candle_versions,
        regime_versions=regime_versions,
        include_latest_candle=needs_latest_candle,
        include_latest_regime=needs_latest_regime,
    )


def apply_filter_gates(
    *,
    rule_results: List[Mapping[str, Any]],
    instrument_id: str,
    timeframe_seconds: int,
    stats_snapshot: StatsSnapshot,
    global_filters: Sequence[FilterDefinition],
    rule_filters_by_rule: Mapping[str, Sequence[FilterDefinition]],
) -> None:
    def _signal_candle_time(signal: Optional[Mapping[str, Any]]) -> Optional[datetime]:
        epoch = _extract_signal_epoch(signal)
        if epoch is None:
            return None
        floored = epoch - (epoch % timeframe_seconds)
        try:
            return datetime.utcfromtimestamp(floored)
        except (OverflowError, OSError, ValueError):
            return None

    for res in rule_results:
        if not res.get("matched"):
            continue
        last_signal = None
        signals = res.get("signals") or []
        if isinstance(signals, list) and signals:
            last_signal = signals[-1]
        candle_time = _signal_candle_time(last_signal)
        context = FilterContext(
            instrument_id=instrument_id,
            candle_time=candle_time,
            candle_stats_latest=stats_snapshot.candle_stats_latest,
            candle_stats_by_version=stats_snapshot.candle_stats_by_version,
            regime_stats_latest=stats_snapshot.regime_stats_latest,
            regime_stats_by_version=stats_snapshot.regime_stats_by_version,
        )
        global_results = evaluate_filter_definitions(global_filters, context)
        rule_filters = rule_filters_by_rule.get(res.get("rule_id") or "", [])
        rule_results_eval = evaluate_filter_definitions(rule_filters, context)
        res["global_filters"] = global_results
        res["rule_filters"] = rule_results_eval
        global_passed, global_failed = summarize_filter_results(global_results)
        rule_passed, rule_failed = summarize_filter_results(rule_results_eval)
        allowed = bool(global_passed and rule_passed)
        reason_parts: List[str] = []
        if global_failed:
            reason_parts.append("global_filters_failed")
        if rule_failed:
            reason_parts.append("rule_filters_failed")
        if candle_time is None and (global_filters or rule_filters):
            reason_parts.append("signal_time_missing")
        res["final_decision"] = {
            "allowed": allowed,
            "reason": ",".join(reason_parts) if reason_parts else "allowed",
        }
