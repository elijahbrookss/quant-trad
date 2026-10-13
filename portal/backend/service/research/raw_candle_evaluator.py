"""Versioned canonical raw-candle predicates and bounded forward outcomes."""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
import math
from typing import Any

import pandas as pd

from data_providers.utils.ohlcv import interval_to_timedelta

from research_science.check import CheckDefinition, CheckRequest, ResolvedCheckPlan
from . import checks
from .event_outcomes import resolve_event_outcomes
from core.execution_control import execution_checkpoint

OUTCOME_SCHEMA = "raw_candle_forward_outcome.v1"
RESULT_SCHEMA = "raw_candle_forward_outcome_result.v1"
MAX_CANDLES = 100_000


def detector_fields(detector: Mapping[str, Any]) -> set[str]:
    fields: set[str] = set()
    for key in ("all", "any"):
        if key in detector:
            for child in detector[key]:
                fields.update(detector_fields(child))
            return fields
    if "not" in detector:
        return detector_fields(detector["not"])
    for key in ("field", "value_field"):
        if detector.get(key) is not None:
            fields.add(str(detector[key]).strip().lower())
    return fields


def _integer(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"raw_candle_check_invalid: {field} must be an integer in [{minimum},{maximum}]")
    return value


def _validate_predicate(detector):
    pending, count = [(detector, 0)], 0
    while pending:
        node, depth = pending.pop()
        count += 1
        if not isinstance(node, Mapping) or depth > 32 or count > 500:
            raise ValueError("raw_candle_check_invalid: predicate must be a bounded mapping tree")
        logical = set(node) & {"all", "any", "not"}
        if logical:
            if len(logical) != 1 or set(node) != logical:
                raise ValueError("raw_candle_check_invalid: logical nodes contain exactly one operator")
            key = next(iter(logical))
            children = [node[key]] if key == "not" else node[key]
            if not isinstance(children, list) or not children:
                raise ValueError("raw_candle_check_invalid: logical lists must be nonempty")
            pending.extend((child, depth + 1) for child in children)
        else:
            if set(node) - {"type", "field", "operator", "value", "value_field"}:
                raise ValueError("raw_candle_check_invalid: unsupported predicate fields")
            if node.get("value_field") is None and str(node.get("operator", "eq")).strip().lower() not in {"is_true", "true"}:
                value = node.get("value")
                values = value if isinstance(value, list) else [value]
                if not values or any(_finite(v) is None for v in values):
                    raise ValueError("raw_candle_check_invalid: finite numeric predicate literals required")
    checks.validate_check_detector(check_family=checks.RAW_FORWARD_OUTCOME, detector=detector)


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    _validate_predicate(detector)
    if outcomes.get("schema_version") != OUTCOME_SCHEMA:
        raise ValueError("raw_candle_check_invalid: explicit raw candle outcome schema is required")
    allowed = {"schema_version", "forward_bars", "entry_lag_bars", "direction", "min_sample_count",
               "min_edge_pct", "max_examples", "outcome_boundary"}
    if set(outcomes) - allowed:
        raise ValueError(f"raw_candle_check_invalid: unsupported outcomes {sorted(set(outcomes)-allowed)}")
    if inputs or statistics or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("raw_candle_check_invalid: candle-only method has no Indicator, typed aliases or statistical procedure")
    if gap_policy != "continue_degraded":
        raise ValueError("raw_candle_check_invalid: continue_degraded with explicit local path exclusions is required")
    if outcomes.get("outcome_boundary", "evaluation_end_exclusive") != "evaluation_end_exclusive":
        raise ValueError("raw_candle_check_invalid: outcomes must end strictly before evaluation end")
    horizons = outcomes.get("forward_bars", [1, 3, 5, 10])
    if not isinstance(horizons, list) or not horizons or len(horizons) > 20:
        raise ValueError("raw_candle_check_invalid: forward_bars must contain 1..20 positive integer horizons")
    horizons = sorted({_integer(h, field="forward_bars", minimum=1, maximum=10_000) for h in horizons})
    lag = _integer(outcomes.get("entry_lag_bars", 0), field="entry_lag_bars", minimum=0, maximum=10_000)
    if lag + max(horizons) > 10_000:
        raise ValueError("raw_candle_check_invalid: total entry and outcome path exceeds 10000 bars")
    direction = str(outcomes.get("direction", "long")).strip().lower()
    if direction not in {"long", "short"}:
        raise ValueError("raw_candle_check_invalid: direction must be long or short")
    edge = float(outcomes.get("min_edge_pct", 0.0))
    if not math.isfinite(edge):
        raise ValueError("raw_candle_check_invalid: min_edge_pct must be finite")
    return {"schema_version": OUTCOME_SCHEMA, "forward_bars": horizons, "entry_lag_bars": lag,
            "direction": direction, "min_sample_count": _integer(outcomes.get("min_sample_count", 20),
                field="min_sample_count", minimum=1, maximum=MAX_CANDLES),
            "min_edge_pct": edge, "max_examples": _integer(outcomes.get("max_examples", 250),
                field="max_examples", minimum=0, maximum=1000),
            "outcome_boundary": "evaluation_end_exclusive"}


def _clock(value: Any, field: str) -> pd.Timestamp:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"raw_candle_input_invalid: {field} must be a timestamp") from exc
    if pd.isna(parsed) or parsed.tzinfo is None:
        raise ValueError(f"raw_candle_input_invalid: {field} must have an explicit timezone")
    return parsed.tz_convert("UTC")


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _prepare(candles: pd.DataFrame, *, step: timedelta, volume_required: bool):
    if not isinstance(candles, pd.DataFrame) or len(candles) > MAX_CANDLES:
        raise ValueError("raw_candle_input_invalid: canonical frame required, maximum 100000 candles")
    required = {"timestamp", "open", "high", "low", "close", "close_time", "known_at",
                "known_at_method", "source_identity_key"}
    if volume_required:
        required.add("volume")
    if required - set(candles.columns):
        raise ValueError(f"raw_candle_input_invalid: missing canonical fields {sorted(required-set(candles.columns))}")
    frame = candles.reset_index(drop=True).copy(deep=True)
    rows = frame.to_dict("records")
    sources: set[str] = set()
    previous = None
    for index, row in enumerate(rows):
        if index % 256 == 0:
            execution_checkpoint()
        opened = _clock(row["timestamp"], "timestamp")
        closed = _clock(row["close_time"], "close_time")
        known = _clock(row["known_at"], "known_at")
        if previous is not None and opened <= previous:
            raise ValueError("raw_candle_input_invalid: duplicate or unordered canonical candle identity")
        if previous is not None and (opened-previous) % pd.Timedelta(step) != pd.Timedelta(0):
            raise ValueError("raw_candle_input_invalid: inconsistent canonical candle cadence")
        if closed != opened + step or known < closed:
            raise ValueError("raw_candle_input_invalid: invalid candle duration or availability clock")
        method = row["known_at_method"]
        source = row["source_identity_key"]
        if not isinstance(method, str) or not method.strip() or not isinstance(source, str) or not source.strip():
            raise ValueError("raw_candle_input_invalid: source and known_at_method must be retained")
        prices = {key: _finite(row[key]) for key in ("open", "high", "low", "close")}
        if (any(value is None or value <= 0 for value in prices.values())
                or prices["high"] < max(prices["open"], prices["close"])
                or prices["low"] > min(prices["open"], prices["close"])):
            raise ValueError("raw_candle_input_invalid: malformed or nonpositive OHLC; no row dropping")
        row.update(prices)
        row.update(open_time=opened, close_time=closed, known_at=known, known_at_method=method)
        previous = opened
        sources.add(source)
    if len(sources) > 1:
        raise ValueError("raw_candle_input_invalid: one exact canonical candle source is required")
    return frame, rows


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RawCandleEvaluator:
    evaluator_id: str = checks.RAW_FORWARD_OUTCOME
    version: str = "3"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition: CheckDefinition, request: CheckRequest):
        del definition
        fields = detector_fields(request.parameters["detector"])
        outcomes = request.parameters["outcomes"]
        lookback = int(any(field.startswith("previous_") for field in fields))
        warmup = max(lookback, _integer(request.scope.get("warmup_bars") or 0,
            field="scope.warmup_bars", minimum=0, maximum=MAX_CANDLES))
        start = _clock(request.scope.get("start"), "scope.start")
        end = _clock(request.scope.get("end"), "scope.end")
        step = interval_to_timedelta(str(request.scope.get("timeframe") or request.scope.get("interval") or ""))
        if step.total_seconds() <= 0 or not 0 < (end-start).total_seconds() <= (MAX_CANDLES-warmup)*step.total_seconds():
            raise ValueError("raw_candle_check_invalid: nominal materialization exceeds 100000 candles or has an empty range")
        required = ["open", "high", "low", "close", "known_at"]
        if any(field.removeprefix("previous_") == "volume" for field in fields):
            required.append("volume")
        return {"input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": 0,
                "feature_lookback_bars": lookback,
                "feature_windows_seconds_by_alias": {}, "outcome_horizons": outcomes["forward_bars"],
                "required_outcome_horizons": outcomes["forward_bars"], "horizon_kind": "bars",
                "entry_lag_bars": outcomes["entry_lag_bars"], "invalidation_max_bars": 0,
                "outcome_boundary": "evaluation_end_exclusive", "primary_candle_required_fields": required}

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]):
        detector = inputs["detector"]
        outcomes = inputs["outcomes"]
        fields = detector_fields(detector)
        previous_required = any(field.startswith("previous_") for field in fields)
        volume_required = any(field.removeprefix("previous_") == "volume" for field in fields)
        primary = next(r for r in plan.market_data_requirements if r["alias"] == "primary_bars")
        seconds = int(primary["timeframe_seconds"])
        if seconds <= 0:
            raise ValueError("raw_candle_input_invalid: positive canonical timeframe is required")
        step = timedelta(seconds=seconds)
        start, end = (_clock(plan.evaluation_range[key], key) for key in ("start", "end_exclusive"))
        if start >= end:
            raise ValueError("raw_candle_input_invalid: evaluation range must be nonempty")
        frame, rows = _prepare(inputs["candles"], step=step, volume_required=volume_required)
        by_open = {row["open_time"]: row for row in rows}
        indexes = {row["open_time"]: index for index, row in enumerate(rows)}
        horizons = outcomes["forward_bars"]
        candidate_exclusions, event_exclusions = Counter(), Counter()
        horizon_resolution = {h: Counter() for h in horizons}
        baseline, events, examples = [], [], []
        eligible = predicate_eligible = complete = 0
        direction = -1.0 if outcomes["direction"] == "short" else 1.0
        for index, current in enumerate(rows):
            if index % 128 == 0:
                execution_checkpoint()
            opened = current["open_time"]
            if not start <= opened < end:
                continue
            eligible += 1
            prior = by_open.get(opened-step) if previous_required else None
            if previous_required and prior is None:
                candidate_exclusions["previous_bar_missing"] += 1
                continue
            condition_rows = [current] + ([prior] if prior is not None else [])
            volume_rows = ([current] if "volume" in fields else []) + ([prior] if "previous_volume" in fields else [])
            if any(_finite(r.get("volume")) is None or _finite(r.get("volume")) < 0
                   for r in volume_rows):
                candidate_exclusions["predicate_volume_unavailable"] += 1
                continue
            predicate_eligible += 1
            matched = checks._evaluate_detector(detector, frame, index)
            decision = max(r["known_at"] for r in condition_rows)
            entry_open = opened + step*outcomes["entry_lag_bars"]
            entry = by_open.get(entry_open)
            entry_reason = None
            if entry is None or any(opened+step*i not in by_open for i in range(outcomes["entry_lag_bars"]+1)):
                entry_reason = "gap_in_entry_path"
            elif entry["close_time"] >= end:
                entry_reason = "entry_outside_evaluation"
            elif decision > entry["close_time"]:
                entry_reason = "predicate_known_after_entry_close"
            elif entry["known_at"] > entry["close_time"]:
                entry_reason = "entry_price_known_after_close"
            resolved, unresolved = {}, {}
            if entry_reason:
                unresolved = {h: entry_reason for h in horizons}
            else:
                admissible = []
                for h in horizons:
                    if entry["close_time"] + step*h >= end:
                        unresolved[h] = "target_outside_evaluation"
                    else:
                        admissible.append(h)
                row = {"outcomes": {}}
                counters = {h: Counter() for h in admissible}
                resolve_event_outcomes(row=row, outcomes={"horizons": admissible, "horizon_kind": "bars"},
                    detector_type=checks.RAW_FORWARD_OUTCOME, entry_time=entry_open,
                    entry_close_time=entry["close_time"], entry_close=entry["close"], direction=direction,
                    step=step, evaluation_end=end, candles=rows, candle_by_open=by_open,
                    event_payload={}, horizon_resolution=counters, invalidation_resolution=Counter())
                for h in admissible:
                    value = row["outcomes"][str(h)]
                    if value["status"] != "resolved":
                        unresolved[h] = value["reason"]
                        continue
                    if any(not math.isfinite(value[key]) for key in
                           ("forward_return", "maximum_favorable_excursion", "maximum_adverse_excursion")):
                        unresolved[h] = "outcome_numeric_unavailable"
                        continue
                    path = [by_open[entry_open+step*i] for i in range(1,h+1)]
                    resolved[h] = {"forward_return_pct": value["forward_return"]*direction,
                        "max_favorable_excursion_pct": value["maximum_favorable_excursion"],
                        "max_adverse_excursion_pct": value["maximum_adverse_excursion"],
                        "target_time": _iso(path[-1]["close_time"]),
                        "outcome_known_at": _iso(max([entry["known_at"]]+[r["known_at"] for r in path]))}
            event = checks.EventOutcome(index, _iso(opened), current["close"],
                indexes.get(entry_open, -1), _iso(entry["close_time"]) if entry is not None else "",
                entry["close"] if entry is not None else 0.0, resolved)
            baseline.append(event)
            if not matched:
                continue
            events.append(event)
            if entry_reason:
                event_exclusions[entry_reason] += 1
            if len(resolved) == len(horizons):
                complete += 1
            for h in horizons:
                horizon_resolution[h]["resolved" if h in resolved else "unresolved:"+unresolved[h]] += 1
            if len(examples) < outcomes["max_examples"]:
                examples.append({"event_time": _iso(opened), "event_close_time": _iso(current["close_time"]),
                    "decision_known_at": _iso(decision), "entry_time": event.entry_time or None,
                    "entry_price_rule": "canonical_candle_close", "entry_reason": entry_reason,
                    "outcomes": {str(h): {"status": "resolved", **resolved[h]} if h in resolved
                        else {"status": "unresolved", "reason": unresolved[h]} for h in horizons}})
        summary = checks._summarize_outcomes(events, baseline, forward_bars=horizons,
            min_sample_count=outcomes["min_sample_count"], min_edge_pct=outcomes["min_edge_pct"])
        return {"schema_version": RESULT_SCHEMA, "check_family": checks.RAW_FORWARD_OUTCOME,
                "status": "completed", "event_ownership": "check", "sample_count": len(events),
                "complete_sample_count": complete, "eligible_bars": eligible,
                "predicate_eligible_bars": predicate_eligible, "candidate_exclusions": dict(candidate_exclusions),
                "entry_exclusions": dict(event_exclusions), "detector": dict(detector),
                "outcomes": {**dict(outcomes), "summary": summary["by_window"]},
                "outcome_resolution": {str(h): dict(v) for h,v in horizon_resolution.items()},
                "known_at_methods": dict(Counter(r["known_at_method"] for r in rows)),
                "return_unit": "fraction", "data_quality": dict(inputs.get("data_quality") or {}),
                "recommendation": summary["recommendation"], "caveats": summary["caveats"] + [
                    "Returns are fractions despite retained _pct field names; multiply by100 only for display.",
                    "All-bar baseline is not an opposing-cohort contrast. Overlapping outcomes are descriptive, not independent, causal or profitable evidence.",
                    "Known-at method labels are retained; inferred interval-close availability is not measured publication latency."],
                "events": examples, "event_count_truncated": max(0,len(events)-len(examples))}
