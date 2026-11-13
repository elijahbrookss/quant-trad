"""Admit public event observations, resolve their entry and attach calculations."""
from __future__ import annotations
from bisect import bisect_left
from collections import Counter
from datetime import datetime, timedelta
from typing import Any
from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from .event_time import _utc, _iso, _known_at
from .event_configuration import _INDICATOR_EVENT_DETECTOR, _FACT_SNAPSHOT_DETECTOR, _REQUIRED_FACTS_AVAILABLE
from .event_facts import _fact_material, _selected_record_key
from .event_features import baseline_features
from .event_outcomes import resolve_event_outcomes


def evaluate_event_observations(
    *,
    detector,
    detector_type,
    direction_by_key,
    output_name,
    event_rows,
    outcomes,
    interval_seconds,
    evaluation_start,
    evaluation_end,
    candles,
    candle_by_open,
    detector_snapshots,
    baseline_specs,
    atr_by_period,
):
    selected_fact_material: dict[tuple[Any, ...], dict[str, Any]] = {}
    exclusion_counts: Counter[str] = Counter()
    event_results: list[dict[str, Any]] = []
    horizon_resolution: dict[int, Counter[str]] = {
        int(horizon): Counter() for horizon in outcomes["horizons"]
    }
    invalidation_resolution: Counter[str] = Counter()
    entry_lag = int(outcomes.get("entry_lag_bars") or 0)
    step = timedelta(seconds=interval_seconds)
    available_price_candles = sorted(
        (
            (_utc(row["close_time"], field="candle.close_time"), opened)
            for opened, row in candle_by_open.items()
            if _utc(row.get("known_at") or row["close_time"], field="candle.known_at")
            <= _utc(row["close_time"], field="candle.close_time")
        ),
    ) if detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE else []
    available_price_times = [row[0] for row in available_price_candles]
    for output in event_rows:
        execution_checkpoint()
        event_time = _utc(output.get("time"), field="event.time")
        event_payload = dict(output.get("event") or {})
        event_decision_time = _known_at(
            event_payload.get("known_at")
            or output.get("known_at")
            or candle_by_open.get(event_time, {}).get("known_at")
            or candle_by_open.get(event_time, {}).get("close_time"),
            field="event.known_at",
        )
        if not (
            evaluation_start <= event_time < evaluation_end
            and event_decision_time <= evaluation_end
        ):
            continue
        direction_text = str(event_payload.get("direction") or "").strip().lower()
        if detector_type == _INDICATOR_EVENT_DETECTOR:
            configured_direction = direction_by_key[str(output.get("event_key"))]
            if direction_text not in {"long", "short"}:
                raise RuntimeError(
                    "event_fact_event_direction_missing: Indicator event must own long/short direction"
                )
            if direction_text != configured_direction:
                raise RuntimeError(
                    "event_fact_event_direction_mismatch: configured direction cannot relabel Indicator evidence"
                )
            direction = 1.0 if direction_text == "long" else -1.0
        else:
            if direction_text != "neutral":
                raise RuntimeError(
                    "event_fact_event_ownership_invalid: Check-owned fact snapshots must be neutral"
                )
            direction = 1.0
        entry_time = event_time + step * entry_lag
        if detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE:
            earliest_price = max(event_decision_time, entry_time + step)
            price_index = bisect_left(available_price_times, earliest_price)
            entry_time = (
                available_price_candles[price_index][1]
                if price_index < len(available_price_candles) else None
            )
        event_candle = candle_by_open.get(event_time)
        entry = candle_by_open.get(entry_time)
        row: dict[str, Any] = {
            "event_time": _iso(event_time),
            "decision_time": _iso(event_decision_time),
            "entry_time": None,
            "entry_known_at": None,
            "event_key": output.get("event_key"),
            "direction": direction_text,
            "indicator_id": (
                output.get("indicator_id")
                if detector_type == _INDICATOR_EVENT_DETECTOR
                else None
            ),
            "indicator_output": output_name,
            "event": dict(output.get("event") or {}),
            "features": {},
            "outcomes": {},
            "eligible": True,
            "exclusion_reasons": [],
        }
        if detector_type == _FACT_SNAPSHOT_DETECTOR:
            row["detector_input_alias"] = detector.get("input_alias")
            detector_exclusion = str(
                output.get("detector_exclusion_reason") or ""
            ).strip()
            detector_record = output.get("detector_record")
            if detector_exclusion:
                row["eligible"] = False
                row["exclusion_reasons"].append(detector_exclusion)
                exclusion_counts[detector_exclusion] += 1
            if detector_record is not None:
                detector_alias = str(detector["input_alias"])
                detector_material = {
                    "alias": detector_alias,
                    "role": "detector",
                    **_fact_material(detector_record),
                }
                row["detector_fact_reference"] = detector_material
                selected_fact_material[
                    _selected_record_key(detector_alias, detector_record)
                ] = detector_material
            else:
                row["detector_fact_reference"] = None
        if output.get("detector_exclusion_reason") == "required_facts_unavailable":
            row["decision_time"] = None
            row["population_eligible"] = False
            row["analysis_eligible"] = False
            row["fact_references"] = {}
            for horizon in outcomes["horizons"]:
                row["outcomes"][str(horizon)] = {
                    "status": "unresolved", "reason": "required_facts_unavailable",
                    "horizon_kind": outcomes["horizon_kind"],
                }
                horizon_resolution[int(horizon)]["unresolved:required_facts_unavailable"] += 1
            event_results.append(row)
            continue
        effective_entry_lag = (
            int((entry_time - event_time) / step)
            if entry_time is not None else entry_lag
        )
        entry_path_missing = [
            event_time + step * offset
            for offset in range(0, effective_entry_lag + 1)
            if event_time + step * offset not in candle_by_open
        ]
        if event_candle is None or entry is None or entry_path_missing:
            entry_reason = (
                "entry_bar_missing"
                if event_candle is None or entry is None
                else "gap_before_delayed_entry"
            )
            row["eligible"] = False
            row["exclusion_reasons"].append(entry_reason)
            exclusion_counts[entry_reason] += 1
            for horizon in outcomes["horizons"]:
                row["outcomes"][str(horizon)] = {
                    "status": "unresolved",
                    "reason": entry_reason,
                    "horizon_kind": outcomes["horizon_kind"],
                }
                horizon_resolution[int(horizon)][f"unresolved:{entry_reason}"] += 1
            row["population_eligible"] = False
            row["analysis_eligible"] = False
            row["fact_references"] = {}
            event_results.append(row)
            continue
        entry_known_at = _utc(
            entry.get("known_at") or entry.get("close_time"),
            field="entry.known_at",
        )
        entry_close_time = _utc(
            entry.get("close_time") or entry.get("known_at"),
            field="entry.close_time",
        )
        row["entry_time"] = _iso(entry_time)
        row["entry_known_at"] = _iso(entry_known_at)
        if detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE:
            row["entry_sample_time"] = _iso(entry_close_time)
            row["entry_price_rule"] = "first_available_candle_close_at_or_after_decision.v1"
            for reference in event_payload.get("metadata", {}).get("required_fact_references", []):
                selected_fact_material[(reference["alias"], -1, "trigger", semantic_hash(reference), 0, 0)] = {
                    **reference, "role": "trigger",
                }
        entry_close = float(entry["close"])
        event_close = float(event_candle["close"])
        row["entry_price"] = entry_close
        row["event_price"] = event_close

        resolve_event_outcomes(
            row=row, outcomes=outcomes, detector_type=detector_type,
            entry_time=entry_time, entry_close_time=entry_close_time,
            entry_close=entry_close, direction=direction, step=step,
            evaluation_end=evaluation_end, candles=candles,
            candle_by_open=candle_by_open, event_payload=event_payload,
            horizon_resolution=horizon_resolution,
            invalidation_resolution=invalidation_resolution,
        )

        row["features"].update(baseline_features(
            baseline_specs=baseline_specs, output=output, direction=direction,
            event_time=event_time, step=step, event_close=event_close,
            event_candle=event_candle, candle_by_open=candle_by_open,
            atr_by_period=atr_by_period,
        ))

        unresolved_required = [
            int(horizon)
            for horizon in outcomes["required_horizons"]
            if (row["outcomes"].get(str(horizon)) or {}).get("status")
            != "resolved"
        ]
        for horizon in unresolved_required:
            outcome = row["outcomes"].get(str(horizon)) or {}
            reason = (
                f"required_outcome_unresolved:{horizon}:{outcome.get('reason')}"
            )
            row["eligible"] = False
            row["exclusion_reasons"].append(reason)
            exclusion_counts[reason] += 1
        if any(
            row["features"].get(str(spec["name"])) is None
            for spec in baseline_specs
        ):
            row["eligible"] = False
            if "baseline_feature_incomplete" not in row["exclusion_reasons"]:
                row["exclusion_reasons"].append("baseline_feature_incomplete")
                exclusion_counts["baseline_feature_incomplete"] += 1
        row["population_eligible"] = bool(row["eligible"])
        row["analysis_eligible"] = False
        row["fact_references"] = {}
        event_results.append(row)

    return (event_results, selected_fact_material, exclusion_counts,
            horizon_resolution, invalidation_resolution, step)
