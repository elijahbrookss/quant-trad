"""Check-owned descriptive attribution on one canonical Indicator timeline."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from statistics import fmean
from typing import Any, Mapping

from market_data.frozen import semantic_hash
from research_science.check import ResolvedCheckPlan

from .event_fact_evaluator import EventFactEvaluator, _candle_rows, normalize_event_fact_configuration


def _invalid(message: str) -> ValueError:
    return ValueError(f"matched_origin_invalid: {message}")


def _time(value: Any) -> datetime:
    if isinstance(value, bool) or value is None:
        raise _invalid("an explicit event/candle clock is required")
    if isinstance(value, (int, float)):
        if not isfinite(value):
            raise _invalid("event clock must be finite")
        return datetime.fromtimestamp(value, UTC)
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise _invalid("event/candle clocks must include a timezone")
    return result.astimezone(UTC)


def _number(value: Any, *, field: str, positive: bool = False) -> float:
    if isinstance(value, bool) or value is None:
        raise _invalid(f"{field} must be finite")
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise _invalid(f"{field} must be numeric") from exc
    if not isfinite(result) or (positive and result <= 0):
        raise _invalid(f"{field} must be finite" + (" and positive" if positive else ""))
    return result


def _path(event: Mapping[str, Any], path: str) -> Any:
    value: Any = event
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            raise _invalid(f"required event identity is missing: {path}")
        value = value[part]
    return value


def normalize_matched_origin(
    config: Mapping[str, Any], *, detector: Mapping[str, Any],
    outcomes: Mapping[str, Any], statistics: Mapping[str, Any],
) -> dict[str, Any]:
    allowed = {"detector", "origin_time_path", "origin_event_key_path", "reference_path"}
    if not isinstance(config, Mapping) or set(config) != allowed:
        raise _invalid("matched_origin requires exactly detector, origin_time_path, origin_event_key_path and reference_path")
    if detector.get("type") != "indicator_event" or outcomes.get("horizon_kind") != "bars":
        raise _invalid("only candle Indicator events with bar horizons are supported")
    if any((statistics.get("features") or {}).values()) or any(
        statistics.get(key) for key in ("folds", "model", "bootstrap", "direct_tests", "feature_bins", "purge_bars", "embargo_bars")
    ):
        raise _invalid("attribution is descriptive only; feature/model/inference configuration is unsupported")
    if any(statistics.get("eligibility", {}).get(key) for key in (
        "min_class_count", "min_validation_samples_per_fold", "min_valid_folds"
    )):
        raise _invalid("attribution supports only sample-count and UTC-day eligibility thresholds")
    raw_followup = config["detector"]
    if not isinstance(raw_followup, Mapping) or set(raw_followup) != {"type", "output_name", "event_keys"}:
        raise _invalid("follow-up detector requires exactly type, output_name and event_keys")
    followup, _, _ = normalize_event_fact_configuration(
        detector=raw_followup, outcomes=outcomes, statistics={},
    )
    if followup.get("type") != "indicator_event" or followup["output_name"] == detector["output_name"]:
        raise _invalid("follow-up must be a distinct Indicator signal output")
    if {row["direction"] for row in followup["event_keys"]} != {row["direction"] for row in detector["event_keys"]}:
        raise _invalid("origin and follow-up direction sets must match")
    result: dict[str, Any] = {"detector": followup}
    for key in ("origin_time_path", "origin_event_key_path", "reference_path"):
        value = config[key]
        if not isinstance(value, str) or not value.startswith("metadata.") or any(not part for part in value.split(".")):
            raise _invalid(f"{key} must be an explicit metadata path")
        result[key] = value
    return result


def _identity(row: Mapping[str, Any], config: Mapping[str, Any], *, followup: bool) -> tuple[Any, ...]:
    event = row["event"]
    origin = _path(event, config["origin_time_path"]) if followup else row["event_time"]
    key = _path(event, config["origin_event_key_path"]) if followup else row["event_key"]
    if not isinstance(key, str) or not key or not row.get("indicator_id"):
        raise _invalid("Indicator and origin event key are required")
    return row["indicator_id"], row["direction"], key, _time(origin)


def _reference(row: Mapping[str, Any], config: Mapping[str, Any]) -> Mapping[str, Any]:
    value = _path(row["event"], config["reference_path"])
    if not isinstance(value, Mapping) or not value.get("key") or not value.get("source"):
        raise _invalid("reference must carry immutable key, source and price")
    _number(value.get("price"), field="reference.price", positive=True)
    return value


def _entry_state(row: Mapping[str, Any], reference: Mapping[str, Any]) -> str:
    if row.get("entry_price") is None:
        return "unavailable"
    price = _number(row["entry_price"], field="entry_price", positive=True)
    boundary = _number(reference["price"], field="reference.price", positive=True)
    difference = (price - boundary) * (1 if row["direction"] == "long" else -1)
    return "outside" if difference > 0 else "inside" if difference < 0 else "at_boundary"


def _mean(values: list[float]) -> float | None:
    return fmean(values) if values else None


def _delta(left: float | None, right: float | None) -> float | None:
    return None if left is None or right is None else left - right


def _clock(row: Mapping[str, Any], candles: Mapping[datetime, Mapping[str, Any]]) -> dict[str, Any]:
    opened = _time(row["entry_time"])
    candle = candles[opened]
    close = _time(candle["close_time"])
    known = _time(row["entry_known_at"])
    decision = _time(row["decision_time"])
    if known != close or decision > close:
        raise _invalid("entry close was not a contemporaneously available post-decision sample")
    return {"entry_bar_open": opened.isoformat(), "entry_sample_close": close.isoformat(), "entry_known_at": known.isoformat(), "decision_time": decision.isoformat()}


def _attribute(
    raw: Mapping[str, Any], followup: Mapping[str, Any], *, config: Mapping[str, Any],
    outcomes: Mapping[str, Any], statistics: Mapping[str, Any], candle_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    origins = list(raw["events"])
    confirmations = list(followup["events"])
    candles: dict[datetime, Mapping[str, Any]] = {}
    for candle in candle_rows:
        key = _time(candle.get("open_time") or candle.get("time"))
        if key in candles:
            raise _invalid("duplicate primary candle open time")
        candles[key] = candle
    for arm in (origins, confirmations):
        for row in arm:
            if row["population_eligible"]:
                _clock(row, candles)
                _number(row.get("entry_price"), field="entry_price", positive=True)
    origin_index: dict[tuple[Any, ...], int] = {}
    for index, row in enumerate(origins):
        key = _identity(row, config, followup=False)
        _reference(row, config)
        if key in origin_index:
            raise _invalid(f"ambiguous duplicate origin: indicator={key[0]} event={key[2]} time={key[3].isoformat()}")
        origin_index[key] = index
    pairs: dict[int, int] = {}
    seen_followups: set[tuple[Any, ...]] = set()
    unmatched = []
    for index, row in enumerate(confirmations):
        key = _identity(row, config, followup=True)
        ref = _reference(row, config)
        if key in seen_followups:
            raise _invalid(f"ambiguous duplicate follow-up: indicator={key[0]} event={key[2]} time={key[3].isoformat()}")
        seen_followups.add(key)
        origin_index_value = origin_index.get(key)
        reason = None
        if origin_index_value is None:
            reason = "origin_absent_in_evaluation_window"
        else:
            origin = origins[origin_index_value]
            if _time(row["event_time"]) < _time(origin["event_time"]) or _time(row["decision_time"]) < _time(origin["decision_time"]):
                raise _invalid("follow-up precedes its origin")
            if semantic_hash(ref) != semantic_hash(_reference(origin, config)):
                reason = "origin_reference_changed"
        if reason:
            unmatched.append({"followup_index": index, "origin_index": origin_index_value, "reason": reason})
        else:
            pairs[origin_index_value] = index
    records = []
    for index, origin in enumerate(origins):
        record: dict[str, Any] = {
            "origin_index": index, "origin_time": origin["event_time"],
            "origin_entry_state": _entry_state(origin, _reference(origin, config)),
            "followup_index": pairs.get(index),
            "status": "matched" if index in pairs else "unmatched",
            "reason": None if index in pairs else "no_matching_followup_in_evaluation_window",
            "horizons": {},
        }
        records.append(record)
        if index not in pairs:
            continue
        confirmed = confirmations[pairs[index]]
        record["followup_entry_state"] = _entry_state(confirmed, _reference(confirmed, config))
        for horizon in outcomes["horizons"]:
            h = str(horizon)
            left, right = origin["outcomes"].get(h, {}), confirmed["outcomes"].get(h, {})
            reason = None
            if not origin["population_eligible"] or not confirmed["population_eligible"]:
                reason = "arm_population_ineligible"
            elif left.get("status") != "resolved" or right.get("status") != "resolved":
                reason = "arm_outcome_unresolved"
            if reason:
                record["horizons"][h] = {"status": "unresolved", "reason": reason}
                continue
            raw_clock, confirmed_clock = _clock(origin, candles), _clock(confirmed, candles)
            target = candles[_time(left["target_time"])]
            target_close = _time(target["close_time"])
            if _time(confirmed_clock["entry_sample_close"]) > target_close:
                record["horizons"][h] = {"status": "unresolved", "reason": "followup_entry_after_common_endpoint"}
                continue
            e1 = _number(confirmed["entry_price"], field="followup.entry_price", positive=True)
            price = _number(target["close"], field="common_endpoint.price", positive=True)
            direction = 1 if origin["direction"] == "long" else -1
            record["horizons"][h] = {
                "status": "resolved", "reason": None,
                "origin_clock": raw_clock, "followup_clock": confirmed_clock,
                "common_endpoint_bar_open": left["target_time"],
                "common_endpoint_close": target_close.isoformat(),
                "common_endpoint_known_at": _time(target.get("known_at") or target["close_time"]).isoformat(),
                "followup_endpoint_bar_open": right["target_time"],
                "followup_endpoint_close": _time(candles[_time(right["target_time"])]["close_time"]).isoformat(),
                "origin_signed_return": _number(left["direction_signed_forward_return"], field="origin.return"),
                "common_endpoint_signed_return": _number(direction * (price / e1 - 1), field="common_endpoint.return"),
                "followup_signed_return": _number(right["direction_signed_forward_return"], field="followup.return"),
            }
    summaries = {}
    for horizon in outcomes["horizons"]:
        h = str(horizon)
        raw_values = [_number(row["outcomes"][h]["direction_signed_forward_return"], field="origin.return") for row in origins if row["population_eligible"] and row["outcomes"][h]["status"] == "resolved"]
        followup_values = [_number(row["outcomes"][h]["direction_signed_forward_return"], field="followup.return") for row in confirmations if row["population_eligible"] and row["outcomes"][h]["status"] == "resolved"]
        matched = [row["horizons"][h] for row in records if row["horizons"].get(h, {}).get("status") == "resolved"]
        a, e = _mean(raw_values), _mean(followup_values)
        b = _mean([row["origin_signed_return"] for row in matched])
        c = _mean([row["common_endpoint_signed_return"] for row in matched])
        d = _mean([row["followup_signed_return"] for row in matched])
        paired_origins = [origins[row["origin_index"]] for row in records
                          if row["horizons"].get(h, {}).get("status") == "resolved"]
        distinct_days = len({_time(row["decision_time"]).date() for row in paired_origins})
        criteria = dict(statistics.get("eligibility") or {})
        reasons = []
        if not matched:
            reasons.append("no_common_pairs")
        if len(matched) < criteria.get("min_samples", 0):
            reasons.append("minimum_common_pair_count_not_met")
        if distinct_days < criteria.get("min_distinct_utc_days", 0):
            reasons.append("minimum_common_pair_utc_days_not_met")
        summaries[h] = {
            "paired_eligibility": {"eligible": not reasons, "reasons": reasons,
                                   "criteria": criteria, "distinct_origin_utc_days": distinct_days},
            "horizon_kind": "bars", "raw_count": len(raw_values),
            "followup_count": len(followup_values), "common_pair_count": len(matched),
            "followup_population_reconciled": len(matched) == len(followup_values) and bool(matched),
            "unresolved_pair_reasons": dict(sorted(Counter(row["horizons"][h]["reason"] for row in records if row["horizons"].get(h, {}).get("status") == "unresolved").items())),
            "raw_all_mean": a, "raw_matched_mean": b, "common_endpoint_mean": c,
            "followup_matched_mean": d, "followup_all_mean": e,
            "selection_difference": _delta(b, a), "entry_repricing_difference": _delta(c, b),
            "endpoint_extension_difference": _delta(d, c), "unmatched_followup_residual": _delta(e, d),
            "observed_mean_difference": _delta(e, a),
        }
    return {
        "schema_version": "matched_origin_attribution.v1",
        "interpretation": "retrospective_descriptive_accounting_not_causal_or_executable",
        "matching": "exact_indicator_direction_origin_key_time_and_reference",
        "followup_scope": "same_evaluation_window_unmatched_is_not_proof_of_no_future_confirmation",
        "origin_count": len(origins), "followup_count": len(confirmations), "matched_count": len(pairs),
        "unmatched_origin_count": len(origins) - len(pairs), "unmatched_followups": unmatched,
        "origins": records, "horizons": summaries,
    }


@dataclass(frozen=True)
class MatchedOriginEvaluator(EventFactEvaluator):
    version: str = "6"
    result_schema_version: str = "event_fact_analysis_result.v6"
    descriptive_outcomes_enabled: bool = True

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        outcomes = dict(inputs.get("outcomes") or {})
        config = normalize_matched_origin(
            outcomes.get("matched_origin"), detector=inputs["detector"],
            outcomes=outcomes, statistics=inputs["statistics"],
        )
        if inputs.get("indicator_gap_rejection"):
            return super().evaluate(plan=plan, inputs=inputs)
        candle_rows = _candle_rows(inputs.get("indicator_evidence", {}).get("candles"), field="indicator_evidence.candles")
        for candle in candle_rows:
            _number(candle.get("close"), field="candle.close", positive=True)
        raw = dict(super().evaluate(plan=plan, inputs=inputs))
        if raw["status"] == "blocked":
            return raw
        followup = super().evaluate(plan=plan, inputs={**inputs, "detector": config["detector"]})
        # Reuse the single admitted graph's public snapshots and existing outcome
        # evaluator. No second engine run, external result ingestion or Fact synthesis.
        attribution = _attribute(
            raw, followup, config=config, outcomes=outcomes, statistics=inputs["statistics"],
            candle_rows=candle_rows,
        )
        required = set(outcomes.get("required_horizons") or outcomes["horizons"])
        paired_reasons = [
            f"paired:{horizon}:{reason}"
            for horizon in sorted(required)
            for reason in attribution["horizons"][str(horizon)]["paired_eligibility"]["reasons"]
        ]
        reasons = (
            [f"origin:{reason}" for reason in raw["eligibility"]["reasons"]]
            + [f"followup:{reason}" for reason in followup["eligibility"]["reasons"]]
            + paired_reasons
        )
        attribution["eligibility"] = {"eligible": not reasons, "reasons": reasons,
                                      "origin": raw["eligibility"], "followup": followup["eligibility"]}
        raw["analysis_status"] = "insufficient_evidence" if reasons else raw["analysis_status"]
        raw["eligibility"] = {**raw["eligibility"], "eligible": not reasons, "reasons": reasons}
        raw["matched_origin_attribution"] = attribution
        raw["followup_result"] = followup
        raw["hashes"] = {**raw["hashes"], "matched_origin_attribution_hash": semantic_hash(attribution), "followup_result_hash": semantic_hash(followup)}
        return raw
