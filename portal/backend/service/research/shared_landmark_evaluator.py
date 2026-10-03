"""Fixed-landmark descriptive groups and profile influence on canonical snapshots."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from statistics import fmean, median
from typing import Any, Mapping

from market_data.frozen import semantic_hash
from research_science.check import ResolvedCheckPlan
from .event_fact_evaluator import EventFactEvaluator, _candle_rows
from .matched_origin_evaluator import (
    MatchedOriginEvaluator, _clock, _entry_state, _number, _reference, _time,
)

_CONFIRMED = "confirmed_by_landmark"
_COMPLEMENT = "no_confirmation_by_landmark_complete_observation"
_STATES = ("all", "outside", "inside", "at_boundary")


def normalize_shared_landmark(config: Mapping[str, Any], *, outcomes: Mapping[str, Any], gap_policy: str) -> dict[str, Any]:
    expected = {"classification_lag_bars", "sample_lag_bars", "readiness_contract", "dependence"}
    if not isinstance(config, Mapping) or set(config) != expected:
        raise ValueError("shared_landmark_invalid: declare classification_lag_bars, sample_lag_bars, readiness_contract and dependence")
    values = {}
    for key in ("classification_lag_bars", "sample_lag_bars"):
        value = config[key]
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
            raise ValueError(f"shared_landmark_invalid: {key} must be an integer from 0 to 100")
        values[key] = value
    if values["sample_lag_bars"] <= values["classification_lag_bars"]:
        raise ValueError("shared_landmark_invalid: sample must follow classification candle")
    shift = values["sample_lag_bars"] - int(outcomes["entry_lag_bars"])
    if shift <= 0 or any(int(h) <= shift for h in outcomes["horizons"]):
        raise ValueError("shared_landmark_invalid: later sample must precede every original endpoint")
    if config["readiness_contract"] != "market_profile.value_location.v1":
        raise ValueError("shared_landmark_invalid: unsupported public readiness contract")
    if config["dependence"] != "leave_one_original_profile_out.v1":
        raise ValueError("shared_landmark_invalid: unsupported dependence sensitivity")
    if gap_policy != "reject":
        raise ValueError("shared_landmark_invalid: complete classification currently requires gap_policy=reject")
    return {**values, "readiness_contract": config["readiness_contract"], "dependence": config["dependence"]}


def _distribution(values: list[float]) -> dict[str, Any]:
    return {"count": len(values), "mean": fmean(values) if values else None,
            "median": median(values) if values else None,
            "minimum": min(values) if values else None, "maximum": max(values) if values else None,
            "sum": sum(values), "positive_count": sum(value > 0 for value in values)}


def _values(rows: list[dict[str, Any]], horizon: str) -> list[float]:
    return [row["outcomes"][horizon]["signed_return"] for row in rows
            if row["outcomes"][horizon]["status"] == "resolved"]


def _contrast(rows: list[dict[str, Any]], horizon: str, state: str) -> dict[str, Any]:
    selected = [row for row in rows if state == "all" or row["entry_state"] == state]
    groups = {label: _distribution(_values([r for r in selected if r["classification"] == label], horizon))
              for label in (_CONFIRMED, _COMPLEMENT)}
    left, right = (groups[label]["mean"] for label in (_CONFIRMED, _COMPLEMENT))
    return {"groups": groups, "confirmed_minus_complement": None if left is None or right is None else left - right,
            "undefined_reason": "empty_comparison_group" if left is None or right is None else None}


def _overlap(rows: list[dict[str, Any]], horizon: str) -> dict[str, Any]:
    active = []
    pairs = within = 0
    for row in sorted(rows, key=lambda row: (row.get("sample_known_at") or "", row["origin_index"])):
        outcome = row["outcomes"][horizon]
        if row["classification"] not in (_CONFIRMED, _COMPLEMENT) or outcome["status"] != "resolved":
            continue
        start, end = _time(row["sample_known_at"]), _time(outcome["endpoint_close"])
        active = [(stop, cluster) for stop, cluster in active if stop > start]
        pairs += len(active)
        within += sum(cluster == row["profile_cluster"] for _, cluster in active)
        active.append((end, row["profile_cluster"]))
    return {"overlapping_pair_count": pairs, "within_profile_pair_count": within,
            "cross_profile_pair_count": pairs - within, "rule": "positive_duration_intersection_of_sample_to_endpoint.v1",
            "interpretation": "dependence_diagnostic_not_effective_sample_size"}


def _summaries(rows: list[dict[str, Any]], horizons: list[int], criteria: Mapping[str, Any]) -> dict[str, Any]:
    summaries = {}
    for h in map(str, horizons):
        clusters = sorted({row["profile_cluster"] for row in rows if row["classification"] in (_CONFIRMED, _COMPLEMENT) and row["outcomes"][h]["status"] == "resolved"})
        contrasts = {state: _contrast(rows, h, state) for state in _STATES}
        groups = []
        for label in sorted({row["classification"] for row in rows}):
            for state in ("all", "outside", "inside", "at_boundary", "unavailable"):
                subset = [r for r in rows if r["classification"] == label and (state == "all" or r["entry_state"] == state)]
                if not subset and state != "all":
                    continue
                values = _values(subset, h)
                days = len({r["origin_utc_day"] for r in subset if r["outcomes"][h]["status"] == "resolved"})
                reasons = []
                if len(values) < criteria.get("min_samples", 0): reasons.append("minimum_group_samples_not_met")
                if days < criteria.get("min_distinct_utc_days", 0): reasons.append("minimum_group_days_not_met")
                if not values: reasons.append("no_resolved_group_outcomes")
                groups.append({"classification": label, "entry_state": state, "event_count": len(subset),
                    "resolved": _distribution(values), "distinct_origin_utc_days": days,
                    "unresolved_reasons": dict(sorted(Counter(r["outcomes"][h]["reason"] for r in subset if r["outcomes"][h]["status"] != "resolved").items())),
                    "eligibility": {"eligible": not reasons, "reasons": reasons, "criteria": dict(criteria)}})
        for state, comparison in contrasts.items():
            reasons = []
            for label in (_CONFIRMED, _COMPLEMENT):
                group = next((g for g in groups if g["classification"] == label and g["entry_state"] == state), None)
                reasons.extend([f"{label}:{reason}" for reason in (group["eligibility"]["reasons"] if group else ["empty_comparison_group"])])
            comparison["eligibility"] = {"eligible": not reasons, "reasons": reasons, "criteria": dict(criteria)}
        contributions = []
        keys = sorted({(r["profile_cluster"], r["origin_utc_day"]) for r in rows})
        for cluster, day in keys:
            subset = [r for r in rows if r["profile_cluster"] == cluster and r["origin_utc_day"] == day]
            entry = {"profile_cluster": cluster, "profile_key": subset[0]["profile_key"], "origin_utc_day": day,
                     "event_count": len(subset), "classifications": dict(sorted(Counter(r["classification"] for r in subset).items())), "groups": {}}
            for label in (_CONFIRMED, _COMPLEMENT):
                values = _values([r for r in subset if r["classification"] == label], h)
                total = contrasts["all"]["groups"][label]["count"]
                entry["groups"][label] = {**_distribution(values), "contribution_to_full_group_mean": sum(values) / total if total else None}
            contributions.append(entry)
        deletions = []
        for cluster in clusters:
            remaining = [r for r in rows if r["profile_cluster"] != cluster]
            removed = [r for r in rows if r["profile_cluster"] == cluster]
            values = {state: _contrast(remaining, h, state) for state in _STATES}
            for state, comparison in values.items():
                full = contrasts[state]["confirmed_minus_complement"]
                reduced = comparison["confirmed_minus_complement"]
                comparison["change_from_full"] = None if full is None or reduced is None else reduced - full
            deletions.append({"removed_profile_cluster": cluster, "profile_key": removed[0]["profile_key"],
                "removed_event_count": len(removed), "removed_classifications": dict(sorted(Counter(r["classification"] for r in removed).items())), "comparisons": values})
        defined = [x["comparisons"]["all"]["confirmed_minus_complement"] for x in deletions if x["comparisons"]["all"]["confirmed_minus_complement"] is not None]
        eligible = [r for r in rows if r["classification"] in (_CONFIRMED, _COMPLEMENT) and r["outcomes"][h]["status"] == "resolved"]
        center = median(_values(eligible, h)) if eligible else None
        episodes = lambda selected: [{"origin_index": r["origin_index"], "origin_time": r["origin_time"], "profile_cluster": r["profile_cluster"], "classification": r["classification"], "entry_state": r["entry_state"], "signed_return": r["outcomes"][h]["signed_return"]} for r in selected]
        summaries[h] = {"groups": groups, "comparisons": contrasts, "profile_day_contributions": contributions,
            "overlap": _overlap(rows, h), "leave_one_profile_out": {"method": "leave_one_original_profile_out.v1", "cluster_count": len(clusters),
                "deletions": deletions, "defined_deletion_count": len(defined), "contrast_minimum": min(defined) if defined else None,
                "contrast_maximum": max(defined) if defined else None, "inference": "influence_sensitivity_not_confidence_interval_or_independence_correction"},
            "episodes": {"first_chronological": episodes(sorted(eligible, key=lambda r: (r["origin_time"], r["origin_index"]))[:3]),
                "largest_absolute_outcomes": episodes(sorted(eligible, key=lambda r: (-abs(r["outcomes"][h]["signed_return"]), r["origin_time"]))[:3]),
                "most_adverse": episodes(sorted(eligible, key=lambda r: (r["outcomes"][h]["signed_return"], r["origin_time"]))[:3]),
                "nearest_median": episodes(sorted(eligible, key=lambda r: (abs(r["outcomes"][h]["signed_return"] - center), r["origin_time"]))[:3])}}
    return summaries


@dataclass(frozen=True)
class SharedLandmarkEvaluator(MatchedOriginEvaluator):
    version: str = "7"
    result_schema_version: str = "event_fact_analysis_result.v7"

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        outcomes = dict(inputs["outcomes"])
        config = normalize_shared_landmark(outcomes.get("shared_landmark"), outcomes=outcomes, gap_policy=plan.gap_policy)
        result = dict(super().evaluate(plan=plan, inputs=inputs))
        if result["status"] == "blocked":
            return result
        shift = config["sample_lag_bars"] - outcomes["entry_lag_bars"]
        delayed_outcomes = {**outcomes, "entry_lag_bars": config["sample_lag_bars"],
            "horizons": [h - shift for h in outcomes["horizons"]], "primary_horizon": outcomes["primary_horizon"] - shift,
            "required_horizons": [h - shift for h in outcomes.get("required_horizons", [])]}
        delayed_outcomes.pop("matched_origin", None)
        delayed_outcomes.pop("shared_landmark", None)
        delayed = EventFactEvaluator.evaluate(self, plan=plan, inputs={**inputs, "outcomes": delayed_outcomes})
        if len(result["events"]) != len(delayed["events"]):
            raise ValueError("shared_landmark_invalid: origin membership changed")
        candles = {_time(c["open_time"]): c for c in _candle_rows(inputs["indicator_evidence"]["candles"], field="indicator_evidence.candles")}
        contexts = {}
        for output in inputs["indicator_evidence"]["outputs"]:
            if output.get("output_name") == "value_location" and output.get("output_type") == "context":
                key = (output.get("indicator_id"), _time(output["time"]))
                if key in contexts: raise ValueError("shared_landmark_invalid: duplicate public context")
                contexts[key] = output
        step = timedelta(seconds=int(plan.warmup["timeframe_seconds"]))
        evaluation_end = _time(plan.evaluation_range["end_exclusive"])
        records = []
        matching = outcomes["matched_origin"]
        unmatched = result["matched_origin_attribution"]["unmatched_followups"]
        for index, (origin, sample, pair) in enumerate(zip(result["events"], delayed["events"], result["matched_origin_attribution"]["origins"])):
            if (origin["event_time"], origin["event_key"]) != (sample["event_time"], sample["event_key"]):
                raise ValueError("shared_landmark_invalid: origin ordering changed")
            ref = _reference(origin, matching)
            if ref["source"] != "market_profile": raise ValueError("shared_landmark_invalid: readiness contract requires market_profile reference")
            opened = _time(origin["event_time"])
            cutoff_open = opened + step * config["classification_lag_bars"]
            cutoff_candle = candles.get(cutoff_open)
            cutoff = _time(cutoff_candle["close_time"]) if cutoff_candle else cutoff_open + step
            context = contexts.get((origin["indicator_id"], cutoff_open))
            classification, reason = _COMPLEMENT, None
            current_profile = None
            if cutoff >= evaluation_end:
                classification, reason = "incomplete_classification_window", "landmark_at_or_beyond_signal_observation_end"
            elif any(opened + step * n not in candles for n in range(config["classification_lag_bars"] + 1)):
                classification, reason = "incomplete_classification_window", "classification_candle_missing"
            elif any(_time(candles[opened + step * n].get("known_at") or candles[opened + step * n]["close_time"]) > cutoff for n in range(config["classification_lag_bars"] + 1)):
                classification, reason = "incomplete_classification_window", "classification_candle_not_known_by_landmark"
            elif _time(origin["decision_time"]) > cutoff:
                classification, reason = "incomplete_classification_window", "origin_not_known_by_landmark"
            elif any(not isinstance(contexts.get((origin["indicator_id"], opened + step * n), {}).get("value", {}).get("fields", {}).get("active_profile_key"), str)
                     for n in range(config["classification_lag_bars"] + 1)):
                classification, reason = "incomplete_classification_window", "public_readiness_context_unavailable"
            else:
                current_profile = context["value"]["fields"]["active_profile_key"]
                if any(contexts[(origin["indicator_id"], opened + step * n)]["value"]["fields"]["active_profile_key"] != ref["key"] for n in range(config["classification_lag_bars"] + 1)):
                    classification, reason = "unresolved_identity_or_context", "original_profile_changed"
                elif any(row.get("origin_index") == index and _time(result["followup_result"]["events"][row["followup_index"]]["decision_time"]) <= cutoff for row in unmatched):
                    classification, reason = "unresolved_identity_or_context", "followup_identity_or_reference_disagrees"
                elif pair["followup_index"] is not None:
                    followup = result["followup_result"]["events"][pair["followup_index"]]
                    if _time(followup["decision_time"]) <= cutoff:
                        classification = _CONFIRMED
                    elif _time(followup["event_time"]) <= cutoff_open:
                        classification, reason = "incomplete_classification_window", "followup_not_known_by_landmark"
            entry_state = _entry_state(sample, ref)
            clock = _clock(sample, candles) if sample.get("entry_time") is not None else None
            if clock and _time(clock["entry_sample_close"]) <= cutoff:
                raise ValueError("shared_landmark_invalid: sample does not follow cutoff")
            record = {"origin_index": index, "origin_time": origin["event_time"], "origin_utc_day": _time(origin["decision_time"]).date().isoformat(),
                "indicator_id": origin["indicator_id"], "direction": origin["direction"], "profile_key": ref["key"],
                "profile_cluster": semantic_hash({"indicator_id": origin["indicator_id"], "original_profile_key": ref["key"]}),
                "original_reference": dict(ref), "classification": classification, "classification_reason": reason,
                "classification_cutoff": cutoff.isoformat(), "observed_profile_at_landmark": current_profile,
                "entry_state": entry_state, "entry_price": sample.get("entry_price"), "sample_clock": clock,
                "sample_known_at": sample.get("entry_known_at"), "followup_index": pair["followup_index"],
                "population_eligible": sample["population_eligible"], "outcomes": {}}
            for horizon in outcomes["horizons"]:
                h, shifted = str(horizon), str(horizon - shift)
                value, prior = sample["outcomes"][shifted], origin["outcomes"][h]
                if value["status"] == "resolved" and prior["status"] == "resolved" and value["target_time"] != prior["target_time"]:
                    raise ValueError("shared_landmark_invalid: original endpoint changed")
                why = value.get("reason") if value["status"] != "resolved" else None
                if not sample["population_eligible"]: why = "sample_population_ineligible"
                if classification not in (_CONFIRMED, _COMPLEMENT): why = reason
                record["outcomes"][h] = {"status": "unresolved", "reason": why} if why else {
                    "status": "resolved", "reason": None, "signed_return": _number(value["direction_signed_forward_return"], field="shared_return"),
                    "endpoint_bar_open": value["target_time"], "endpoint_close": _time(candles[_time(value["target_time"])]["close_time"]).isoformat(),
                    "remaining_bars_after_sample": horizon - shift}
            records.append(record)
        analysis = {"schema_version": "shared_landmark_comparison.v1", "classification_rule": "fixed_cutoff_complete_public_context.v1",
            "readiness_contract": config["readiness_contract"], "negative_label_meaning": "no_confirmation_by_fixed_landmark_not_failed_or_never_confirmed",
            "context_scope": "same_evaluation_window_with_explicit_right_censoring",
            "entry_state_meaning": "direction_relative_to_original_boundary_not_full_value_area_membership",
            "inference": "descriptive_observed_groups_not_causal_or_executable", "origin_count": len(records),
            "classification_counts": dict(sorted(Counter(r["classification"] for r in records).items())),
            "classification_reasons": dict(sorted(Counter(r["classification_reason"] for r in records if r["classification_reason"]).items())),
            "origins": records, "horizons": _summaries(records, outcomes["horizons"], inputs["statistics"].get("eligibility", {}))}
        reasons = [f"shared:{h}:{reason}" for h in outcomes.get("required_horizons", outcomes["horizons"])
                   for reason in analysis["horizons"][str(h)]["comparisons"]["all"]["eligibility"]["reasons"]]
        analysis["eligibility"] = {"eligible": not reasons, "reasons": reasons}
        analysis["analysis_status"] = "insufficient_evidence" if reasons else "completed"
        if reasons:
            result["analysis_status"] = "insufficient_evidence"
            result["eligibility"] = {**result["eligibility"], "eligible": False,
                                     "reasons": result["eligibility"]["reasons"] + reasons}
        result["shared_landmark_comparison"] = analysis
        result["hashes"] = {**result["hashes"], "shared_landmark_comparison_hash": semantic_hash(analysis)}
        return result
