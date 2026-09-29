"""Check-owned original-POC outcomes for public first-return events at a fixed clock."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Mapping

from market_data.frozen import semantic_hash
from research_science.check import ResolvedCheckPlan
from .event_fact_evaluator import EventFactEvaluator, _candle_rows
from .matched_origin_evaluator import _clock, _number, _time
from .shared_landmark_evaluator import _distribution, normalize_shared_landmark

RETURNED = "returned_by_landmark"
OUTSIDE = "still_outside_at_landmark"
GROUPS = (RETURNED, OUTSIDE)


def normalize_first_return(config: Mapping[str, Any], *, outcomes: Mapping[str, Any],
                           detector: Mapping[str, Any], statistics: Mapping[str, Any], gap_policy: str) -> dict[str, Any]:
    if not isinstance(config, Mapping) or config.get("readiness_contract") != "market_profile.first_return_state.v2":
        raise ValueError("first_return_invalid: explicit market_profile.first_return_state.v2 contract required")
    normalized = normalize_shared_landmark({**config, "readiness_contract": "market_profile.value_location.v1"},
                                           outcomes=outcomes, gap_policy=gap_policy)
    if outcomes.get("invalidation"):
        raise ValueError("first_return_invalid: invalidation outcomes are unsupported")
    if detector.get("type") != "indicator_event" or detector.get("output_name") != "balance_breakout" or outcomes.get("horizon_kind") != "bars":
        raise ValueError("first_return_invalid: raw Market Profile breakouts and bar horizons required")
    if any(row["key"] != "balance_breakout_" + row["direction"] for row in detector["event_keys"]):
        raise ValueError("first_return_invalid: canonical breakout keys required")
    if any((statistics.get("features") or {}).values()) or any(statistics.get(k) for k in
            ("folds", "model", "bootstrap", "direct_tests", "feature_bins", "purge_bars", "embargo_bars")):
        raise ValueError("first_return_invalid: descriptive groups only; inference/features unsupported")
    if any(statistics.get("eligibility", {}).get(k) for k in
            ("min_class_count", "min_validation_samples_per_fold", "min_valid_folds")):
        raise ValueError("first_return_invalid: only sample and day eligibility supported")
    return {**normalized, "readiness_contract": config["readiness_contract"]}


def _key(indicator: str, origin: Mapping[str, Any]) -> tuple[Any, ...]:
    return indicator, origin["breakout_event_key"], _time(origin["breakout_time"])


def _summary(rows: list[dict[str, Any]], horizon: str, criteria: Mapping[str, Any]) -> dict[str, Any]:
    def comparison(subset):
        values = {label: [r["outcomes"][horizon]["distance_reduction_atr"] for r in subset
                         if r["classification"] == label and r["outcomes"][horizon]["status"] == "resolved"] for label in GROUPS}
        distributions = {k: _distribution(v) for k, v in values.items()}
        left, right = (distributions[k]["mean"] for k in GROUPS)
        return {"groups": distributions, "returned_minus_outside": None if left is None or right is None else left-right}
    full = comparison(rows)
    groups = []
    reasons = []
    for label in sorted(set(GROUPS) | {r["classification"] for r in rows}):
        subset = [r for r in rows if r["classification"] == label]
        resolved = [r for r in subset if r["outcomes"][horizon]["status"] == "resolved"]
        days = len({r["origin_utc_day"] for r in resolved})
        why = []
        if not resolved: why.append("no_resolved_group_outcomes")
        if len(resolved) < criteria.get("min_samples", 0): why.append("minimum_group_samples_not_met")
        if days < criteria.get("min_distinct_utc_days", 0): why.append("minimum_group_days_not_met")
        if label in GROUPS: reasons.extend(f"{label}:{reason}" for reason in why)
        groups.append({"classification": label, "event_count": len(subset), "resolved_count": len(resolved),
            "distinct_origin_utc_days": days,
            "distance_reduction_atr": _distribution([r["outcomes"][horizon]["distance_reduction_atr"] for r in resolved]),
            "initial_distance_atr": _distribution([r["initial_distance_atr"] for r in resolved]),
            "entry_states": dict(Counter(r["entry_state"] for r in subset)),
            "profile_changed_by_landmark_count": sum(bool(r["profile_changed_by_landmark"]) for r in subset),
            "center_states": dict(Counter(r["center_entry_state"] for r in subset)),
            "center_crossing": dict(Counter(r["outcomes"][horizon]["center_crossing_status"] for r in resolved)),
            "unresolved_reasons": dict(Counter(r["outcomes"][horizon]["reason"] for r in subset if r["outcomes"][horizon]["status"] != "resolved")),
            "eligibility": {"eligible": not why, "reasons": why, "criteria": dict(criteria)}})
    eligible = [r for r in rows if r["classification"] in GROUPS and r["outcomes"][horizon]["status"] == "resolved"]
    clusters = sorted({r["profile_cluster"] for r in eligible})
    deletions = [{"removed_profile_cluster": key, "comparison": comparison([r for r in rows if r["profile_cluster"] != key])} for key in clusters]
    contributions = [{"profile_cluster": cluster, "origin_utc_day": day,
                      "comparison": comparison([r for r in rows if (r["profile_cluster"], r["origin_utc_day"]) == (cluster, day)])}
                     for cluster, day in sorted({(r["profile_cluster"], r["origin_utc_day"]) for r in rows})]
    active = []; pairs = within = 0
    for row in sorted(eligible, key=lambda r: (r["sample_known_at"], r["origin_index"])):
        start, end = _time(row["sample_known_at"]), _time(row["outcomes"][horizon]["endpoint_close"])
        active = [(stop, cluster) for stop, cluster in active if stop > start]
        pairs += len(active); within += sum(cluster == row["profile_cluster"] for _, cluster in active)
        active.append((end, row["profile_cluster"]))
    return {"groups": groups, "comparison": full, "profile_day_contributions": contributions,
            "leave_one_profile_out": {"method": "leave_one_original_profile_out.v1", "cluster_count": len(clusters),
                "deletions": deletions, "inference": "influence_sensitivity_not_independence_correction"},
            "overlap": {"overlapping_pair_count": pairs, "within_profile_pair_count": within,
                "cross_profile_pair_count": pairs-within, "interpretation": "not_effective_sample_size"},
            "eligibility": {"eligible": not reasons, "reasons": reasons}}


@dataclass(frozen=True)
class FirstReturnEvaluator(EventFactEvaluator):
    version: str = "8"
    result_schema_version: str = "event_fact_analysis_result.v8"
    descriptive_outcomes_enabled: bool = True

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        outcomes = dict(inputs["outcomes"])
        config = normalize_first_return(outcomes.get("first_return"), outcomes=outcomes,
            detector=inputs["detector"], statistics=inputs["statistics"], gap_policy=plan.gap_policy)
        result = dict(super().evaluate(plan=plan, inputs=inputs))
        if result["status"] == "blocked": return result
        shift = config["sample_lag_bars"] - outcomes["entry_lag_bars"]
        shifted = {**outcomes, "entry_lag_bars": config["sample_lag_bars"],
            "horizons": [h-shift for h in outcomes["horizons"]], "primary_horizon": outcomes["primary_horizon"]-shift,
            "required_horizons": [h-shift for h in outcomes.get("required_horizons", [])]}
        sampled = super().evaluate(plan=plan, inputs={**inputs, "outcomes": shifted})
        candles = {_time(c["open_time"]): c for c in _candle_rows(inputs["indicator_evidence"]["candles"], field="indicator_evidence.candles")}
        contexts, returns = {}, {}
        for output in inputs["indicator_evidence"]["outputs"]:
            if output.get("output_name") == "first_return_state" and output.get("output_type") == "context":
                key = output["indicator_id"], _time(output["time"])
                if key in contexts: raise ValueError("first_return_invalid: duplicate public context")
                contexts[key] = output.get("value", {}).get("fields", {})
            if output.get("output_name") == "first_value_return" and output.get("output_type") == "signal":
                event = output["event"]; key = _key(output["indicator_id"], event["metadata"])
                if key in returns: raise ValueError("first_return_invalid: duplicate first return")
                returns[key] = {"time": output["time"], "event": event}
        if len(result["events"]) != len(sampled["events"]):
            raise ValueError("first_return_invalid: changed origin population")
        step = timedelta(seconds=int(plan.warmup["timeframe_seconds"]))
        records = []
        for index, (origin, sample) in enumerate(zip(result["events"], sampled["events"])):
            if (origin["event_time"], origin["event_key"]) != (sample["event_time"], sample["event_key"]):
                raise ValueError("first_return_invalid: changed origin ordering")
            indicator, opened = origin["indicator_id"], _time(origin["event_time"])
            identity = indicator, origin["event_key"], opened
            ref = origin["event"]["metadata"]["reference"]
            if ref.get("source") != "market_profile": raise ValueError("first_return_invalid: original Market Profile reference required")
            levels = ref["context"]["active_value_area"]
            val, poc, vah = (_number(levels[k], field=k, positive=True) for k in ("val", "poc", "vah"))
            cutoff_open = opened + step*config["classification_lag_bars"]
            cutoff = cutoff_open + step
            state = contexts.get((indicator, cutoff_open), {})
            matches = [o for o in state.get("origins", []) if _key(indicator, o) == identity]
            if len(matches) > 1: raise ValueError("first_return_invalid: ambiguous origin context")
            observed = matches[0] if matches else None
            reason = None; label = OUTSIDE
            if cutoff >= _time(plan.evaluation_range["end_exclusive"]): reason = "classification_right_censored"
            elif not val < vah or not val <= poc <= vah: reason = "original_range_invalid"
            elif _time(origin["decision_time"]) > cutoff: reason = "origin_not_known_by_landmark"
            elif any(opened+step*n not in candles for n in range(config["classification_lag_bars"]+1)): reason = "classification_candle_missing"
            elif any(_time(candles[opened+step*n].get("known_at") or candles[opened+step*n]["close_time"]) > cutoff for n in range(config["classification_lag_bars"]+1)): reason = "classification_candle_late"
            elif observed is None: reason = "public_origin_state_unavailable"
            elif config["classification_lag_bars"] > state.get("lifetime_bars", 0): reason = "classification_exceeds_origin_lifetime"
            elif semantic_hash(observed["reference"]) != semantic_hash(ref): reason = "original_reference_disagrees"
            elif _time(observed["observed_known_at"]) > cutoff: reason = "public_origin_state_late"
            elif observed["status"] == "censored": reason = observed.get("reason") or "origin_censored"
            elif observed["status"] == "returned":
                follow = returns.get(identity)
                if follow is None: reason = "public_return_signal_unavailable"
                elif follow["event"].get("direction") != origin["direction"] or follow["event"].get("key") != "first_value_return_"+origin["direction"]: reason = "return_direction_disagrees"
                elif not opened < _time(follow["time"]) <= cutoff_open: reason = "return_outside_classification_window"
                elif semantic_hash(follow["event"]["metadata"]["reference"]) != semantic_hash(ref): reason = "return_reference_disagrees"
                elif _time(follow["event"]["known_at"]) > cutoff: reason = "return_not_known_by_landmark"
                elif _time(follow["time"]) != _time(observed["first_return_time"]): reason = "return_clock_disagrees"
                else: label = RETURNED
            elif observed["status"] != "no_return_observed": reason = "unknown_origin_status"
            elif observed["location"] == "at_boundary": label = "at_boundary_at_landmark"
            elif observed["location"] not in ("above", "below"): reason = "outside_state_unavailable"
            if reason: label = "unresolved"
            sample_open = opened + step*config["sample_lag_bars"]
            sample_context = contexts.get((indicator, sample_open), {})
            atr = sample_context.get("prior_atr") if sample_context.get("prior_atr_ready") else None
            if atr is not None: atr = _number(atr, field="strictly_prior_atr", positive=True)
            atr_known = sample_context.get("prior_atr_known_at")
            if atr_known is None or _time(atr_known) > sample_open:
                atr = None
            price = sample.get("entry_price")
            entry = "unavailable" if price is None else "inside" if val < price < vah else "at_boundary" if price in (val,vah) else "above" if price > vah else "below"
            side = 1 if origin["direction"] == "long" else -1
            center = "unavailable" if price is None else "already_at_or_beyond_center" if side*(price-poc) <= 0 else "before_center"
            clock = _clock(sample, candles) if sample.get("entry_time") else None
            record = {"origin_index": index, "origin_time": origin["event_time"], "origin_utc_day": _time(origin["decision_time"]).date().isoformat(),
                "indicator_id": indicator, "direction": origin["direction"], "profile_key": ref["key"],
                "profile_cluster": semantic_hash({"indicator_id": indicator, "original_profile_key": ref["key"]}),
                "original_reference": ref, "classification": label, "classification_reason": reason,
                "classification_cutoff": cutoff.isoformat(),
                "profile_at_landmark": state.get("active_profile_key"), "profile_at_sample": sample_context.get("active_profile_key"),
                "profile_changed_by_landmark": observed.get("profile_changed_since_origin") if observed else None,
                "entry_state": entry, "center_entry_state": center,
                "sample_clock": clock, "sample_known_at": sample.get("entry_known_at"), "entry_price": price,
                "prior_atr": atr, "prior_atr_known_at": atr_known, "initial_distance_atr": abs(price-poc)/atr if price is not None and atr else None, "outcomes": {}}
            for horizon in outcomes["horizons"]:
                outcome = sample["outcomes"][str(horizon-shift)]
                why = reason or ("classification_not_comparable" if label not in GROUPS else None)
                if not sample["population_eligible"]: why = why or "sample_population_ineligible"
                if atr is None: why = why or "strictly_prior_atr_unready"
                if outcome["status"] != "resolved": why = why or outcome.get("reason") or "outcome_unresolved"
                if why:
                    record["outcomes"][str(horizon)] = {"status": "unresolved", "reason": why}; continue
                endpoint = _time(outcome["target_time"])
                original = origin["outcomes"][str(horizon)]
                if original["status"] == "resolved" and _time(original["target_time"]) != endpoint:
                    raise ValueError("first_return_invalid: original endpoint changed")
                path = [candles.get(sample_open+step*n) for n in range(1, horizon-shift+1)]
                if any(c is None or _time(c.get("known_at") or c["close_time"]) > _time(c["close_time"]) for c in path):
                    record["outcomes"][str(horizon)] = {"status": "unresolved", "reason": "outcome_path_missing_or_late"}; continue
                crossing = next((c for c in path if side*(_number(c["close"], field="close")-poc) <= 0), None)
                record["outcomes"][str(horizon)] = {"status": "resolved", "reason": None,
                    "distance_reduction_atr": (abs(price-poc)-abs(_number(candles[endpoint]["close"], field="endpoint.close")-poc))/atr,
                    "endpoint_bar_open": endpoint.isoformat(), "endpoint_close": _time(candles[endpoint]["close_time"]).isoformat(),
                    "center_crossing_status": "already_at_or_beyond_center" if center != "before_center" else "crossed" if crossing else "not_crossed",
                    "first_center_crossing_close": _time(crossing["close_time"]).isoformat() if crossing and center == "before_center" else None,
                    "remaining_bars_after_sample": horizon-shift}
            records.append(record)
        summaries = {str(h): _summary(records, str(h), inputs["statistics"].get("eligibility", {})) for h in outcomes["horizons"]}
        reasons = [f"first_return:{h}:{reason}" for h in outcomes.get("required_horizons", outcomes["horizons"])
                   for reason in summaries[str(h)]["eligibility"]["reasons"]]
        analysis = {"schema_version": "first_return_comparison.v1", "readiness_contract": config["readiness_contract"],
            "measurement": "reduction_in_absolute_distance_to_original_poc_divided_by_strictly_prior_sample_atr",
            "inference": "descriptive_not_causal_or_executable; initial_distance_and_regime_remain_confounders",
            "classification_counts": dict(Counter(r["classification"] for r in records)), "origins": records, "horizons": summaries,
            "eligibility": {"eligible": not reasons, "reasons": reasons}, "analysis_status": "insufficient_evidence" if reasons else "completed"}
        result["first_return_comparison"] = analysis
        if reasons:
            result["analysis_status"] = "insufficient_evidence"
            result["eligibility"] = {**result["eligibility"], "eligible": False, "reasons": result["eligibility"]["reasons"]+reasons}
        result["hashes"] = {**result["hashes"], "first_return_comparison_hash": semantic_hash(analysis)}
        return result
