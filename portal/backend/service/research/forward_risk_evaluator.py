"""Versioned, descriptive candle-risk comparison over canonical snapshot evidence."""
from __future__ import annotations

from core.execution_control import execution_checkpoint

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import heapq
import json
from typing import Any, Mapping

import numpy as np
import pandas as pd

from indicators.candle_stats.definition import CandleStatsIndicator
from market_data.frozen import semantic_hash
from research_science.check import ResolvedCheckPlan
from .event_fact_evaluator import EventFactEvaluator, _candle_rows

GROUPS = ("ordinary", "shock_crossing", "persistent_high")
RMS_BINS_BPS = (0, 1, 2, 4, 8, 16, 32)
MAX_MINUTES = 370 * 24 * 60
PINNED_PARAMS = {"atr_short_window": 14, "atr_long_window": 50, "atr_z_window": 100,
    "directional_efficiency_window": 20, "slope_window": 20, "range_window": 20,
    "expansion_window": 20, "volume_window": 50, "overlap_window": 8,
    "slope_stability_lookback": 150, "warmup_bars": 200, "atr_expansion_signal_threshold": 2.0}


def _seconds(value: Any) -> int:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None or stamp.value % 1000000000:
        raise ValueError(f"forward_risk_invalid: UTC whole-second clock required value={value!r}")
    return int(stamp.timestamp())


def _iso(seconds: int) -> str:
    return datetime.fromtimestamp(int(seconds), timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_forward_risk(config: Any, *, detector: Mapping[str, Any],
                           outcomes: Mapping[str, Any], statistics: Mapping[str, Any],
                           gap_policy: str) -> dict[str, Any]:
    expected = {"schema_version": "candle_risk_comparison.v1",
                "baseline_bars": 120, "readiness_contract": "candle_stats.public_outputs.v1",
                "outcome_boundary": "evaluation_end_exclusive"}
    if not isinstance(config, Mapping) or dict(config) != expected:
        raise ValueError("forward_risk_invalid: explicit fixed candle_risk_comparison.v1 configuration required")
    if (detector.get("type") != "indicator_event" or detector.get("output_name") != "atr_expansion"
            or [(r.get("key"), r.get("direction")) for r in detector.get("event_keys", [])]
            != [("atr_expansion_long", "long")]):
        raise ValueError("forward_risk_invalid: canonical Candle Stats ATR crossing required")
    if (outcomes.get("horizon_kind") != "elapsed_time"
            or list(outcomes.get("horizons") or []) != [1800, 7200, 21600]
            or outcomes.get("primary_horizon") != 7200
            or outcomes.get("entry_lag_bars") != 0 or outcomes.get("invalidation")):
        raise ValueError("forward_risk_invalid: fixed 30/120/360 elapsed-minute risk horizons required")
    if gap_policy != "reset_rewarm":
        raise ValueError("forward_risk_invalid: explicit reset_rewarm policy required")
    if any((statistics.get("features") or {}).values()) or any(statistics.get(k) for k in
            ("folds", "model", "bootstrap", "direct_tests", "feature_bins", "purge_bars", "embargo_bars")):
        raise ValueError("forward_risk_invalid: descriptive risk comparison cannot consume inference configuration")
    if any((statistics.get("eligibility") or {}).values()):
        raise ValueError("forward_risk_invalid: no event-count promotion gate is registered")
    return expected


def _distribution(values: np.ndarray) -> dict[str, Any]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    return {"count": int(finite.size), "mean": float(np.mean(finite)) if finite.size else None,
            "median": float(np.median(finite)) if finite.size else None,
            "minimum": float(np.min(finite)) if finite.size else None,
            "maximum": float(np.max(finite)) if finite.size else None,
            "p10": float(np.quantile(finite, .1)) if finite.size else None,
            "p90": float(np.quantile(finite, .9)) if finite.size else None}


def _intervals(times: np.ndarray, labels: np.ndarray) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for stamp, reason in zip(times, labels):
        if not reason:
            continue
        if result and result[-1]["reason"] == reason and result[-1]["end_exclusive"] == _iso(stamp):
            result[-1]["end_exclusive"] = _iso(stamp + 60)
            result[-1]["minutes"] += 1
        else:
            result.append({"start": _iso(stamp), "end_exclusive": _iso(stamp + 60),
                           "reason": str(reason), "minutes": 1})
    return result


def _contrasts(counts: np.ndarray, sums: np.ndarray) -> dict[str, Any]:
    matched = (counts[0] > 0) & (counts[1] > 0)
    shock_count = int(counts[1].sum())
    matched_count = int(counts[1, matched].sum())
    value = None
    if matched_count:
        contrasts = sums[1, matched] / counts[1, matched] - sums[0, matched] / counts[0, matched]
        value = float(np.sum(contrasts * counts[1, matched]) / matched_count)
    return {"shock_weighted_within_stratum_contrast": value,
            "matched_shock_count": matched_count, "unmatched_shock_count": shock_count - matched_count,
            "matched_strata": int(matched.sum()),
            "strata_without_both_groups": int(((counts.sum(axis=0) > 0) & ~matched).sum()),
            "inference": "descriptive_association_not_causal_or_independent"}


def _stratified(values: np.ndarray, cohorts: np.ndarray, strata: np.ndarray,
                days: np.ndarray, weeks: np.ndarray, *, deletions: bool = True) -> dict[str, Any]:
    valid = np.isfinite(values) & (strata >= 0) & np.isin(cohorts, [0, 1])
    count = np.zeros((2, 384), dtype=np.int64)
    total = np.zeros((2, 384), dtype=float)
    np.add.at(count, (cohorts[valid], strata[valid]), 1)
    np.add.at(total, (cohorts[valid], strata[valid]), values[valid])
    result = _contrasts(count, total)
    result["stratification"] = "UTC_month_x_six_hour_block_x_prior_RMS_log_return_bps"
    result["prior_rms_bin_lower_edges_bps"] = list(RMS_BINS_BPS)
    result["strata"] = [{"id": int(i), "ordinary_count": int(count[0, i]),
                          "shock_count": int(count[1, i]), "utc_month": int(i)//32+1,
                          "utc_six_hour_block": (int(i)%32)//8, "prior_rms_bin_lower_bps": RMS_BINS_BPS[int(i)%8],
                          "ordinary_mean": float(total[0, i] / count[0, i]) if count[0, i] else None,
                          "shock_mean": float(total[1, i] / count[1, i]) if count[1, i] else None}
                         for i in np.flatnonzero(count.sum(axis=0))]
    for name, keys in ((("day", days), ("week", weeks)) if deletions else ()):
        unique, inverse = np.unique(keys[valid], return_inverse=True)
        dc = np.zeros((len(unique), 2, 384), dtype=np.int64)
        ds = np.zeros((len(unique), 2, 384), dtype=float)
        np.add.at(dc, (inverse, cohorts[valid], strata[valid]), 1)
        np.add.at(ds, (inverse, cohorts[valid], strata[valid]), values[valid])
        result[f"leave_one_{name}_out"] = [
            {"removed": str(key), **_contrasts(count - dc[i], total - ds[i])}
            for i, key in enumerate(unique)]
    return result


def _overlap(starts: np.ndarray, ends: np.ndarray) -> dict[str, Any]:
    active: list[int] = []
    pairs = episodes = largest = current_count = 0
    right = -1
    for i in np.argsort(starts, kind="stable"):
        start, end = int(starts[i]), int(ends[i])
        while active and active[0] <= start:
            heapq.heappop(active)
        pairs += len(active)
        heapq.heappush(active, end)
        if start >= right:
            episodes += 1
            current_count = 0
        current_count += 1
        largest = max(largest, current_count)
        right = max(right, end)
    return {"positive_duration_overlapping_pairs": pairs, "connected_episodes": episodes,
            "largest_episode_observations": largest, "effective_sample_size": None}


def _take_nonoverlapping(cohorts: np.ndarray, starts: np.ndarray, valid: np.ndarray) -> np.ndarray:
    keep = np.zeros(cohorts.size, dtype=bool)
    for group in range(3):
        last_end = -1
        indices = np.flatnonzero((cohorts == group) & valid)
        for index in indices[np.argsort(starts[indices], kind="stable")]:
            if starts[index] >= last_end:
                keep[index] = True
                last_end = int(starts[index]) + 21600
    return keep


@dataclass(frozen=True)
class ForwardRiskEvaluator(EventFactEvaluator):
    version: str = "9"
    result_schema_version: str = "event_fact_analysis_result.v9"

    def declare_requirements(self, *, definition, request):
        base = dict(super().declare_requirements(definition=definition, request=request))
        if str(request.scope.get("timeframe") or request.scope.get("interval")) != "1m":
            raise ValueError("forward_risk_invalid: one-minute source timeframe required")
        if int(request.scope.get("warmup_bars") or 200) != 200:
            raise ValueError("forward_risk_invalid: fixed 200-bar finite initialization required")
        return {**base, "warmup_floor_bars": 200, "feature_lookback_bars": 121,
                "outcome_boundary": "evaluation_end_exclusive",
                "capture_output_readiness": True, "event_source": "check_candle_snapshot",
                "fact_history_required": False}

    def evaluate(self, *, plan: ResolvedCheckPlan, inputs: Mapping[str, Any]) -> Mapping[str, Any]:
        config = normalize_forward_risk(inputs["outcomes"].get("forward_risk"),
            detector=inputs["detector"], outcomes=inputs["outcomes"],
            statistics=inputs.get("statistics", {}), gap_policy=plan.gap_policy)
        if (int(plan.warmup["timeframe_seconds"]) != 60 or int(plan.warmup["bars"]) != 200
                or plan.materialization_range["end_exclusive"] != plan.evaluation_range["end_exclusive"]):
            raise ValueError("forward_risk_invalid: pinned one-minute/200-bar/no-tail plan required")
        evidence = dict(inputs.get("indicator_evidence") or {})
        indicator = evidence.get("indicator") or {}
        params = indicator.get("params") or {}
        if (indicator.get("type") != "candle_stats"
                or CandleStatsIndicator.resolve_config(params, strict_unknown=True) != PINNED_PARAMS):
            raise ValueError("forward_risk_invalid: pinned Candle Stats defaults required")
        readiness = evidence.get("output_readiness") or {}
        if (readiness.get("schema_version") != "indicator_output_readiness.v1"
                or not indicator.get("id") or readiness.get("indicator_id") != indicator["id"]):
            raise ValueError("forward_risk_invalid: explicit canonical output readiness evidence required")
        begin = _seconds(plan.materialization_range["start"])
        start = _seconds(plan.evaluation_range["start"])
        stop = _seconds(plan.evaluation_range["end_exclusive"])
        if any(value % 60 for value in (begin, start, stop)) or stop <= start or (stop-begin)//60 > MAX_MINUTES:
            raise ValueError("forward_risk_invalid: bounded aligned evaluation window required")
        if (start - begin != 200 * 60
                or datetime.fromtimestamp(start, timezone.utc).year != datetime.fromtimestamp(stop-1, timezone.utc).year):
            raise ValueError("forward_risk_invalid: fixed finite seed and single calendar year required")
        times = np.arange(begin, stop, 60, dtype=np.int64)
        n = times.size
        close = np.full(n, np.nan); high = close.copy(); low = close.copy(); known = close.copy()
        present = np.zeros(n, dtype=bool)
        for candle in _candle_rows(evidence.get("candles") or [], field="indicator_evidence.candles"):
            execution_checkpoint()
            stamp = _seconds(candle["open_time"])
            if stamp < begin or stamp >= stop or (stamp-begin) % 60:
                raise ValueError("forward_risk_invalid: candle outside pinned range or off-grid")
            index = (stamp-begin)//60
            if present[index]:
                raise ValueError("forward_risk_invalid: duplicate candle identity")
            if _seconds(candle["close_time"]) != stamp + 60:
                raise ValueError("forward_risk_invalid: noncanonical one-minute interval")
            present[index] = True
            close[index], high[index], low[index] = (float(candle[k]) for k in ("close", "high", "low"))
            known[index] = _seconds(candle.get("known_at") or candle["close_time"])
        ready = np.zeros(n, dtype=bool); seen = np.zeros(n, dtype=bool); segment = np.full(n, -1, dtype=int)
        for interval in readiness.get("intervals") or []:
            left, right = _seconds(interval["start"]), _seconds(interval["end_exclusive"])
            if left < begin or right > stop or left >= right or left % 60 or right % 60:
                raise ValueError("forward_risk_invalid: readiness interval outside pinned candle range")
            a, b = (left-begin)//60, (right-begin)//60
            if seen[a:b].any():
                raise ValueError("forward_risk_invalid: overlapping readiness intervals")
            seen[a:b] = True
            outputs = interval["ready_outputs"]
            if "candle_stats" not in outputs or "atr_expansion" not in outputs:
                raise ValueError("forward_risk_invalid: both public outputs must have explicit readiness")
            ready[a:b] = outputs["candle_stats"] is True and outputs["atr_expansion"] is True
            segment[a:b] = int(interval["segment"])
        if (seen & ~present).any():
            raise ValueError("forward_risk_invalid: readiness evidence invents a missing candle")
        metric = np.full(n, np.nan); shock = np.zeros(n, dtype=bool); metric_seen = shock.copy()
        for output in evidence.get("outputs") or []:
            execution_checkpoint()
            if output.get("indicator_id") != indicator["id"]:
                raise ValueError("forward_risk_invalid: output Indicator identity mismatch")
            stamp = _seconds(output["time"])
            if stamp < start or stamp >= stop:
                continue
            index = (stamp-begin)//60
            if (stamp-begin) % 60 or not present[index]:
                raise ValueError("forward_risk_invalid: output has no exact source candle")
            if output.get("output_name") == "candle_stats":
                value = output.get("value") or {}; fields = value.get("fields", value)
                if metric_seen[index]:
                    raise ValueError("forward_risk_invalid: duplicate public metric")
                metric_seen[index] = True
                metric[index] = float(fields["atr_zscore"]) if fields.get("atr_zscore") is not None else np.nan
            if output.get("output_name") == "atr_expansion":
                if output.get("event_key") != "atr_expansion_long" or shock[index]:
                    raise ValueError("forward_risk_invalid: unexpected or duplicate shock event")
                if _seconds(output.get("known_at")) != known[index]:
                    raise ValueError("forward_risk_invalid: event known-at disagrees with source")
                shock[index] = True
        valid_close = present & np.isfinite(close) & (close > 0)
        valid_path = valid_close & np.isfinite(high) & np.isfinite(low) & (low > 0) & (high >= low) & (high >= close) & (low <= close)
        state_known = np.full(n, np.inf)
        watermark = -np.inf; prior_segment = -2
        for i in range(n):
            execution_checkpoint()
            if not present[i]:
                watermark = -np.inf; prior_segment = -2
                continue
            if segment[i] != prior_segment:
                watermark = -np.inf; prior_segment = segment[i]
            watermark = max(watermark, known[i]); state_known[i] = watermark
        in_period = times >= start
        reasons = np.full(n, "", dtype=object)
        rules = [(~present, "missing_source_candle"), (~valid_path, "invalid_source_candle"),
                 (~seen, "snapshot_readiness_unavailable"), (~ready, "indicator_not_ready"),
                 (~np.isfinite(metric), "public_metric_unavailable"),
                 (known < times+60, "source_known_before_close"),
                 (state_known > known, "state_history_not_known_at_decision"),
                 (known >= stop, "decision_at_or_after_evaluation_end")]
        flags = {reason: int((mask & in_period).sum()) for mask, reason in rules}
        for mask, reason in rules:
            reasons[mask & in_period & (reasons == "")] = reason
        observable = in_period & (reasons == "")
        if (shock & observable & (metric <= 2)).any():
            raise ValueError("forward_risk_invalid: shock event contradicts public current metric")
        cohort = np.full(n, -1, dtype=int)
        cohort[observable & (metric <= 2)] = 0
        cohort[observable & (metric > 2) & ~shock] = 2
        cohort[observable & shock] = 1
        logs = np.full(n, np.nan); logs[valid_close] = np.log(close[valid_close])
        increments = np.full(n, np.nan); increments[1:] = np.diff(logs) ** 2
        prefix = np.r_[0., np.cumsum(np.nan_to_num(increments, nan=0.))]
        bad = np.r_[0, np.cumsum(~np.isfinite(increments))]
        path_bad = np.r_[0, np.cumsum(~valid_path)]
        baseline = np.full(n, np.nan)
        prior_known = pd.Series(known).rolling(121, min_periods=121).max().shift(1).to_numpy()
        indices = np.arange(n)
        prior_bad_clock = np.r_[0, np.cumsum(present & (known < times+60))]
        history_ok = (indices >= 121) & (prior_known <= known)
        history_candidates = np.flatnonzero(history_ok)
        history_ok[history_candidates] &= (prior_bad_clock[history_candidates] - prior_bad_clock[history_candidates-121]) == 0
        where = np.flatnonzero(history_ok)
        where = where[(bad[where] - bad[where-120]) == 0]
        baseline[where] = (prefix[where] - prefix[where-120]) / 120
        dates = pd.to_datetime(np.where(present, known, times), unit="s", utc=True)
        source_months = np.asarray(pd.to_datetime(times, unit="s", utc=True).strftime("%Y-%m"))
        months = np.asarray(dates.strftime("%Y-%m")); days = np.asarray(dates.strftime("%Y-%m-%d")); weeks = np.asarray(dates.strftime("%G-W%V"))
        stratum = np.full(n, -1, dtype=int)
        baseline_ok = observable & np.isfinite(baseline)
        bins = np.searchsorted(np.asarray(RMS_BINS_BPS), np.sqrt(baseline[baseline_ok])*10000, side="right") - 1
        stratum[baseline_ok] = ((dates.month.to_numpy()[baseline_ok]-1)*4 + dates.hour.to_numpy()[baseline_ok]//6)*8 + bins
        price_indices = np.flatnonzero(valid_close & (known <= times+60) & (times+60 < stop))
        sample = np.full(n, -1, dtype=int)
        decisions = np.flatnonzero(observable)
        positions = np.searchsorted(times[price_indices]+60, np.maximum(known[decisions], times[decisions]+60), side="left")
        resolved = positions < price_indices.size
        sample[decisions[resolved]] = price_indices[positions[resolved]]
        sample_ok = observable & (sample >= 0)
        sample_times = np.full(n, -1, dtype=np.int64)
        sample_times[sample_ok] = times[sample[sample_ok]]+60
        outcome_values: dict[str, np.ndarray] = {}; outcome_ranges: dict[str, np.ndarray] = {}; outcome_reasons: dict[str, np.ndarray] = {}; range_reasons: dict[str, np.ndarray] = {}; outcome_known: dict[str, np.ndarray] = {}
        for seconds in (1800, 7200, 21600):
            execution_checkpoint()
            width = seconds//60
            values = np.full(n, np.nan); ranges = values.copy()
            why = np.full(n, "entry_sample_unavailable", dtype=object)
            why[~observable] = "undetectable"
            candidate = np.flatnonzero(sample_ok)
            target = sample[candidate] + width
            inside = (sample_times[candidate] + seconds) < stop
            why[candidate[~inside]] = "administrative_end_of_discovery"
            candidate, target = candidate[inside], target[inside]
            future_known = pd.Series(known).rolling(width, min_periods=width).max().shift(-width).to_numpy()
            clock_complete = (np.isfinite(future_known[sample[candidate]])
                & ((prior_bad_clock[target+1] - prior_bad_clock[sample[candidate]+1]) == 0))
            full = ((bad[target+1] - bad[sample[candidate]+1]) == 0) & clock_complete
            why[candidate] = "missing_or_invalid_future_close_path"
            selected, endpoint = candidate[full], target[full]
            values[selected] = (prefix[endpoint+1] - prefix[sample[selected]+1]) / width
            why[selected] = ""
            future_high = pd.Series(high).rolling(width, min_periods=width).max().shift(-width).to_numpy()
            future_low = pd.Series(low).rolling(width, min_periods=width).min().shift(-width).to_numpy()
            path_full = ((path_bad[target+1] - path_bad[sample[candidate]+1]) == 0) & clock_complete
            chosen = candidate[path_full]
            ranges[chosen] = (future_high[sample[chosen]] - future_low[sample[chosen]]) / close[sample[chosen]]
            range_why = why.copy()
            range_why[candidate] = "missing_or_invalid_future_high_low_path"
            range_why[chosen] = ""
            range_reasons[str(seconds)] = range_why
            available_at = np.full(n, np.nan)
            available_at[candidate[clock_complete]] = np.maximum(
                future_known[sample[candidate[clock_complete]]], known[sample[candidate[clock_complete]]])
            outcome_known[str(seconds)] = available_at
            outcome_values[str(seconds)] = values; outcome_ranges[str(seconds)] = ranges; outcome_reasons[str(seconds)] = why
        # Bounded examples plus an ordered all-observation digest avoid storing a second
        # year-sized copy of candles/results. Frozen inputs and versions own replay.
        digest = hashlib.sha256(); examples = {name: [] for name in GROUPS}
        for i in np.flatnonzero(in_period):
            execution_checkpoint()
            row = [int(times[i]), int(cohort[i]), str(reasons[i]), int(segment[i]),
                   float(baseline[i]) if np.isfinite(baseline[i]) else None,
                   int(sample_times[i]) if sample_ok[i] else None]
            for horizon in outcome_values:
                row.extend([float(outcome_values[horizon][i]) if np.isfinite(outcome_values[horizon][i]) else None,
                            float(outcome_ranges[horizon][i]) if np.isfinite(outcome_ranges[horizon][i]) else None,
                            str(outcome_reasons[horizon][i]), str(range_reasons[horizon][i]),
                            int(outcome_known[horizon][i]) if np.isfinite(outcome_known[horizon][i]) else None])
            row.append(int(known[i]) if present[i] else None)
            digest.update(json.dumps(row, separators=(",", ":"), allow_nan=False).encode()+b"\n")
            if cohort[i] >= 0 and len(examples[GROUPS[cohort[i]]]) < 5:
                examples[GROUPS[cohort[i]]].append(row)
        annual: dict[str, Any] = {}
        outcome_ratios: dict[str, np.ndarray] = {}; ratio_reasons: dict[str, np.ndarray] = {}
        nonoverlapping = _take_nonoverlapping(cohort, sample_times, sample_ok)
        for horizon, values in outcome_values.items():
            execution_checkpoint()
            ratios = np.full(n, np.nan); valid_ratio = np.isfinite(values) & np.isfinite(baseline) & (baseline > 0)
            ratios[valid_ratio] = values[valid_ratio] / baseline[valid_ratio]
            ratio_why = outcome_reasons[horizon].copy()
            ratio_why[np.isfinite(values) & ~np.isfinite(baseline)] = "prior_baseline_unavailable"
            ratio_why[np.isfinite(values) & (baseline == 0)] = "zero_prior_risk"
            outcome_ratios[horizon] = ratios; ratio_reasons[horizon] = ratio_why
            groups = {}
            for code, name in enumerate(GROUPS):
                mask = cohort == code; measured = mask & np.isfinite(values)
                day_counts = Counter(days[measured]); week_counts = Counter(weeks[measured])
                groups[name] = {"candidate_count": int(mask.sum()), "raw_risk": _distribution(values[mask]),
                    "path_range": _distribution(outcome_ranges[horizon][mask]), "future_prior_risk_ratio": _distribution(ratios[mask]),
                    "path_range_unresolved_reasons": dict(Counter(range_reasons[horizon][mask & ~np.isfinite(outcome_ranges[horizon])])),
                    "risk_ratio_unresolved_reasons": dict(Counter(ratio_why[mask & ~np.isfinite(ratios)])),
                    "outcome_known_after_target_count": int((measured & (outcome_known[horizon] > sample_times+int(horizon))).sum()),
                    "outcome_availability_delay_seconds": _distribution(np.maximum(0, outcome_known[horizon][measured]-sample_times[measured]-int(horizon))),
                    "zero_prior_risk_count": int((measured & (baseline == 0)).sum()),
                    "prior_baseline_unavailable_count": int((mask & ~np.isfinite(baseline)).sum()),
                    "unresolved_reasons": dict(Counter(outcome_reasons[horizon][mask & ~np.isfinite(values)])),
                    "distinct_days": len(day_counts), "distinct_weeks": len(week_counts),
                    "reset_segments": len(set(segment[measured])),
                    "largest_day_count": max(day_counts.values(), default=0),
                    "largest_week_count": max(week_counts.values(), default=0),
                    "largest_day_share": max(day_counts.values(), default=0)/int(measured.sum()) if measured.any() else None,
                    "largest_week_share": max(week_counts.values(), default=0)/int(measured.sum()) if measured.any() else None,
                    "overlap": _overlap(sample_times[measured], sample_times[measured]+int(horizon)),
                    "first_per_nonoverlapping_six_hours": _distribution(values[mask & nonoverlapping])}
            annual[horizon] = {"cohorts": groups,
                "raw_risk_stratified_comparison": _stratified(values, cohort, stratum, days, weeks),
                "risk_ratio_stratified_comparison": _stratified(ratios, cohort, stratum, days, weeks)}
        month_rows = []
        for month in sorted(set(source_months[in_period])):
            execution_checkpoint()
            source_mask = in_period & (source_months == month)
            mask = observable & (months == month)
            row = {"month": str(month), "expected_minutes": int(source_mask.sum()),
                   "observed_source_minutes": int((source_mask & present).sum()),
                   "detector_observable_source_minutes": int((source_mask & observable).sum()),
                   "observable_decisions": int(mask.sum()),
                   "first_blocking_stage_counts": dict(Counter(reasons[source_mask & ~observable])), "horizons": {}}
            for horizon, values in outcome_values.items():
                row["horizons"][horizon] = {name: {"candidate_count": int((mask & (cohort == code)).sum()),
                    "raw_risk": _distribution(values[mask & (cohort == code)]),
                    "outcome_known_after_target_count": int((mask & (cohort == code) & np.isfinite(values) & (outcome_known[horizon] > sample_times+int(horizon))).sum()),
                    "outcome_availability_delay_seconds": _distribution(np.maximum(0, outcome_known[horizon][mask & (cohort == code) & np.isfinite(values)]-sample_times[mask & (cohort == code) & np.isfinite(values)]-int(horizon))),
                    "path_range": _distribution(outcome_ranges[horizon][mask & (cohort == code)]),
                    "future_prior_risk_ratio": _distribution(outcome_ratios[horizon][mask & (cohort == code)]),
                    "unresolved_reasons": dict(Counter(outcome_reasons[horizon][mask & (cohort == code) & ~np.isfinite(values)])),
                    "path_range_unresolved_reasons": dict(Counter(range_reasons[horizon][mask & (cohort == code) & ~np.isfinite(outcome_ranges[horizon])])),
                    "risk_ratio_unresolved_reasons": dict(Counter(ratio_reasons[horizon][mask & (cohort == code) & ~np.isfinite(outcome_ratios[horizon])]))}
                    for code, name in enumerate(GROUPS)}
                row["horizons"][horizon]["raw_risk_stratified_comparison"] = _stratified(values[mask], cohort[mask], stratum[mask], days[mask], weeks[mask], deletions=False)
                row["horizons"][horizon]["risk_ratio_stratified_comparison"] = _stratified(outcome_ratios[horizon][mask], cohort[mask], stratum[mask], days[mask], weeks[mask], deletions=False)
            month_rows.append(row)
        undetectable = _intervals(times[in_period], reasons[in_period])
        observable_indices = np.flatnonzero(observable)
        for interval in undetectable:
            excluded_start = _seconds(interval["start"])
            eligible_prior = observable_indices[(times[observable_indices] < excluded_start)
                & (known[observable_indices] <= excluded_start)]
            if eligible_prior.size:
                prior = int(eligible_prior[-1])
                interval["preceding_observable_context"] = {"candle_open": _iso(times[prior]),
                    "known_at": _iso(known[prior]), "cohort": GROUPS[cohort[prior]],
                    "atr_zscore": float(metric[prior]),
                    "prior_risk": float(baseline[prior]) if np.isfinite(baseline[prior]) else None,
                    "minutes_since_candle_open": (excluded_start-int(times[prior]))//60}
            else:
                interval["preceding_observable_context"] = None
        analysis = {"schema_version": "candle_risk_comparison.v1", "analysis_status": "descriptive_only",
            "period": dict(plan.evaluation_range), "configuration": config, "indicator_parameters": dict(PINNED_PARAMS),
            "coverage": {"expected_minutes": int(in_period.sum()), "observed_source_minutes": int((present & in_period).sum()),
                "detector_observable_minutes": int(observable.sum()),
                "undetectable_intervals": undetectable,
                "coverage_clock": "source_candle_open", "cohort_calendar_clock": "decision_known_at",
                "missingness_interpretation": "Preceding observable context does not impute missing periods or establish random missingness.",
                "first_blocking_stage_counts": dict(Counter(reasons[in_period & ~observable])),
                "overlapping_dependency_flags": flags, "unknown_event_count": None},
            "horizons": annual, "months": month_rows,
            "entry_delay_seconds": _distribution(sample_times[sample_ok]-known[sample_ok]),
            "observation_ledger": {"schema_version": "candle_risk_observation_digest.v1",
                "columns": ["candle_open_epoch", "cohort_code", "detection_exclusion", "reset_segment", "prior_mean_squared_log_return", "sample_close_epoch"]
                    + [f"{h}_{field}" for h in outcome_values for field in ("mean_squared_log_return", "path_range", "risk_exclusion", "range_exclusion", "outcome_known_at_epoch")] + ["decision_known_at_epoch"],
                "cohort_codes": {str(i): name for i, name in enumerate(GROUPS)},
                "ordered_clock_rows": int(in_period.sum()), "sha256_json_lines": digest.hexdigest(),
                "examples": examples, "example_selection": "first_five_chronological_per_cohort",
                "full_rows_persisted": False},
            "outcome_availability_policy": "Retrospective frozen complete paths retain late reports with explicit outcome known-at; never reused as decision features.",
            "interpretation": "No independence, significance, causal, trading or promotion claim; unseen events remain unknown."}
        return {"schema_version": self.result_schema_version, "check_family": self.evaluator_id,
                "status": "completed", "analysis_status": "descriptive_only", "sample_count": int(observable.sum()),
                "verdict": "descriptive_only", "summary": "Declared-window candle risk comparison; inspect coverage, calendar variation and dependence.",
                "forward_risk_comparison": analysis, "data_quality": dict(inputs.get("data_quality") or {}),
                "hashes": {"forward_risk_comparison_hash": semantic_hash(analysis)},
                "promotion_authority": False, "execution_authority": False}
