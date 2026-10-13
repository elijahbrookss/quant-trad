"""Fixed, outcome-blind candle reversal matching owned by raw Check version 4."""
from __future__ import annotations

from bisect import bisect_left, insort
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
import math

import numpy as np

from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from . import checks
from .crossing_state_comparison import _balance
from .event_outcomes import resolve_event_outcomes
from .raw_candle_evaluator import _clock, _iso, _prepare

OUTCOME_SCHEMA = "controlled_reversal_comparison.v1"
RESULT_SCHEMA = "controlled_reversal_comparison_result.v1"
DETECTOR = {"type": "controlled_bullish_reversal"}
STEP = timedelta(minutes=1)
LOOKBACK = 21
HORIZON = 5
MAX_COMPARISONS = 3_000_000
FEATURES = ("log_body_size", "log_prior_rms", "decline_intensity")
CALIPERS = (math.log(1.25), math.log(1.25), .5)
FIXED_RULES = {
    "candidate": "bullish current, bearish previous, five prior close intervals declining",
    "treatment": "inclusive bullish body engulfing; control is its complement",
    "features": list(FEATURES), "calipers": list(CALIPERS),
    "prior_rms_returns": 20, "feature_lookback_bars": LOOKBACK,
    "stratum": "current candle close UTC date and hour",
    "matching": "chronological treated, minimum squared normalized distance, no control reuse",
    "tie_break": "absolute time distance then earlier control",
    "intervals": "closed entry-to-target intervals, shared endpoints overlap",
    "entry_lag_bars": 1, "forward_bars": [HORIZON], "timeframe": "1m",
    "outcome_boundary": "evaluation_end_exclusive", "return_unit": "fraction",
    "support": {"complete_pairs": 100, "utc_days": 10, "matching_fraction": .5,
                "outcome_completeness": .95, "absolute_smd_max": .1},
    "nonoverlap_support": {"complete_pairs": 30, "utc_days": 6},
    "dependence": "leave UTC day out and feature-frozen joint cross-arm nonoverlap; no rematching",
    "limits": {"evaluation_days": 32, "candles": 100000, "candidate_comparisons": MAX_COMPARISONS},
    "inference": "descriptive_only",
}


def _validate_scope(scope):
    if scope.get("timeframe", scope.get("interval")) != "1m":
        raise ValueError("controlled_reversal_invalid: fixed 1m timeframe required")
    warmup = scope.get("warmup_bars", LOOKBACK)
    if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup != LOOKBACK:
        raise ValueError("controlled_reversal_invalid: warmup must be exactly 21 bars when supplied")
    start, end = (_clock(scope.get(k), "scope."+k) for k in ("start", "end"))
    if start != start.floor("min") or end != end.floor("min") or not timedelta(0) < end-start <= timedelta(days=32):
        raise ValueError("controlled_reversal_invalid: aligned minute range of at most 32 days required")
    return start, end


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    if detector != DETECTOR or outcomes != {"schema_version": OUTCOME_SCHEMA}:
        raise ValueError("controlled_reversal_invalid: exact fixed detector and outcome schema required; no tuning fields")
    if inputs or statistics or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("controlled_reversal_invalid: no Indicator, additional inputs or statistics settings")
    if gap_policy != "continue_degraded":
        raise ValueError("controlled_reversal_invalid: continue_degraded with explicit local exclusions required")
    _validate_scope(scope)
    return {"schema_version": OUTCOME_SCHEMA}


def prepare_candidates(rows, *, start, end, choices=None):
    """Inspect only current/prior features; entry and forward availability are not selection inputs."""
    step = timedelta(seconds=INTERVALS[choices["interval"]]) if choices else STEP
    lookback = choices["volatility_lookback_bars"] + 1 if choices else LOOKBACK
    trend = choices["trend_lookback_bars"] if choices else 5
    horizon = choices["outcome_horizon_bars"] if choices else HORIZON
    lag = choices["entry_lag_bars"] if choices else 1
    by_open = {r["open_time"]: r for r in rows}
    candidates, exclusions = [], Counter()
    evaluated = 0
    for current in rows:
        execution_checkpoint()
        opened = current["open_time"]
        if not start <= opened < end:
            continue
        evaluated += 1
        history = [by_open.get(opened-step*i) for i in range(lookback, 0, -1)]
        if any(r is None for r in history):
            exclusions[f"prior_{lookback}_candle_path_missing"] += 1
            continue
        previous = history[-1]
        if not current["close"] > current["open"] or not previous["close"] < previous["open"]:
            exclusions["not_bullish_after_bearish"] += 1
            continue
        decline = previous["close"]/history[-(trend+1)]["close"]-1.
        if not decline < 0:
            exclusions["prior_five_interval_move_not_down" if not choices else "prior_decline_not_down"] += 1
            continue
        entry_close, target_close = opened+(lag+1)*step, opened+(lag+1+horizon)*step
        if target_close >= end:
            exclusions["nominal_target_outside_evaluation"] += 1
            continue
        known = max(r["known_at"] for r in [*history, current])
        if known > entry_close:
            exclusions["feature_known_after_entry_close"] += 1
            continue
        returns = [b["close"]/a["close"]-1. for a,b in zip(history, history[1:])]
        # hypot avoids overflowing the sum of squared returns for valid positive prices.
        rms = math.hypot(*returns)/math.sqrt(lookback-1)
        body = math.log(current["close"]/current["open"])
        intensity = -decline/(math.sqrt(trend)*rms) if rms > 0 else math.nan
        if not all(math.isfinite(v) and v > 0 for v in (rms, body, intensity)):
            exclusions["matching_feature_numeric_unavailable"] += 1
            continue
        candidates.append({"event_time": _iso(opened), "event_close_time": _iso(current["close_time"]),
            "decision_known_at": _iso(known), "entry_time": _iso(entry_close), "target_time": _iso(target_close),
            "day": current["close_time"].date().isoformat(),
            "stratum": (current["close_time"].strftime("%H:%M:%S")
                if choices and choices["matching_policy"] == "same_utc_close_slot"
                else _iso(current["close_time"].floor("h"))),
            "treated": current["open"] <= previous["close"] and current["close"] >= previous["open"],
            "features": dict(zip(FEATURES, (math.log(body), math.log(rms), intensity)))})
    return candidates, dict(exclusions), evaluated


def _disjoint(a, b):
    return a["target_time"] < b["entry_time"] or b["target_time"] < a["entry_time"]


def prepare_pairs(candidates, *, choices=None):
    """Freeze identities using features only. A failed future outcome never supplies another control."""
    pools = defaultdict(list)
    for c in candidates:
        if not c["treated"]:
            pools[c["stratum"]].append(c)
    used, pairs, unmatched = set(), [], []
    comparisons = 0
    for a in sorted((c for c in candidates if c["treated"]), key=lambda c: c["event_time"]):
        execution_checkpoint()
        best = None
        for b in pools[a["stratum"]]:
            comparisons += 1
            if comparisons % 256 == 0:
                execution_checkpoint()
            if comparisons > MAX_COMPARISONS:
                raise ValueError("controlled_reversal_resource_limit: candidate comparisons exceeded")
            if choices and choices["matching_policy"] == "same_utc_close_slot" and abs((_clock(a["event_close_time"], "close") -
                    _clock(b["event_close_time"], "close")).total_seconds()) > choices["comparison_window_days"] * 86400:
                continue
            if b["event_time"] in used or not _disjoint(a,b):
                continue
            distances = [abs(a["features"][f]-b["features"][f]) for f in FEATURES]
            if any(d > limit for d,limit in zip(distances,CALIPERS)):
                continue
            score = sum((d/limit)**2 for d,limit in zip(distances,CALIPERS))
            rank = (score, abs((_clock(a["event_time"],"event")-_clock(b["event_time"],"event")).total_seconds()), b["event_time"])
            if best is None or rank < best[0]:
                best = (rank,b)
        if best is None:
            unmatched.append({"event_time": a["event_time"], "reason": "no_unused_control_satisfies_fixed_constraints"})
        else:
            rank,b = best
            used.add(b["event_time"])
            pairs.append({"treated": a, "control": b, "matching_score": rank[0]})
    # Sorted disjoint retained intervals allow a bounded neighbor check even when
    # the selected control is later than its treatment in the same hour.
    intervals = []
    for pair in pairs:
        execution_checkpoint()
        proposed = [(pair[arm]["entry_time"],pair[arm]["target_time"]) for arm in ("treated","control")]
        retain = True
        for lo,hi in proposed:
            at = bisect_left(intervals,(lo,))
            if (at and intervals[at-1][1] >= lo) or (at < len(intervals) and intervals[at][0] <= hi):
                retain = False
        pair["joint_nonoverlap_selected"] = retain
        if retain:
            for interval in proposed:
                insort(intervals,interval)
    return pairs, unmatched, comparisons


def resolve_candidate(candidate, *, rows, by_open, end, choices=None):
    step = timedelta(seconds=INTERVALS[choices["interval"]]) if choices else STEP
    horizon = choices["outcome_horizon_bars"] if choices else HORIZON
    lag = choices["entry_lag_bars"] if choices else 1
    event_open = _clock(candidate["event_time"],"event")
    entry_open = event_open+lag*step
    if choices and any(event_open+step*i not in by_open for i in range(lag+1)):
        return {"status": "unresolved", "reason": "gap_in_entry_path"}
    entry = by_open.get(entry_open)
    if entry is None:
        return {"status": "unresolved", "reason": "gap_in_entry_path"}
    if entry["known_at"] > entry["close_time"]:
        return {"status": "unresolved", "reason": "entry_price_known_after_close"}
    row = {"outcomes": {}}
    resolve_event_outcomes(row=row, outcomes={"horizons": [horizon], "horizon_kind": "bars"},
        detector_type=checks.RAW_FORWARD_OUTCOME, entry_time=entry_open,
        entry_close_time=entry["close_time"], entry_close=entry["close"], direction=1.,
        step=step, evaluation_end=end, candles=rows, candle_by_open=by_open,
        event_payload={}, horizon_resolution={horizon: Counter()}, invalidation_resolution=Counter())
    result = row["outcomes"][str(horizon)]
    if result["status"] != "resolved":
        return {"status": "unresolved", "reason": result["reason"]}
    if not math.isfinite(result["forward_return"]):
        return {"status": "unresolved", "reason": "outcome_numeric_unavailable"}
    path = [by_open[entry_open+step*i] for i in range(horizon+1)]
    return {"status": "resolved", "forward_return": result["forward_return"],
            "entry_known_at": _iso(entry["known_at"]),
            "outcome_known_at": _iso(max(r["known_at"] for r in path))}


def _distribution(values):
    values = np.asarray(values,dtype=float)
    return {"count": len(values), "mean": float(values.mean()) if len(values) else None,
        "median": float(np.median(values)) if len(values) else None,
        "positive_fraction": float(np.mean(values > 0)) if len(values) else None}


def _summary(pairs):
    a = np.asarray([p["outcomes"]["treated"]["forward_return"] for p in pairs],dtype=float)
    b = np.asarray([p["outcomes"]["control"]["forward_return"] for p in pairs],dtype=float)
    return {"complete_pairs": len(pairs), "utc_days": len({p["treated"]["day"] for p in pairs}),
        "mean_paired_return_difference": float(np.mean(a-b)) if len(pairs) else None,
        "engulfing": _distribution(a), "control": _distribution(b)}


def summarize_pairs(pairs, *, eligible_treated, both_arm_days=False):
    complete = [p for p in pairs if all(v["status"] == "resolved" for v in p["outcomes"].values())]
    primary = _summary(complete)
    balance = {}
    for feature in FEATURES:
        a,b = (np.asarray([p[arm]["features"][feature] for p in complete],dtype=float) for arm in ("treated","control"))
        value = _balance(a,b)
        value["engulfing_mean"] = value.pop("crossing_mean")
        balance[feature] = value
    matching = len(pairs)/eligible_treated if eligible_treated else None
    completeness = len(complete)/len(pairs) if pairs else None
    gates = {"complete_pairs": len(complete) >= 100, "utc_days": primary["utc_days"] >= 10,
        "treated_matching": matching is not None and matching >= .5,
        "outcome_completeness": completeness is not None and completeness >= .95,
        "balance": all(v["absolute_smd"] is not None and v["absolute_smd"] <= .1 for v in balance.values())}
    days = sorted({p[arm]["day"] for p in complete
                   for arm in (("treated", "control") if both_arm_days else ("treated",))})
    if both_arm_days:
        primary["both_arm_utc_days"] = len(days)
    deletion = []
    for day in days:
        execution_checkpoint()
        deletion.append({"omitted_utc_day": day, **_summary([p for p in complete if p["treated"]["day"] != day and (not both_arm_days or p["control"]["day"] != day)])})
    subset = _summary([p for p in complete if p["joint_nonoverlap_selected"]])
    subset["supported"] = subset["complete_pairs"] >= 30 and subset["utc_days"] >= 6
    delta = primary["mean_paired_return_difference"]
    if not all(gates.values()):
        decision = "unsupported"
    elif delta <= 0:
        decision = "nonpositive"
    elif (subset["supported"] and subset["mean_paired_return_difference"] > 0 and deletion
          and all(d["mean_paired_return_difference"] is not None and d["mean_paired_return_difference"] > 0 for d in deletion)):
        decision = "positive_descriptive_lead"
    else:
        decision = "mixed"
    missing = Counter()
    for p in pairs:
        for arm,value in p["outcomes"].items():
            if value["status"] != "resolved":
                missing[arm+":"+value["reason"]] += 1
    return {"primary": primary, "balance": balance, "support_gates": gates,
        "treated_matching_fraction": matching, "matched_pair_outcome_completeness": completeness,
        "incomplete_pairs": len(pairs)-len(complete), "missing_outcome_reasons": dict(missing),
        "leave_one_utc_day_out": deletion, "joint_nonoverlap": subset, "decision": decision}


@dataclass(frozen=True)
class ControlledReversalEvaluator:
    evaluator_id: str = checks.RAW_FORWARD_OUTCOME
    version: str = "4"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        del definition
        _validate_scope(request.scope)
        return {"input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": 0,
            "feature_lookback_bars": LOOKBACK, "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [HORIZON], "required_outcome_horizons": [HORIZON], "horizon_kind": "bars",
            "entry_lag_bars": 1, "invalidation_max_bars": 0, "outcome_boundary": "evaluation_end_exclusive",
            "primary_candle_required_fields": ["open", "high", "low", "close", "known_at"]}

    def evaluate(self, *, plan, inputs: Mapping):
        primary = next(r for r in plan.market_data_requirements if r["alias"] == "primary_bars")
        if primary["timeframe_seconds"] != 60:
            raise ValueError("controlled_reversal_invalid: canonical 1m requirement needed")
        start,end = _validate_scope({"timeframe": "1m", "start": plan.evaluation_range["start"],
                                    "end": plan.evaluation_range["end_exclusive"]})
        _,rows = _prepare(inputs["candles"],step=STEP,volume_required=False)
        candidates,exclusions,evaluated = prepare_candidates(rows,start=start,end=end)
        pairs,unmatched,comparisons = prepare_pairs(candidates)
        # Commit the complete feature map and subset before any entry/path resolution.
        feature_hash = semantic_hash({"candidates": candidates, "pairs": pairs, "unmatched": unmatched})
        by_open = {r["open_time"]: r for r in rows}
        for pair in pairs:
            execution_checkpoint()
            pair["outcomes"] = {arm: resolve_candidate(pair[arm],rows=rows,by_open=by_open,end=end)
                                for arm in ("treated","control")}
        treated = sum(c["treated"] for c in candidates)
        analysis = summarize_pairs(pairs,eligible_treated=treated)
        return {"schema_version": RESULT_SCHEMA, "check_family": checks.RAW_FORWARD_OUTCOME,
            "status": "completed", "event_ownership": "check", "return_unit": "fraction",
            "sample_count": len(pairs), "complete_sample_count": analysis["primary"]["complete_pairs"],
            "eligible_bars": evaluated, "candidate_exclusions": exclusions,
            "eligible_treated": treated, "eligible_controls": len(candidates)-treated,
            "candidate_comparisons": comparisons, "feature_frozen_hash": feature_hash,
            "controlled_reversal_comparison": analysis, "pairs": pairs, "unmatched": unmatched,
            "known_at_methods": dict(Counter(r["known_at_method"] for r in rows)),
            "data_quality": dict(inputs.get("data_quality") or {}),
            "caveats": ["Descriptive matched association, not causality, significance or profitability.",
                "Day deletion and nonoverlap do not guarantee independence or remove unmeasured confounding.",
                "Inferred interval-close availability is not measured publication latency.",
                "Current-body, prior-risk and decline calipers are fixed; no outcome-driven rematching."]}


# V4 keeps its constants, material rules and entry point. V5 pins choices in the
# configured definition; bar counts mean exact elapsed intervals, never row offsets.
CONFIGURABLE_SCHEMA = "controlled_reversal_comparison.v2"
INTERVALS = {"1m": 60, "15m": 900, "30m": 1800, "1h": 3600}
CONFIGURABLE_RULES = {
    k: v for k, v in FIXED_RULES.items()
    if k not in {"candidate", "prior_rms_returns", "feature_lookback_bars", "stratum",
                 "entry_lag_bars", "forward_bars", "timeframe", "dependence"}
}
CONFIGURABLE_RULES.update({
    "choices_contract": CONFIGURABLE_SCHEMA,
    "candidate": "bullish after bearish with configured prior close decline",
    "dependence": "delete whole pair on either arm close date; frozen joint nonoverlap; no rematching",
    "support_day_count": "treated candle-close dates; both-arm coverage reported separately",
    "time_windows": "exact contiguous interval paths; no gap compression",
})


def validate_choices(outcomes, scope):
    keys = {"schema_version", "interval", "trend_lookback_bars", "volatility_lookback_bars",
            "entry_lag_bars", "outcome_horizon_bars", "matching_policy", "comparison_window_days"}
    if set(outcomes) != keys or outcomes.get("schema_version") != CONFIGURABLE_SCHEMA:
        raise ValueError("controlled_reversal_invalid: explicit v2 research choices required")
    c = dict(outcomes)
    interval = c["interval"]
    if interval not in INTERVALS or scope.get("timeframe", scope.get("interval")) != interval:
        raise ValueError("controlled_reversal_invalid: supported interval must equal scope timeframe")
    for key in ("trend_lookback_bars", "volatility_lookback_bars", "entry_lag_bars", "outcome_horizon_bars"):
        if isinstance(c[key], bool) or not isinstance(c[key], int) or not 1 <= c[key] <= 10000:
            raise ValueError(f"controlled_reversal_invalid: {key} must be an integer 1..10000")
    if c["trend_lookback_bars"] > c["volatility_lookback_bars"] or c["entry_lag_bars"] + c["outcome_horizon_bars"] > 10000:
        raise ValueError("controlled_reversal_invalid: decline must fit prior RMS window and lag+horizon <=10000")
    days = c["comparison_window_days"]
    if isinstance(days, bool) or not isinstance(days, int):
        raise ValueError("controlled_reversal_invalid: comparison_window_days must be an integer")
    if c["matching_policy"] == "same_utc_close_slot":
        if not 1 <= days <= 31:
            raise ValueError("controlled_reversal_invalid: cross-date window must be 1..31 days")
    elif c["matching_policy"] == "same_date_hour":
        if days != 0 or c["outcome_horizon_bars"] * INTERVALS[interval] >= 3600 - INTERVALS[interval]:
            raise ValueError("controlled_reversal_invalid: same-hour matching needs zero day radius and room for two disjoint closed paths")
    else:
        raise ValueError("controlled_reversal_invalid: unsupported matching policy")
    warmup = scope.get("warmup_bars", c["volatility_lookback_bars"] + 1)
    if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup != c["volatility_lookback_bars"] + 1:
        raise ValueError("controlled_reversal_invalid: warmup must equal volatility lookback +1")
    start, end = (_clock(scope.get(k), "scope."+k) for k in ("start", "end"))
    seconds = INTERVALS[interval]
    if any(t.timestamp() % seconds for t in (start, end)) or not timedelta(0) < end-start <= timedelta(days=32):
        raise ValueError("controlled_reversal_invalid: UTC aligned range of at most 32 days required")
    if (c["entry_lag_bars"] + 1 + c["outcome_horizon_bars"]) * seconds >= (end-start).total_seconds():
        raise ValueError("controlled_reversal_invalid: entry and outcome cannot fit evaluation range")
    return c, start, end


def normalize_configurable_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    if detector != DETECTOR or inputs or statistics or scope.get("indicator_id") or scope.get("indicator_param_overrides"):
        raise ValueError("controlled_reversal_invalid: fixed detector and no Indicator, inputs or statistics required")
    if gap_policy != "continue_degraded":
        raise ValueError("controlled_reversal_invalid: continue_degraded required")
    return validate_choices(outcomes, scope)[0]


@dataclass(frozen=True)
class ConfigurableControlledReversalEvaluator:
    evaluator_id: str = checks.RAW_FORWARD_OUTCOME
    version: str = "5"
    result_schema_version: str = "controlled_reversal_comparison_result.v2"

    def declare_requirements(self, *, definition, request):
        del definition
        c, _, _ = validate_choices(request.parameters["outcomes"], request.scope)
        return {"input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": 0,
            "feature_lookback_bars": c["volatility_lookback_bars"]+1, "feature_windows_seconds_by_alias": {},
            "outcome_horizons": [c["outcome_horizon_bars"]], "required_outcome_horizons": [c["outcome_horizon_bars"]],
            "horizon_kind": "bars", "entry_lag_bars": c["entry_lag_bars"], "invalidation_max_bars": 0,
            "outcome_boundary": "evaluation_end_exclusive",
            "primary_candle_required_fields": ["open", "high", "low", "close", "known_at"]}

    def evaluate(self, *, plan, inputs: Mapping):
        primary = next(r for r in plan.market_data_requirements if r["alias"] == "primary_bars")
        raw = inputs["outcomes"]
        c, start, end = validate_choices(raw, {"timeframe": raw["interval"],
            "start": plan.evaluation_range["start"], "end": plan.evaluation_range["end_exclusive"]})
        seconds = INTERVALS[c["interval"]]
        if primary["timeframe_seconds"] != seconds:
            raise ValueError("controlled_reversal_invalid: canonical requirement interval differs from pinned choices")
        _, rows = _prepare(inputs["candles"], step=timedelta(seconds=seconds), volume_required=False)
        candidates, exclusions, evaluated = prepare_candidates(rows, start=start, end=end, choices=c)
        pairs, unmatched, comparisons = prepare_pairs(candidates, choices=c)
        feature_hash = semantic_hash({"candidates": candidates, "pairs": pairs, "unmatched": unmatched})
        by_open = {r["open_time"]: r for r in rows}
        for pair in pairs:
            execution_checkpoint()
            pair["outcomes"] = {arm: resolve_candidate(pair[arm], rows=rows, by_open=by_open, end=end, choices=c)
                                for arm in ("treated", "control")}
        treated = sum(c["treated"] for c in candidates)
        analysis = summarize_pairs(pairs, eligible_treated=treated, both_arm_days=True)
        return {"schema_version": self.result_schema_version, "check_family": checks.RAW_FORWARD_OUTCOME,
            "status": "completed", "event_ownership": "check", "return_unit": "fraction", "research_choices": c,
            "elapsed_seconds": {"decline": c["trend_lookback_bars"]*seconds,
                "prior_rms": c["volatility_lookback_bars"]*seconds,
                "pattern_close_to_entry": c["entry_lag_bars"]*seconds,
                "entry_to_target": c["outcome_horizon_bars"]*seconds},
            "sample_count": len(pairs), "complete_sample_count": analysis["primary"]["complete_pairs"],
            "eligible_bars": evaluated, "candidate_exclusions": exclusions,
            "eligible_treated": treated, "eligible_controls": len(candidates)-treated,
            "candidate_comparisons": comparisons, "feature_frozen_hash": feature_hash,
            "controlled_reversal_comparison": analysis, "pairs": pairs, "unmatched": unmatched,
            "known_at_methods": dict(Counter(r["known_at_method"] for r in rows)),
            "data_quality": dict(inputs.get("data_quality") or {}),
            "caveats": ["Descriptive matched association; no causality, significance, profitability or execution claim.",
                "Exact elapsed-time paths exclude gaps; availability must permit the declared entry close.",
                "Cross-date controls may be later; pairing is retrospective and either-arm day deletion does not establish independence.",
                "Decline and RMS are interval-specific summaries, not a broad regime or annualized volatility.",
                "Inferred interval-close availability is not measured publication latency."]}
