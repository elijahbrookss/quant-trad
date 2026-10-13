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


def prepare_candidates(rows, *, start, end):
    """Inspect only current/prior features; entry and forward availability are not selection inputs."""
    by_open = {r["open_time"]: r for r in rows}
    candidates, exclusions = [], Counter()
    evaluated = 0
    for current in rows:
        execution_checkpoint()
        opened = current["open_time"]
        if not start <= opened < end:
            continue
        evaluated += 1
        history = [by_open.get(opened-STEP*i) for i in range(LOOKBACK, 0, -1)]
        if any(r is None for r in history):
            exclusions["prior_21_candle_path_missing"] += 1
            continue
        previous = history[-1]
        if not current["close"] > current["open"] or not previous["close"] < previous["open"]:
            exclusions["not_bullish_after_bearish"] += 1
            continue
        decline = previous["close"]/history[-6]["close"]-1.
        if not decline < 0:
            exclusions["prior_five_interval_move_not_down"] += 1
            continue
        entry_close, target_close = opened+2*STEP, opened+7*STEP
        if target_close >= end:
            exclusions["nominal_target_outside_evaluation"] += 1
            continue
        known = max(r["known_at"] for r in [*history, current])
        if known > entry_close:
            exclusions["feature_known_after_entry_close"] += 1
            continue
        returns = [b["close"]/a["close"]-1. for a,b in zip(history, history[1:])]
        # hypot avoids overflowing the sum of squared returns for valid positive prices.
        rms = math.hypot(*returns)/math.sqrt(20)
        body = math.log(current["close"]/current["open"])
        intensity = -decline/(math.sqrt(5)*rms) if rms > 0 else math.nan
        if not all(math.isfinite(v) and v > 0 for v in (rms, body, intensity)):
            exclusions["matching_feature_numeric_unavailable"] += 1
            continue
        candidates.append({"event_time": _iso(opened), "event_close_time": _iso(current["close_time"]),
            "decision_known_at": _iso(known), "entry_time": _iso(entry_close), "target_time": _iso(target_close),
            "day": current["close_time"].date().isoformat(),
            "stratum": _iso(current["close_time"].floor("h")),
            "treated": current["open"] <= previous["close"] and current["close"] >= previous["open"],
            "features": dict(zip(FEATURES, (math.log(body), math.log(rms), intensity)))})
    return candidates, dict(exclusions), evaluated


def _disjoint(a, b):
    return a["target_time"] < b["entry_time"] or b["target_time"] < a["entry_time"]


def prepare_pairs(candidates):
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


def resolve_candidate(candidate, *, rows, by_open, end):
    entry_open = _clock(candidate["event_time"],"event")+STEP
    entry = by_open.get(entry_open)
    if entry is None:
        return {"status": "unresolved", "reason": "gap_in_entry_path"}
    if entry["known_at"] > entry["close_time"]:
        return {"status": "unresolved", "reason": "entry_price_known_after_close"}
    row = {"outcomes": {}}
    resolve_event_outcomes(row=row, outcomes={"horizons": [HORIZON], "horizon_kind": "bars"},
        detector_type=checks.RAW_FORWARD_OUTCOME, entry_time=entry_open,
        entry_close_time=entry["close_time"], entry_close=entry["close"], direction=1.,
        step=STEP, evaluation_end=end, candles=rows, candle_by_open=by_open,
        event_payload={}, horizon_resolution={HORIZON: Counter()}, invalidation_resolution=Counter())
    result = row["outcomes"][str(HORIZON)]
    if result["status"] != "resolved":
        return {"status": "unresolved", "reason": result["reason"]}
    if not math.isfinite(result["forward_return"]):
        return {"status": "unresolved", "reason": "outcome_numeric_unavailable"}
    path = [by_open[entry_open+STEP*i] for i in range(HORIZON+1)]
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


def summarize_pairs(pairs, *, eligible_treated):
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
    days = sorted({p["treated"]["day"] for p in complete})
    deletion = []
    for day in days:
        execution_checkpoint()
        deletion.append({"omitted_utc_day": day, **_summary([p for p in complete if p["treated"]["day"] != day])})
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
