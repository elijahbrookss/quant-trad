"""Complete event retention and descriptive distribution diagnostics over raw candles."""
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timedelta
import math
from statistics import fmean, median
import numpy as np

from core.execution_control import execution_checkpoint
from market_data.frozen import semantic_hash
from .raw_candle_evaluator import (RawCandleEvaluator, OUTCOME_SCHEMA as RAW_SCHEMA,
    normalize_configuration as normalize_raw, detector_fields, _clock, _iso)

OUTCOME_SCHEMA = "raw_candle_distribution.v1"
RESULT_SCHEMA = "raw_candle_distribution_result.v1"
RULES = {"cohorts": "volume_gt_previous_vs_lte.v1", "prior_volatility_bars": 20,
         "quantiles": [.01, .05, .25, .5, .75, .95, .99],
         "quantile_method": "linear", "tail_fractions": [.01, .05],
         "tail_membership": "ceil_n_fraction_rank_then_event_id.v1",
         "dependence": "closed_formation_to_target_overlap_components.v1",
         "inference": "descriptive_only", "complete_event_limit": 10000}


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    if outcomes.get("schema_version") != OUTCOME_SCHEMA:
        raise ValueError("candle_distribution_invalid: explicit outcome schema required")
    if any(k in outcomes for k in ("distribution", "quantiles", "trim", "winsorize")):
        raise ValueError("candle_distribution_invalid: fixed diagnostics; no tail removal or tuning")
    if scope.get("warmup_bars", 1) != 1:
        raise ValueError("candle_distribution_invalid: preserve one-bar source binding; context may be unavailable")
    if not {"previous_volume", "volume"} <= detector_fields(detector):
        raise ValueError("candle_distribution_invalid: explicit current and previous volume predicates required")
    raw = normalize_raw(detector=detector, outcomes={**outcomes, "schema_version": RAW_SCHEMA},
        statistics=statistics, inputs=inputs, scope=scope, gap_policy=gap_policy)
    return {**raw, "schema_version": OUTCOME_SCHEMA}


def formation_context(current, by_open, *, step, decision, prior_bars=20):
    """Completed prior candles only; no extra source rows or missing-value imputation."""
    opened = current["open_time"]
    history = [by_open.get(opened-step*i) for i in range(prior_bars, 0, -1)]
    available = all(r is not None and r["known_at"] <= decision for r in history)
    returns = [b["close"]/a["close"]-1 for a,b in zip(history, history[1:])] if available else []
    return {"formation_body_fraction": (current["close"]-current["open"])/current["open"],
        "formation_range_fraction": (current["high"]-current["low"])/current["open"],
        "prior_rms_return": math.hypot(*returns)/math.sqrt(len(returns)) if returns else None,
        "prior_context_status": "available" if available else f"prior_{prior_bars}_bar_context_unavailable"}


def describe(rows, horizon, calendar_days=()):
    resolved = [r for r in rows if r["outcomes"][str(horizon)]["status"] == "resolved"]
    ordered = sorted(resolved, key=lambda r: (r["outcomes"][str(horizon)]["forward_return_pct"], r["event_id"]))
    values = [r["outcomes"][str(horizon)]["forward_return_pct"] for r in ordered]
    if not values:
        return {"sample_count": 0, "unresolved_count": len(rows), "mean": None,
                "quantiles": {}, "tails": [], "days": [{"day":d,"count":0,"mean":None,
                    "return_sum":0.,"contribution_to_full_mean":None} for d in calendar_days], "leave_one_day_out": []}
    n = len(values)
    tails = []
    for fraction in RULES["tail_fractions"]:
        count = math.ceil(n * fraction)
        for side, selected in (("lower", ordered[:count]), ("upper", ordered[-count:])):
            total = math.fsum(r["outcomes"][str(horizon)]["forward_return_pct"] for r in selected)
            tails.append({"fraction": fraction, "side": side, "count": count,
                "return_sum": total, "contribution_to_full_mean": total/n,
                "event_ids": [r["event_id"] for r in selected]})
    days = defaultdict(list)
    for day in calendar_days:days[day]=[]
    for r in resolved:
        days[r["utc_day"]].append(r["outcomes"][str(horizon)]["forward_return_pct"])
    daily = [{"day": day, "count": len(v), "return_sum": math.fsum(v),
              "mean": fmean(v) if v else None, "contribution_to_full_mean": math.fsum(v)/n}
             for day, v in sorted(days.items())]
    loo = []
    for day in sorted(days):
        remainder = [v for other, vv in days.items() if other != day for v in vv]
        loo.append({"omitted_day": day, "count": len(remainder),
                    "mean": fmean(remainder) if remainder else None})
    return {"sample_count": n, "unresolved_count": len(rows)-n, "mean": fmean(values),
        "median": median(values), "min": min(values), "max": max(values),
        "positive_rate": sum(v > 0 for v in values)/n,
        "quantiles": {str(q): float(np.quantile(values, q, method="linear")) for q in RULES["quantiles"]},
        "tails": tails, "days": daily, "equal_day_mean": fmean(r["mean"] for r in daily if r["count"]),
        "equal_day_population": "days_with_resolved_events",
        "leave_one_day_out": loo}


def assign_overlap_components(events, horizon, seconds):
    """Closed support intervals join transitively across both cohorts; no thinning."""
    component, right = 0, None
    for row in events:
        execution_checkpoint()
        left = row["formation_start"]
        outcome = row["outcomes"][str(horizon)]
        endpoint = outcome.get("target_time") or _iso(row["_opened"] + timedelta(
            seconds=seconds*(row["_lag"]+1+horizon)))
        # ISO UTC values have uniform second precision from the canonical minute grid.
        if right is None or left > right:
            component += 1
        right = max(right or endpoint, endpoint)
        row.setdefault("overlap_components", {})[str(horizon)] = component
    return component


@dataclass(frozen=True)
class CandleDistributionEvaluator:
    evaluator_id: str = "raw_forward_outcome"
    version: str = "6"
    result_schema_version: str = RESULT_SCHEMA

    def declare_requirements(self, *, definition, request):
        declaration = dict(RawCandleEvaluator().declare_requirements(definition=definition, request=request))
        declaration["primary_candle_required_fields"] = sorted(set(declaration["primary_candle_required_fields"]) | {"volume"})
        declaration["feature_lookback_bars"] = max(1, declaration["feature_lookback_bars"])
        return declaration

    def evaluate(self, *, plan, inputs):
        events = []
        primary = next(r for r in plan.market_data_requirements if r["alias"] == "primary_bars")
        seconds = primary["timeframe_seconds"]
        step = timedelta(seconds=seconds)
        def collect(example, current, prior, by_open):
            if len(events) >= RULES["complete_event_limit"]:
                raise ValueError("candle_distribution_limit: complete event table exceeds 10000; no truncation")
            if prior is None:
                raise ValueError("candle_distribution_invalid: previous formation candle required")
            for r in (prior, current):
                v = r.get("volume")
                if v is None or not math.isfinite(float(v)) or float(v) < 0:
                    raise ValueError("candle_distribution_invalid: unavailable volume cannot form a cohort")
            opened = current["open_time"]
            events.append({**example, "event_id": semantic_hash({"event_time": example["event_time"],
                "source": current["source_identity_key"], "instrument_id": primary.get("instrument_id"),
                "timeframe_seconds": seconds}), "utc_day": current["close_time"].date().isoformat(),
                "cohort": "increasing_volume" if current["volume"] > prior["volume"] else "non_increasing_volume",
                "volume": current["volume"], "previous_volume": prior["volume"],
                "volume_ratio": current["volume"]/prior["volume"] if prior["volume"] else None,
                **formation_context(current, by_open, step=step,
                    decision=_clock(example["decision_known_at"], "decision")),
                "formation_start": _iso(prior["open_time"]),
                "_opened": opened, "_lag": inputs["outcomes"]["entry_lag_bars"]})
        # The collector is private method composition, never an alternate input adapter.
        raw = RawCandleEvaluator().evaluate(plan=plan, inputs=inputs, _event_collector=collect)
        components = {}
        for h in inputs["outcomes"]["forward_bars"]:
            components[str(h)] = assign_overlap_components(events, h, seconds)
        for row in events:
            row.pop("_opened", None); row.pop("_lag", None)
        day=_clock(plan.evaluation_range["start"],"start").date()
        last=(_clock(plan.evaluation_range["end_exclusive"],"end")-timedelta(microseconds=1)).date()
        calendar_days=[]
        while day<=last:
            calendar_days.append(day.isoformat());day+=timedelta(days=1)
        cohorts = {name: {str(h): describe([r for r in events if r["cohort"] == name], h, calendar_days)
            for h in inputs["outcomes"]["forward_bars"]}
            for name in ("increasing_volume", "non_increasing_volume")}
        contrasts = {}
        for h in inputs["outcomes"]["forward_bars"]:
            a,b=(cohorts[n][str(h)] for n in ("increasing_volume","non_increasing_volume"))
            if a["mean"] is None or b["mean"] is None:
                contrasts[str(h)]={"status":"cohort_unresolved"};continue
            contrasts[str(h)]={"status":"descriptive","mean_difference":a["mean"]-b["mean"],
                "quantile_differences":{q:a["quantiles"][q]-b["quantiles"][q] for q in a["quantiles"]},
                "tail_contribution_differences":[{"fraction":x["fraction"],"side":x["side"],
                    "difference":x["contribution_to_full_mean"]-y["contribution_to_full_mean"]}
                    for x,y in zip(a["tails"],b["tails"])],
                "day_contribution_differences":[{"day":x["day"],
                    "difference":x["contribution_to_full_mean"]-y["contribution_to_full_mean"]}
                    for x,y in zip(a["days"],b["days"])]}
        return {**raw, "schema_version": RESULT_SCHEMA, "events": events,
            "recommendation": "inspect_distribution_only",
            "event_count_truncated": 0, "distribution_rules": RULES,
            "distribution": {"cohorts": cohorts, "contrasts":contrasts,"overlap_component_counts": components,
                "context_unavailable": dict(Counter(r["prior_context_status"] for r in events)),
                "mean_difference_increasing_minus_non_increasing": {str(h):
                    cohorts["increasing_volume"][str(h)]["mean"]-cohorts["non_increasing_volume"][str(h)]["mean"]
                    if all(cohorts[n][str(h)]["mean"] is not None for n in cohorts) else None
                    for h in inputs["outcomes"]["forward_bars"]}},
            "caveats": raw["caveats"] + ["Every valid event and tail is retained. Day and overlap diagnostics do not establish independent trials; leave-one-day-out is influence analysis, not claim rescue."]}
