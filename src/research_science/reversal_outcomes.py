"""Fixed descriptive first-passage outcomes for completed reversal chains.

Recognition stays in its sequential engine. This procedure consumes its frozen
events and canonical candles; it never selects a winning chain or trading fill.
"""
from __future__ import annotations

from collections import Counter
from datetime import timedelta
import math
from typing import Callable

from .recognition import RecognitionCandle, RecognitionEvent, plain
from .hashing import stable_hash

HORIZON_BARS = 12
CLASSES = ("continuation", "failure", "neither", "unresolved")
RULES = {
    "version": "reversal_first_passage.v1",
    "reference_basis": "rising_leg_plus_retreat",
    "reference_lock": "first retreat candle close;no subsequent reference changes",
    "anchor": "formation completion close;observational price,not an executed fill",
    "horizon_bars": HORIZON_BARS, "timeframe_seconds": 900,
    "failure_level": "minimum low from formation_start through completion inclusive",
    "continuation_level": "E+(E-L),where E=completion close and L=formation low",
    "touch": "inclusive high/low;begin with the next candle after completion",
    "both_first_touched_same_bar": "unresolved:intrabar_order_unknown",
    "neither": "all12subsequent candles observed without either touch",
    "missing_or_boundary": "unresolved unless a first passage was already observed",
    "overlap": "transitive connected components of closed formation-start to nominal12bar-end intervals",
    "overlap_window": "outcome-blind;never shortened to realized passage time",
    "primary": "equal-component weighted within-component class fractions;all four classes in denominator",
    "inference": "none;components are not a claim of independent observations",
    "population": "retain every completed chain;no winner,expiry,size filter or deduplication",
}


def first_passage(event: RecognitionEvent, candles: tuple[RecognitionCandle, ...], *,
                  by_open: dict, evaluation_end, checkpoint: Callable[[], None] | None = None) -> dict:
    """Evaluate only inside an admitted Check;an early passage needs no future tail."""
    if event.kind != "provisional_reversal_candidate" or event.context.timeframe_seconds != 900:
        raise ValueError("reversal_outcome_invalid: completed15m reversal chain required")
    step = timedelta(seconds=event.context.timeframe_seconds)
    anchor = by_open.get(event.occurred_at-step)
    formation = [c for c in candles if event.formation_start <= c.open_time and c.close_time <= event.occurred_at]
    if anchor is None or not formation or anchor.known_at != event.known_at:
        raise ValueError("reversal_outcome_invalid: completion anchor/path unavailable")
    low = min(c.low for c in formation)
    entry = anchor.close
    return first_passage_path(anchor_at=event.occurred_at, known_at=event.known_at,
        entry=entry, low=low, source_identity_key=event.context.source_identity_key,
        timeframe_seconds=event.context.timeframe_seconds, by_open=by_open,
        evaluation_end=evaluation_end, checkpoint=checkpoint)


def first_passage_path(*, anchor_at, known_at, entry, low, source_identity_key,
                       timeframe_seconds, by_open, evaluation_end, checkpoint=None):
    """Shared path calculation for a locked observational anchor and barriers.

    The caller owns formation/control geometry. This function does not recognize
    events, select pairs, infer intrabar ordering or change the fixed horizon.
    """
    step = timedelta(seconds=timeframe_seconds)
    upper = entry+(entry-low)
    nominal_end = anchor_at+HORIZON_BARS*step
    result = {"anchor_price": entry, "anchor_known_at": plain(known_at),
              "formation_low": low, "formation_low_known_at": plain(known_at),
              "continuation_level": upper if math.isfinite(upper) else None, "failure_level": low,
              "nominal_end": plain(nominal_end), "observed_bars": 0}

    def finish(label, reason, known):
        return {**result, "classification": label, "reason": reason, "outcome_known_at": plain(known)}

    if entry <= low:
        return finish("unresolved", "invalid_nonpositive_formation_risk", known_at)
    if not math.isfinite(upper):
        return finish("unresolved", "invalid_nonfinite_continuation_barrier", known_at)

    for offset in range(HORIZON_BARS):
        if checkpoint:
            checkpoint()
        opened = anchor_at+offset*step
        # Input opens are [origin,end);a final candle closing at end is usable.
        if opened >= evaluation_end:
            return finish("unresolved", "evaluation_boundary", None)
        bar = by_open.get(opened)
        if bar is None:
            return finish("unresolved", "candle_missing", None)
        if bar.source_identity_key != source_identity_key or bar.known_at != bar.close_time:
            return finish("unresolved", "source_or_close_availability_mismatch", None)
        result["observed_bars"] += 1
        up, down = bar.high >= upper, bar.low <= low
        if up and down:
            return finish("unresolved", "intrabar_order_unknown", bar.known_at)
        if up or down:
            return finish("continuation" if up else "failure", "first_touch", bar.known_at)
    return finish("neither", "no_touch_in_complete_horizon", by_open[anchor_at+(HORIZON_BARS-1)*step].known_at)


def overlap_components(events: tuple[RecognitionEvent, ...]) -> tuple[dict, ...]:
    """Fixed interval union;does not read any outcome label or passage time."""
    if len({e.context for e in events}) > 1:
        raise ValueError("reversal_outcome_invalid: components require one exact context")
    groups = []
    for event in sorted(events, key=lambda e: (e.formation_start, e.occurred_at, e.event_id)):
        end = event.occurred_at+timedelta(seconds=HORIZON_BARS*event.context.timeframe_seconds)
        if not groups or event.formation_start > groups[-1]["end"]:
            groups.append({"start": event.formation_start, "end": end, "events": [event]})
        else:
            groups[-1]["end"] = max(groups[-1]["end"], end)
            groups[-1]["events"].append(event)
    return tuple({"component_id": stable_hash(sorted(e.event_id for e in g["events"])),
                  "start": plain(g["start"]), "nominal_end": plain(g["end"]),
                  "event_ids": sorted(e.event_id for e in g["events"])} for g in groups)


def summarize(events: tuple[RecognitionEvent, ...], outcomes: dict, components: tuple[dict, ...]) -> dict:
    counts = Counter(outcomes[e.event_id]["classification"] for e in events)
    component_rows = []
    for component in components:
        c = Counter(outcomes[key]["classification"] for key in component["event_ids"])
        n = len(component["event_ids"])
        component_rows.append({**component, "chain_count": n,
            "class_counts": {k: c[k] for k in CLASSES}, "class_shares": {k: c[k]/n for k in CLASSES}})
    n, g = len(events), len(components)
    hhi = sum((len(c["event_ids"])/n)**2 for c in components) if n else None
    return {"chain_count": n, "component_count": g,
            "class_counts": {k: counts[k] for k in CLASSES},
            "chain_weighted_shares": {k: counts[k]/n if n else None for k in CLASSES},
            "equal_component_weighted_shares": {k: sum(c["class_shares"][k] for c in component_rows)/g if g else None for k in CLASSES},
            "components": component_rows,
            "completion_utc_day_counts": dict(sorted(Counter(plain(e.occurred_at)[:10] for e in events).items())),
            "unresolved_reasons": dict(Counter(o["reason"] for o in outcomes.values() if o["classification"] == "unresolved")),
            "largest_component_fraction": max((c["chain_count"] for c in component_rows), default=0)/n if n else None,
            "component_chain_mass_hhi": hhi,
            "inverse_chain_mass_hhi": 1/hhi if hhi else None,
            "concentration_meaning": "Descriptive chain-mass concentration;inverse HHI is not effective independent sample size.",
            "independence_claim": False, "inferential_or_trading_claim": False}
