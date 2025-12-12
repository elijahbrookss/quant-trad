"""Signal rules for Market Profile indicators."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from indicators.market_profile import MarketProfileIndicator
from signals.engine.signal_generator import signal_rule
from signals.rules.common.cache import append_to_cache, ensure_cache, mark_ready
from signals.rules.market_profile.config import MarketProfileBreakoutConfig
from signals.rules.market_profile.evaluators import (
    _BREAKOUT_PATTERN,
    _RETEST_PATTERN,
    _detect_value_area_retest,
    _resolve_breakout_bar_index,
    _value_area_breakout_evaluator,
    _value_area_retest_evaluator,
)
from signals.rules.market_profile.overlays import market_profile_overlay_adapter
from signals.rules.patterns import evaluate_signal_patterns

log = logging.getLogger("MarketProfileRules")

_BREAKOUT_CACHE_KEY = "market_profile_breakouts"
_BREAKOUT_CACHE_INITIALISED = "_market_profile_breakouts_initialised"
_BREAKOUT_READY_FLAG = "_market_profile_breakouts_ready"


@signal_rule(
    MarketProfileIndicator,
    rule_id="market_profile_breakout",
    label="Value Area Breakout",
    description=(
        "Detects when price closes outside the current value area, flagging potential initiative order flow."
    ),
)
def market_profile_breakout_rule(context: Mapping[str, Any], payload: Any) -> List[Dict[str, Any]]:
    mutable = ensure_cache(
        context,
        _BREAKOUT_CACHE_KEY,
        list,
        ready_flag=_BREAKOUT_READY_FLAG,
        initialised_flag=_BREAKOUT_CACHE_INITIALISED,
    )

    results = evaluate_signal_patterns(context, payload, [_BREAKOUT_PATTERN])

    if mutable is not None and results:
        append_to_cache(context, _BREAKOUT_CACHE_KEY, results)

    mark_ready(context, _BREAKOUT_READY_FLAG, ready=True)

    cache_size = None
    if mutable and isinstance(mutable.get(_BREAKOUT_CACHE_KEY), list):
        cache_size = len(mutable[_BREAKOUT_CACHE_KEY])

    log.debug("mp_brk_rule | emitted=%d | cache_size=%s", len(results), cache_size)
    return results


@signal_rule(
    MarketProfileIndicator,
    rule_id="market_profile_retest",
    label="Value Area Retest",
    description=(
        "Highlights pullbacks to a recently broken value area boundary that hold, signalling continuation setups."
    ),
)
def market_profile_retest_rule(context: Mapping[str, Any], payload: Any) -> List[Dict[str, Any]]:
    df = context.get("df")
    if df is None or getattr(df, "empty", True):
        return []

    if not context.get(_BREAKOUT_READY_FLAG):
        market_profile_breakout_rule(context, payload)

    results = evaluate_signal_patterns(context, payload, [_RETEST_PATTERN])

    mark_ready(context, _BREAKOUT_READY_FLAG, ready=True)

    log.debug("mp_retest_rule | emitted=%d", len(results))
    return results


__all__ = [
    "MarketProfileBreakoutConfig",
    "market_profile_breakout_rule",
    "market_profile_retest_rule",
    "market_profile_overlay_adapter",
    "_value_area_breakout_evaluator",
    "_value_area_retest_evaluator",
    "_detect_value_area_retest",
    "_resolve_breakout_bar_index",
    "_BREAKOUT_CACHE_KEY",
    "_BREAKOUT_CACHE_INITIALISED",
    "_BREAKOUT_READY_FLAG",
]
