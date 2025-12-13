"""
Breakout signal rule for Market Profile indicator.

Detects when price closes outside the current value area boundaries.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping

from signals.engine.signal_generator import signal_rule
from signals.rules.common.cache import append_to_cache, ensure_cache, mark_ready
from signals.rules.patterns import evaluate_signal_patterns

from ._evaluators import (
    BREAKOUT_PATTERN,
    _resolve_breakout_bar_index,
)
from ._config import resolve_breakout_config

log = logging.getLogger("MarketProfileBreakout")

_BREAKOUT_CACHE_KEY = "market_profile_breakouts"
_BREAKOUT_CACHE_INITIALISED = "_market_profile_breakouts_initialised"
_BREAKOUT_READY_FLAG = "_market_profile_breakouts_ready"


@signal_rule(
    "market_profile",
    rule_id="market_profile_breakout",
    label="Value Area Breakout",
    description=(
        "Detects when price closes outside the current value area, "
        "flagging potential initiative order flow."
    ),
)
def market_profile_breakout_rule(
    context: Mapping[str, Any], payload: Any
) -> List[Dict[str, Any]]:
    """
    Detect breakouts from value area boundaries.

    Signals depend ON indicator outputs, not vice versa.

    Args:
        context: Execution context with dataframe and configuration
        payload: Additional payload data

    Returns:
        List of signal dictionaries
    """
    breakout_config = resolve_breakout_config(context)
    df = context.get("df")
    if df is None or df.empty:
        return []

    # Get indicator data from context (not from indicator package!)
    indicator_data = context.get("market_profile")
    if not indicator_data:
        log.debug("No market profile data in context")
        return []

    # Use profiles (should be List[Profile] from new API)
    profiles = getattr(indicator_data, "daily_profiles", [])
    if not profiles:
        log.debug("No profiles available")
        return []

    # Initialize cache if needed
    if not context.get(_BREAKOUT_CACHE_INITIALISED):
        ensure_cache(context, _BREAKOUT_CACHE_KEY)
        context[_BREAKOUT_CACHE_INITIALISED] = True

    # Evaluate breakout pattern
    matches = evaluate_signal_patterns(
        df=df,
        patterns=[BREAKOUT_PATTERN],
        evaluator_context={
            "profiles": profiles,
            "confirmation_bars": breakout_config.confirmation_bars,
        },
    )

    results = []
    for match in matches:
        bar_index = _resolve_breakout_bar_index(match, df)
        if bar_index is None:
            continue

        signal_data = {
            "bar_index": bar_index,
            "direction": match.get("direction"),
            "level_type": match.get("level_type"),
            "level_price": match.get("level_price"),
            "metadata": match,
        }

        # Check cache to avoid duplicates
        if append_to_cache(context, _BREAKOUT_CACHE_KEY, signal_data):
            results.append(signal_data)

    # Mark cache as ready
    mark_ready(context, _BREAKOUT_READY_FLAG)

    cache_size = len(context.get(_BREAKOUT_CACHE_KEY, []))
    log.debug("Breakout rule | emitted=%d | cache_size=%s", len(results), cache_size)
    return results
