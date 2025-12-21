"""Bootstrap helpers for Market Profile rule execution."""

from __future__ import annotations

from typing import Any, MutableMapping, Sequence

from signals.rules.common.cache import mark_ready

from .breakout import _BREAKOUT_READY_FLAG, market_profile_breakout_rule
from .breakout_v2 import _BREAKOUT_V2_READY_FLAG, market_profile_breakout_v2_rule


def ensure_breakouts_ready(context: MutableMapping[str, Any], payloads: Sequence[Any]) -> None:
    """Populate breakout cache once per context before running dependent rules."""

    # Run both v1 and v2 breakout rules to populate their respective caches
    if not context.get(_BREAKOUT_READY_FLAG):
        payload = payloads[0] if payloads else {}
        market_profile_breakout_rule(context, payload)
        mark_ready(context, _BREAKOUT_READY_FLAG, ready=True)

    if not context.get(_BREAKOUT_V2_READY_FLAG):
        payload = payloads[0] if payloads else {}
        market_profile_breakout_v2_rule(context, payload)
        mark_ready(context, _BREAKOUT_V2_READY_FLAG, ready=True)


__all__ = ["ensure_breakouts_ready"]
