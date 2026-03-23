"""Derived bar-state helpers for market profile runtime."""

from __future__ import annotations

from typing import Any

from engines.bot_runtime.core.domain import Candle

from ..compute.internal.runtime_profiles import profile_identity
from .models import MarketProfileBarState


def derive_market_profile_bar_state(
    *,
    bar: Candle,
    active_profile: Any,
    previous_profile_key: str | None,
    previous_location: str | None,
) -> MarketProfileBarState:
    active_profile_key = profile_identity(active_profile)
    resolved_previous_location = (
        previous_location if previous_profile_key == active_profile_key else None
    )

    close = float(bar.close)
    val = float(active_profile.val)
    vah = float(active_profile.vah)
    poc = float(active_profile.poc)
    location = "inside_value"
    if close > vah:
        location = "above_value"
    elif close < val:
        location = "below_value"
    balance_state = "balanced" if location == "inside_value" else "imbalanced"

    return MarketProfileBarState(
        bar_time=bar.time,
        active_profile_key=active_profile_key,
        previous_location=resolved_previous_location,
        location=location,
        balance_state=balance_state,
        close=close,
        val=val,
        vah=vah,
        poc=poc,
    )


__all__ = ["derive_market_profile_bar_state"]
