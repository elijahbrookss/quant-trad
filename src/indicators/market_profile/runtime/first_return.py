"""Original-range first return on the Indicator's incremental timeline (v2 only)."""
from __future__ import annotations

from copy import deepcopy
from math import isfinite
from typing import Any

from .models import MarketProfileBarState
from .signal_state import AtrState
from .signals import build_signal_outputs


class FirstReturnState:
    """Retain independent raw origins for a bounded number of completed bars.

    A return is strict original-range membership, not active-profile occupancy.
    Returned and censored origins remain visible until expiry for fixed-clock reads.
    """

    def __init__(self, *, atr_period: int, lifetime_bars: int) -> None:
        if not 1 <= int(lifetime_bars) <= 100 or int(atr_period) < 1:
            raise ValueError("first_return_config_invalid: positive ATR period and lifetime 1..100 required")
        self.lifetime = int(lifetime_bars)
        self.atr = AtrState(period=int(atr_period))
        self.origins: list[dict[str, Any]] = []
        self.atr_known_at: float | None = None

    def interrupt(self, reason: str) -> None:
        for origin in self.origins:
            if origin["status"] == "no_return_observed":
                origin.update(status="censored", reason=reason)
        self.atr = AtrState(period=self.atr.period)
        self.atr_known_at = None

    def step(self, state: MarketProfileBarState) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        prior = self.atr.current_value if self.atr.seeded_count >= self.atr.period else None
        prior_ready = prior is not None and isfinite(prior) and prior > 0
        prior_known_at = self.atr_known_at
        self.atr.step(state)
        now = (state.known_at or state.bar_time).timestamp()
        self.atr_known_at = max(now, self.atr_known_at or now)
        events = []
        retained = []
        for origin in self.origins:
            origin["age_bars"] += 1
            origin["profile_changed_since_origin"] |= state.active_profile_key != origin["reference"]["key"]
            if origin["age_bars"] > self.lifetime + 1:
                continue
            if origin["age_bars"] == self.lifetime + 1:
                if origin["status"] == "no_return_observed":
                    origin.update(status="censored", reason="origin_lifetime_expired")
                retained.append(origin)
                continue
            origin["observed_known_at"] = max(now, origin["observed_known_at"])
            if origin["status"] == "no_return_observed":
                levels = origin["reference"]["context"]["active_value_area"]
                val, vah = levels["val"], levels["vah"]
                location = ("inside" if val < state.close < vah else "at_boundary" if state.close in (val, vah)
                            else "above" if state.close > vah else "below")
                origin["location"] = location
                if location == "inside":
                    origin.update(status="returned", first_return_time=state.bar_time.timestamp(),
                                  first_return_known_at=origin["observed_known_at"])
                    events.append({"key": "first_value_return_" + origin["direction"],
                        "direction": origin["direction"], "known_at": origin["first_return_known_at"],
                        "metadata": {"breakout_time": origin["breakout_time"],
                            "breakout_event_key": origin["breakout_event_key"],
                            "origin_known_at": origin["origin_known_at"], "reference": deepcopy(origin["reference"]),
                            "trigger_price": state.close, "age_bars": origin["age_bars"],
                            "prior_atr": prior if prior_ready else None, "prior_atr_ready": prior_ready}})
            retained.append(origin)
        # Consume the same public raw event builder, without an alternate breakout rule.
        for event in build_signal_outputs(state)["balance_breakout"].value["events"]:
            ref = deepcopy(event["metadata"]["reference"])
            levels = ref["context"]["active_value_area"]
            val, poc, vah = (levels[k] for k in ("val", "poc", "vah"))
            valid = all(isfinite(x) and x > 0 for x in (val, poc, vah)) and val < vah and val <= poc <= vah
            retained.append({"breakout_time": state.bar_time.timestamp(), "breakout_event_key": event["key"],
                "direction": event["direction"], "origin_known_at": event["known_at"],
                "observed_known_at": now, "reference": ref, "age_bars": 0,
                "status": "no_return_observed" if valid else "censored",
                "reason": None if valid else "original_range_invalid",
                "location": "above" if event["direction"] == "long" else "below",
                "first_return_time": None, "first_return_known_at": None, "profile_changed_since_origin": False})
        self.origins = retained
        return events, {"state_key": "observed", "fields": {
            "origins": deepcopy(retained), "active_profile_key": state.active_profile_key, "prior_atr": prior if prior_ready else None,
            "prior_atr_ready": prior_ready, "prior_atr_known_at": prior_known_at, "lifetime_bars": self.lifetime}}
