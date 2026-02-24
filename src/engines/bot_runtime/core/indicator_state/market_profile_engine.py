"""Market Profile implementation of the IndicatorStateEngine contract."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from engines.bot_runtime.core.domain import Candle, timeframe_to_seconds

from .contracts import IndicatorStateDelta, IndicatorStateEngine, IndicatorStateSnapshot


@dataclass(frozen=True)
class MarketProfileEngineConfig:
    params: Dict[str, Any] = field(default_factory=dict)
    overlay_color: Optional[str] = None


class MarketProfileStateEngine(IndicatorStateEngine):
    def __init__(self, config: Optional[MarketProfileEngineConfig] = None) -> None:
        self._config = config or MarketProfileEngineConfig()

    def _param(self, key: str, default: Any = None) -> Any:
        params = self._config.params if isinstance(self._config.params, Mapping) else {}
        value = params.get(key)
        return default if value is None else value

    def _param_float(self, key: str, default: float) -> float:
        try:
            return float(self._param(key, default))
        except (TypeError, ValueError):
            return default

    def _param_int(self, key: str, default: int) -> int:
        try:
            return int(self._param(key, default))
        except (TypeError, ValueError):
            return default

    def _param_bool(self, key: str, default: bool) -> bool:
        value = self._param(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"1", "true", "yes", "on"}:
                return True
            if text in {"0", "false", "no", "off"}:
                return False
        return bool(value)

    def initialize(self, window_context: Mapping[str, Any]) -> MutableMapping[str, Any]:
        symbol = str(window_context.get("symbol") or "")
        if not symbol:
            raise RuntimeError("indicator_state_init_failed: market_profile requires symbol")
        chart_timeframe = str(window_context.get("timeframe") or "").strip()
        indicator_id = str(window_context.get("indicator_id") or "").strip()
        strategy_id = str(window_context.get("strategy_id") or "").strip()
        if chart_timeframe:
            chart_timeframe_seconds = int(timeframe_to_seconds(chart_timeframe) or 0)
        else:
            chart_timeframe_seconds = 0
        if not chart_timeframe:
            raise RuntimeError("indicator_state_init_failed: market_profile requires timeframe")
        if not indicator_id:
            raise RuntimeError("indicator_state_init_failed: market_profile requires indicator_id")
        runtime_scope = (
            f"engine|{strategy_id or 'none'}|{symbol}|{chart_timeframe}|{indicator_id}"
        )
        return {
            "revision": 0,
            "symbol": symbol,
            "chart_timeframe": chart_timeframe,
            "chart_timeframe_seconds": chart_timeframe_seconds,
            "indicator_id": indicator_id,
            "strategy_id": strategy_id,
            "runtime_scope": runtime_scope,
            "active_session": None,
            "active_histogram": {},
            "active_profile": None,
            "completed_profiles": [],
            "known_at": None,
            "formed_at": None,
        }

    def apply_bar(self, state: MutableMapping[str, Any], bar: Any) -> IndicatorStateDelta:
        if not isinstance(bar, Candle):
            raise RuntimeError("indicator_state_apply_failed: market_profile requires Candle input")

        bar_time = bar.time.astimezone(timezone.utc) if bar.time.tzinfo else bar.time.replace(tzinfo=timezone.utc)
        session = bar_time.date().isoformat()

        active_session = state.get("active_session")
        if active_session is None:
            state["active_session"] = session
        elif active_session != session:
            self._finalize_session(state)
            state["active_histogram"] = {}
            state["active_profile"] = None
            state["active_session"] = session

        histogram = state.setdefault("active_histogram", {})
        if not isinstance(histogram, MutableMapping):
            histogram = {}
            state["active_histogram"] = histogram
        _apply_row_to_tpo_histogram(
            histogram=histogram,
            low=float(bar.low),
            high=float(bar.high),
            bin_size=self._param_float("bin_size", 0.25),
            price_precision=self._param_int("price_precision", 4),
        )
        state["active_profile"] = self._build_session_profile(
            session=str(state.get("active_session") or session),
            histogram=histogram,
            formed_at=bar_time,
        )

        state["revision"] = int(state.get("revision") or 0) + 1
        state["known_at"] = bar_time
        state["formed_at"] = bar_time
        return IndicatorStateDelta(changed=True, revision=int(state["revision"]), known_at=bar_time)

    def snapshot(self, state: Mapping[str, Any]) -> IndicatorStateSnapshot:
        known_at = state.get("known_at")
        formed_at = state.get("formed_at")
        if not isinstance(known_at, datetime):
            known_at = datetime.fromtimestamp(0, tz=timezone.utc)
        if not isinstance(formed_at, datetime):
            formed_at = known_at

        profiles = list(state.get("completed_profiles") or [])
        active_profile = state.get("active_profile")
        if isinstance(active_profile, Mapping):
            profiles.append(dict(active_profile))
        source_timeframe = str(self._param("source_timeframe", "30m"))

        payload = {
            "_indicator_id": str(state.get("indicator_id") or ""),
            "_runtime_scope": str(state.get("runtime_scope") or ""),
            "symbol": state.get("symbol"),
            "chart_timeframe": str(state.get("chart_timeframe") or ""),
            "chart_timeframe_seconds": int(state.get("chart_timeframe_seconds") or 0),
            "source_timeframe": source_timeframe,
            "source_timeframe_seconds": int(timeframe_to_seconds(source_timeframe) or 0),
            "active_session": state.get("active_session"),
            "profiles": profiles,
            # Pass through DB params so runtime and QuantLab share exact indicator config.
            "profile_params": dict(self._config.params or {}),
            "overlay_color": self._config.overlay_color,
        }
        return IndicatorStateSnapshot(
            revision=int(state.get("revision") or 0),
            known_at=known_at,
            formed_at=formed_at,
            source_timeframe=source_timeframe,
            payload=payload,
        )

    def _build_session_profile(
        self,
        *,
        session: str,
        histogram: Mapping[float, int],
        formed_at: datetime,
    ) -> Dict[str, Any]:
        value_area = _extract_value_area(histogram, self._param_int("price_precision", 4))
        if value_area is None:
            raise RuntimeError("indicator_state_finalize_failed: market_profile value area empty")
        return {
            "session": session,
            "start": datetime.fromisoformat(f"{session}T00:00:00+00:00"),
            "end": formed_at,
            "VAH": value_area["VAH"],
            "VAL": value_area["VAL"],
            "POC": value_area["POC"],
            "formed_at": formed_at,
            "known_at": formed_at,
            "status": "active",
        }

    def _finalize_session(self, state: MutableMapping[str, Any]) -> None:
        histogram = state.get("active_histogram")
        if not isinstance(histogram, Mapping) or not histogram:
            return

        session = str(state.get("active_session") or "")
        if not session:
            raise RuntimeError("indicator_state_finalize_failed: market_profile active_session missing")
        formed_at = datetime.fromisoformat(f"{session}T23:59:59+00:00")
        profile = self._build_session_profile(session=session, histogram=histogram, formed_at=formed_at)
        profile["status"] = "completed"
        profile["end"] = formed_at

        completed = state.setdefault("completed_profiles", [])
        completed.append(profile)


def _apply_row_to_tpo_histogram(
    *,
    histogram: MutableMapping[float, int],
    low: float,
    high: float,
    bin_size: float,
    price_precision: int,
) -> None:
    if not math.isfinite(low) or not math.isfinite(high):
        return
    if high < low:
        low, high = high, low
    span = max(high - low, 0.0)
    steps = int(math.floor(span / bin_size + 1e-9))
    for idx in range(steps + 1):
        price = low + idx * bin_size
        bucket = round(round(price / bin_size) * bin_size, price_precision)
        histogram[bucket] = int(histogram.get(bucket, 0)) + 1


def _extract_value_area(histogram: Mapping[float, int], price_precision: int) -> Optional[Dict[str, float]]:
    total = sum(histogram.values())
    if total <= 0:
        return None
    ordered = sorted(histogram.items(), key=lambda item: item[1], reverse=True)
    cumulative = 0
    prices: List[float] = []
    for price, count in ordered:
        cumulative += count
        prices.append(float(price))
        if cumulative >= total * 0.7:
            break
    return {
        "VAH": round(max(prices), price_precision),
        "VAL": round(min(prices), price_precision),
        "POC": round(float(ordered[0][0]), price_precision),
    }
