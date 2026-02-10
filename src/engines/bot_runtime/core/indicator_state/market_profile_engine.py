"""Market Profile implementation of the IndicatorStateEngine contract."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from engines.bot_runtime.core.domain import Candle

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
        return {
            "revision": 0,
            "symbol": symbol,
            "active_session": None,
            "active_rows": [],
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
            state["active_rows"] = []
            state["active_profile"] = None
            state["active_session"] = session

        state.setdefault("active_rows", []).append({"low": float(bar.low), "high": float(bar.high)})
        state["active_profile"] = self._build_session_profile(
            session=str(state.get("active_session") or session),
            rows=list(state.get("active_rows") or []),
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

        payload = {
            "symbol": state.get("symbol"),
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
            source_timeframe=str(self._param("source_timeframe", "30m")),
            payload=payload,
        )

    def _build_session_profile(
        self,
        *,
        session: str,
        rows: List[Mapping[str, float]],
        formed_at: datetime,
    ) -> Dict[str, Any]:
        tpo_histogram = _build_tpo_histogram(
            rows=rows,
            bin_size=self._param_float("bin_size", 0.25),
            price_precision=self._param_int("price_precision", 4),
        )
        value_area = _extract_value_area(tpo_histogram, self._param_int("price_precision", 4))
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
        rows = list(state.get("active_rows") or [])
        if not rows:
            return

        session = str(state.get("active_session") or "")
        if not session:
            raise RuntimeError("indicator_state_finalize_failed: market_profile active_session missing")
        formed_at = datetime.fromisoformat(f"{session}T23:59:59+00:00")
        profile = self._build_session_profile(session=session, rows=rows, formed_at=formed_at)
        profile["status"] = "completed"
        profile["end"] = formed_at

        completed = state.setdefault("completed_profiles", [])
        completed.append(profile)


def _build_tpo_histogram(*, rows: List[Mapping[str, float]], bin_size: float, price_precision: int) -> Dict[float, int]:
    histogram: Dict[float, int] = {}
    for row in rows:
        low = float(row["low"])
        high = float(row["high"])
        if not math.isfinite(low) or not math.isfinite(high):
            continue
        if high < low:
            low, high = high, low
        span = max(high - low, 0.0)
        steps = int(math.floor(span / bin_size + 1e-9))
        for idx in range(steps + 1):
            price = low + idx * bin_size
            bucket = round(round(price / bin_size) * bin_size, price_precision)
            histogram[bucket] = histogram.get(bucket, 0) + 1
    return histogram


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
