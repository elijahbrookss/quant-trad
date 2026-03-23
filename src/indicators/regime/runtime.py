"""Native regime runtime indicator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    Indicator,
    OutputRef,
    RuntimeOverlay,
    RuntimeOutput,
)
from indicators.manifest import build_runtime_spec
from overlays.schema import build_overlay

from .config import RegimeStabilizerConfig
from .engine import RegimeEngineV1
from .manifest import MANIFEST
from .overlays import build_regime_overlays
from .stabilizer import RegimeStabilizer


def resolve_regime_dependency(
    *,
    indicator_id: str,
    meta: Mapping[str, Any],
    strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
) -> str:
    _ = strategy_indicator_metas
    raw_dependencies = meta.get("dependencies")
    if not isinstance(raw_dependencies, (list, tuple)):
        raise RuntimeError(
            "regime_dependency_missing: explicit candle_stats dependency binding required "
            f"indicator_id={indicator_id}"
        )
    refs = [
        item
        for item in raw_dependencies
        if isinstance(item, Mapping)
        and str(item.get("output_name") or "").strip() == "candle_stats"
        and str(item.get("indicator_id") or "").strip()
    ]
    if len(refs) != 1:
        raise RuntimeError(
            "regime_dependency_invalid: exactly one candle_stats dependency binding required "
            f"indicator_id={indicator_id}"
        )
    return str(refs[0].get("indicator_id") or "").strip()


def _as_positive_int(name: str, value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"regime_config_invalid: {name} must be int") from exc
    if parsed <= 0:
        raise RuntimeError(f"regime_config_invalid: {name} must be > 0")
    return parsed


def _as_float(name: str, value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"regime_config_invalid: {name} must be numeric") from exc


class TypedRegimeIndicator(Indicator):
    def __init__(
        self,
        *,
        indicator_id: str,
        version: str,
        params: Mapping[str, Any],
        candle_stats_indicator_id: str,
    ) -> None:
        dependency_ref = OutputRef(
            indicator_id=candle_stats_indicator_id,
            output_name="candle_stats",
        )
        self.runtime_spec = build_runtime_spec(
            MANIFEST,
            instance_id=indicator_id,
            version=version,
            dependencies=(dependency_ref,),
        )
        self._dependency_ref = dependency_ref
        config = RegimeStabilizerConfig(
            min_confidence=_as_float("min_confidence", params.get("min_confidence")),
            structure_min_confidence=_as_float(
                "structure_min_confidence",
                params.get("structure_min_confidence"),
            ),
            confirm_bars={
                "structure": _as_positive_int(
                    "structure_confirm_bars",
                    params.get("structure_confirm_bars"),
                ),
                "volatility": _as_positive_int(
                    "volatility_confirm_bars",
                    params.get("volatility_confirm_bars"),
                ),
                "liquidity": _as_positive_int(
                    "liquidity_confirm_bars",
                    params.get("liquidity_confirm_bars"),
                ),
                "expansion": _as_positive_int(
                    "expansion_confirm_bars",
                    params.get("expansion_confirm_bars"),
                ),
            },
            smoothing_alpha=_as_float("smoothing_alpha", params.get("smoothing_alpha")),
        )
        self._engine = RegimeEngineV1()
        self._stabilizer = RegimeStabilizer(config)
        self._output = RuntimeOutput(bar_time=datetime.min, ready=False, value={})
        self._candles: list[Candle] = []
        self._regime_rows: dict[datetime, Mapping[str, Any]] = {}
        self._timeframe_seconds = 60
        self._current_bar_time = datetime.min
        self._overlay_ready = False
        self._overlay_cache_bar_time: datetime | None = None
        self._overlay_cache: dict[str, RuntimeOverlay] | None = None

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("regime_apply_failed: Candle input required")
        self._current_bar_time = bar.time
        dependency_output = inputs.get(self._dependency_ref)
        if dependency_output is None:
            raise RuntimeError(
                "regime_apply_failed: dependency output missing "
                f"dependency={self._dependency_ref.indicator_id}.{self._dependency_ref.output_name}"
            )
        if not dependency_output.ready:
            self._overlay_ready = False
            self._overlay_cache = None
            self._overlay_cache_bar_time = None
            self._output = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            return

        if self._candles:
            delta_seconds = int((bar.time - self._candles[-1].time).total_seconds())
            if delta_seconds > 0:
                self._timeframe_seconds = delta_seconds
        self._candles.append(bar)
        classified = self._engine.classify(
            candle={
                "time": bar.time,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            },
            stats=dict(dependency_output.value),
        ).as_dict()
        stabilized = self._stabilizer.stabilize(classified, bar_time=bar.time)
        self._regime_rows[bar.time.replace(tzinfo=None) if bar.time.tzinfo is not None else bar.time] = dict(stabilized)
        structure = stabilized.get("structure") or {}
        volatility = stabilized.get("volatility") or {}
        liquidity = stabilized.get("liquidity") or {}
        expansion = stabilized.get("expansion") or {}
        self._output = RuntimeOutput(
            bar_time=bar.time,
            ready=True,
            value={
                "state_key": str(structure.get("state") or ""),
                "fields": {
                    "regime_key": str(stabilized.get("regime_key") or ""),
                    "volatility_state": str(volatility.get("state") or ""),
                    "liquidity_state": str(liquidity.get("state") or ""),
                    "expansion_state": str(expansion.get("state") or ""),
                },
            },
        )
        self._overlay_ready = True
        self._overlay_cache = None
        self._overlay_cache_bar_time = None

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {"market_regime": self._output}

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        if not self._overlay_ready:
            return {
                "regime": RuntimeOverlay(bar_time=self._current_bar_time, ready=False, value={}),
                "regime_markers": RuntimeOverlay(bar_time=self._current_bar_time, ready=False, value={}),
            }
        if self._overlay_cache is None or self._overlay_cache_bar_time != self._current_bar_time:
            built = build_regime_overlays(
                candles=list(self._candles),
                regime_rows=self._regime_rows,
                timeframe_seconds=max(int(self._timeframe_seconds), 1),
                regime_version="v1",
                include_change_markers=True,
                include_marker_overlay=True,
            )
            regime_overlay = next(
                (
                    dict(overlay)
                    for overlay in built
                    if isinstance(overlay, Mapping) and str(overlay.get("type") or "") == "regime_overlay"
                ),
                build_overlay("regime_overlay", {"boxes": [], "segments": [], "summary": {}}),
            )
            marker_overlay = next(
                (
                    dict(overlay)
                    for overlay in built
                    if isinstance(overlay, Mapping) and str(overlay.get("type") or "") == "regime_markers"
                ),
                build_overlay("regime_markers", {"markers": []}),
            )
            self._overlay_cache = {
                "regime": RuntimeOverlay(
                    bar_time=self._current_bar_time,
                    ready=True,
                    value=dict(regime_overlay),
                ),
                "regime_markers": RuntimeOverlay(
                    bar_time=self._current_bar_time,
                    ready=True,
                    value=dict(marker_overlay),
                ),
            }
            self._overlay_cache_bar_time = self._current_bar_time
        return dict(self._overlay_cache)


__all__ = ["TypedRegimeIndicator", "resolve_regime_dependency"]
