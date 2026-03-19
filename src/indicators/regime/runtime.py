"""Native regime runtime indicator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    Indicator,
    IndicatorManifest,
    OverlayDefinition,
    OutputDefinition,
    OutputRef,
    RuntimeOverlay,
    RuntimeOutput,
)
from signals.overlays.schema import build_overlay

from .config import RegimeStabilizerConfig
from .engine import RegimeEngineV1
from .overlays import build_regime_overlays
from .stabilizer import RegimeStabilizer


def resolve_regime_dependency(
    *,
    indicator_id: str,
    meta: Mapping[str, Any],
    strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
) -> str:
    raw_dependencies = meta.get("dependencies")
    if isinstance(raw_dependencies, (list, tuple)):
        refs = [
            item
            for item in raw_dependencies
            if isinstance(item, Mapping)
            and str(item.get("output_name") or "").strip() == "candle_stats"
            and str(item.get("indicator_id") or "").strip()
        ]
        if len(refs) == 1:
            return str(refs[0].get("indicator_id") or "").strip()
        if len(refs) > 1:
            raise RuntimeError(
                f"regime_dependency_invalid: multiple candle_stats dependencies indicator_id={indicator_id}"
            )
    candidates = [
        attached_id
        for attached_id, attached_meta in strategy_indicator_metas.items()
        if attached_id != indicator_id
        and str((attached_meta or {}).get("type") or "").strip().lower() == "candle_stats"
    ]
    if len(candidates) == 1:
        return str(candidates[0])
    if not candidates:
        raise RuntimeError(
            f"regime_dependency_missing: no candle_stats indicator attached for indicator_id={indicator_id}"
        )
    raise RuntimeError(
        "regime_dependency_ambiguous: multiple candle_stats indicators attached "
        f"indicator_id={indicator_id} candidates={sorted(candidates)}"
    )


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
        self.manifest = IndicatorManifest(
            id=indicator_id,
            version=version,
            dependencies=(OutputRef(indicator_id=candle_stats_indicator_id, output_name="candle_stats"),),
            outputs=(OutputDefinition(name="market_regime", type="context"),),
            overlays=(
                OverlayDefinition(name="regime", overlay_type="regime_overlay"),
                OverlayDefinition(name="regime_markers", overlay_type="regime_markers"),
            ),
        )
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
        self._overlays = {
            "regime": RuntimeOverlay(bar_time=datetime.min, ready=False, value={}),
            "regime_markers": RuntimeOverlay(bar_time=datetime.min, ready=False, value={}),
        }
        self._candles: list[Candle] = []
        self._regime_rows: dict[datetime, Mapping[str, Any]] = {}
        self._timeframe_seconds = 60

    def apply_bar(self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("regime_apply_failed: Candle input required")
        dependency_ref = self.manifest.dependencies[0]
        dependency_output = inputs.get(dependency_ref)
        if dependency_output is None:
            raise RuntimeError(
                "regime_apply_failed: dependency output missing "
                f"dependency={dependency_ref.indicator_id}.{dependency_ref.output_name}"
            )
        if not dependency_output.ready:
            self._output = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            self._overlays = {
                "regime": RuntimeOverlay(bar_time=bar.time, ready=False, value={}),
                "regime_markers": RuntimeOverlay(bar_time=bar.time, ready=False, value={}),
            }
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
        self._overlays = {
            "regime": RuntimeOverlay(
                bar_time=bar.time,
                ready=True,
                value=dict(regime_overlay),
            ),
            "regime_markers": RuntimeOverlay(
                bar_time=bar.time,
                ready=True,
                value=dict(marker_overlay),
            ),
        }

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {"market_regime": self._output}

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return dict(self._overlays)


__all__ = ["TypedRegimeIndicator", "resolve_regime_dependency"]
