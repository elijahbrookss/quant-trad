from __future__ import annotations

import logging
import uuid
from copy import deepcopy
from typing import Any, Dict, List, Mapping, Optional, Sequence

import pandas as pd

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from .context import IndicatorServiceContext, _context
from .instances import IndicatorInstanceCreator, IndicatorInstanceUpdater
from .runtime_contract import assert_engine_signal_runtime_path
from .signals import IndicatorSignalExecutor
from .utils import (
    build_indicator_instance,
    build_meta_from_record,
    ensure_color,
    load_indicator_record,
    purge_overlay_cache,
)
from ..indicator_factory import (
    INDICATOR_MAP as _INDICATOR_MAP,
    indicator_default_params,
    indicator_field_types,
    indicator_output_catalog,
    indicator_overlay_catalog,
    indicator_required_params,
    resolve_indicator_params,
    runtime_indicator_builder_for_type,
)
from ...market import candle_service

logger = logging.getLogger(__name__)

_RUNTIME_CONTEXT_KEYS = {
    "symbol",
    "interval",
    "start",
    "end",
    "timeframe",
    "datasource",
    "exchange",
    "provider_id",
    "venue_id",
    "instrument_id",
    "bot_id",
    "strategy_id",
    "bot_mode",
    "run_id",
}


def list_types(*, ctx: IndicatorServiceContext = _context) -> List[str]:
    return list(_INDICATOR_MAP.keys())


def get_type_details(type_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    Cls = _INDICATOR_MAP.get(type_id)
    if not Cls:
        raise KeyError(f"Unknown indicator type: {type_id}")

    indicator_name = getattr(Cls, "NAME", type_id)
    required = list(indicator_required_params(Cls))
    defaults = dict(indicator_default_params(Cls))
    field_types = dict(indicator_field_types(Cls))

    # Remove runtime-only context fields that should not be user-configurable
    required = [key for key in required if key not in _RUNTIME_CONTEXT_KEYS]
    defaults = {k: v for k, v in defaults.items() if k not in _RUNTIME_CONTEXT_KEYS}
    field_types = {k: v for k, v in field_types.items() if k not in _RUNTIME_CONTEXT_KEYS}

    details = {
        "id": type_id,
        "name": indicator_name,
        "required_params": required,
        "default_params": defaults,
        "field_types": field_types,
        "typed_outputs": indicator_output_catalog(Cls),
        "overlay_outputs": indicator_overlay_catalog(Cls),
    }
    runtime_input_specs = [
        {
            "source_timeframe": spec.source_timeframe,
            "source_timeframe_param": spec.source_timeframe_param,
            "lookback_bars": spec.lookback_bars,
            "lookback_bars_param": spec.lookback_bars_param,
            "lookback_days": spec.lookback_days,
            "lookback_days_param": spec.lookback_days_param,
            "session_scope": spec.session_scope,
            "alignment": spec.alignment,
            "normalization": spec.normalization,
        }
        for spec in ctx.factory.get_runtime_input_specs_for_type(type_id)
    ]
    if runtime_input_specs:
        details["runtime_input_specs"] = runtime_input_specs

    return details


def list_instances_meta(*, ctx: IndicatorServiceContext = _context) -> List[Dict[str, Any]]:
    records = ctx.repository.load()
    if not records:
        return []
    return [build_meta_from_record(record, ctx=ctx) for record in records]


def get_instance_meta(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    record = load_indicator_record(inst_id, ctx=ctx)
    return build_meta_from_record(record, ctx=ctx)


def build_runtime_indicator_instance(
    indicator_id: str,
    *,
    meta: Mapping[str, Any],
    strategy_indicator_metas: Mapping[str, Mapping[str, Any]] | None = None,
) -> Any:
    indicator_type = str(meta.get("type") or "").strip()
    indicator_cls = _INDICATOR_MAP.get(indicator_type)
    if indicator_cls is None:
        raise KeyError(f"Unknown indicator type: {indicator_type}")
    builder = runtime_indicator_builder_for_type(indicator_type)
    resolved_params = resolve_indicator_params(indicator_cls, meta.get("params"))
    return builder(
        indicator_id=indicator_id,
        meta=meta,
        resolved_params=resolved_params,
        strategy_indicator_metas=strategy_indicator_metas or {},
    )


def list_indicator_strategies(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> List[Dict[str, Any]]:
    return ctx.repository.strategies_for_indicator(inst_id)


def delete_instance(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    load_indicator_record(inst_id, ctx=ctx)
    # Cache removed: no eviction needed
    purge_overlay_cache(inst_id, ctx=ctx)
    logger.info("event=indicator_delete indicator_id=%s", inst_id)
    ctx.repository.delete(inst_id)


def duplicate_instance(inst_id: str, name: Optional[str] = None, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    base_record = load_indicator_record(inst_id, ctx=ctx)
    clone_id = str(uuid.uuid4())
    clone_record = deepcopy(base_record)
    clone_record["id"] = clone_id
    clone_record["name"] = name or f"{base_record.get('name') or base_record.get('type')} Copy"
    ctx.repository.upsert(clone_record)
    refreshed = ctx.repository.get(clone_id)
    persisted = build_meta_from_record(refreshed, ctx=ctx) if refreshed else build_meta_from_record(clone_record, ctx=ctx)
    # Cache removed: instances are now built fresh from DB on each access
    return persisted


def set_instance_enabled(inst_id: str, enabled: bool, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    record = load_indicator_record(inst_id, ctx=ctx)
    updated = deepcopy(record)
    updated["enabled"] = bool(enabled)
    ctx.repository.upsert(updated)
    refreshed = ctx.repository.get(inst_id)
    persisted = build_meta_from_record(refreshed, ctx=ctx) if refreshed else build_meta_from_record(updated, ctx=ctx)
    # Cache removed: no eviction needed
    return persisted


def bulk_set_enabled(inst_ids: Sequence[str], enabled: bool, *, ctx: IndicatorServiceContext = _context) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for inst_id in inst_ids:
        try:
            results.append(set_instance_enabled(inst_id, enabled, ctx=ctx))
        except KeyError:
            continue
    return results


def bulk_delete_instances(inst_ids: Sequence[str], *, ctx: IndicatorServiceContext = _context) -> int:
    removed = 0
    for inst_id in inst_ids:
        try:
            delete_instance(inst_id, ctx=ctx)
            removed += 1
        except KeyError:
            continue
    return removed


def clear_overlay_cache(*, ctx: IndicatorServiceContext = _context) -> None:
    ctx.overlay_cache.clear()
    logger.info("event=indicator_overlay_cache_cleared")


def create_instance(
    type_str: str,
    name: Optional[str],
    params: Dict[str, Any],
    color: Optional[str] = None,
    *,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    creator = IndicatorInstanceCreator(ctx)
    return creator.create(type_str, name, params, color)


def update_instance(
    inst_id: str,
    type_str: str,
    params: Dict[str, Any],
    name: Optional[str],
    *,
    color: Optional[str] = None,
    color_provided: bool = False,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    updater = IndicatorInstanceUpdater(ctx)
    return updater.update(
        inst_id,
        type_str,
        params,
        name,
        color=color,
        color_provided=color_provided,
    )


def _build_runtime_candles(df: pd.DataFrame) -> List[Candle]:
    if df is None or getattr(df, "empty", False):
        return []
    candles: List[Candle] = []
    timestamps = pd.to_datetime(df.index, utc=True)
    for timestamp, (_, row) in zip(timestamps, df.iterrows()):
        candles.append(
            Candle(
                time=timestamp.to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]) if row.get("volume") is not None else None,
            )
        )
    return candles


def _collect_runtime_overlays(overlays: Mapping[str, Any]) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for overlay_key in sorted(overlays.keys()):
        runtime_overlay = overlays.get(overlay_key)
        if runtime_overlay is None or not getattr(runtime_overlay, "ready", False):
            continue
        indicator_id, _, overlay_name = str(overlay_key).partition(".")
        payload = dict(getattr(runtime_overlay, "value", {}) or {})
        payload.setdefault("overlay_id", overlay_key)
        payload.setdefault("indicator_id", indicator_id)
        payload.setdefault("overlay_name", overlay_name)
        collected.append(payload)
    return collected


def overlays_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    instrument_id: Optional[str] = None,
    *,
    overlay_options: Optional[Mapping[str, Any]] = None,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    _ = overlay_options
    meta = get_instance_meta(inst_id, ctx=ctx)
    if not bool(meta.get("runtime_supported")):
        raise RuntimeError(f"Indicator is not runtime-supported: {inst_id}")

    indicator = build_runtime_indicator_instance(inst_id, meta=meta, strategy_indicator_metas={inst_id: meta})
    engine = IndicatorExecutionEngine([indicator])

    if instrument_id:
        df = candle_service.fetch_ohlcv_by_instrument(
            instrument_id,
            start,
            end,
            interval,
            schedule_stats=False,
        )
    else:
        resolved_symbol = str(symbol or meta.get("params", {}).get("symbol") or "").strip()
        resolved_datasource = str(datasource or meta.get("datasource") or meta.get("params", {}).get("datasource") or "").strip()
        resolved_exchange = exchange or meta.get("exchange") or meta.get("params", {}).get("exchange")
        if not resolved_symbol or not resolved_datasource:
            raise ValueError("Indicator overlay preview requires symbol and datasource.")
        df = candle_service.fetch_ohlcv(
            resolved_symbol,
            start,
            end,
            interval,
            datasource=resolved_datasource,
            exchange=resolved_exchange,
            schedule_stats=False,
        )

    candles = _build_runtime_candles(df)
    if not candles:
        return {
            "indicator_id": inst_id,
            "runtime_path": "typed_indicator_engine_v1",
            "window": {
                "start": start,
                "end": end,
                "interval": interval,
            },
            "overlays": [],
        }

    last_frame = None
    for candle in candles:
        last_frame = engine.step(bar=candle, bar_time=candle.time)

    overlays = _collect_runtime_overlays(last_frame.overlays if last_frame is not None else {})
    return {
        "indicator_id": inst_id,
        "runtime_path": "typed_indicator_engine_v1",
        "window": {
            "start": start,
            "end": end,
            "interval": interval,
        },
        "overlays": overlays,
    }


def generate_signals_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    *,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    executor = IndicatorSignalExecutor(ctx)
    payload = executor.execute(
        inst_id,
        start,
        end,
        interval,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        config=config,
    )
    assert_engine_signal_runtime_path(
        payload,
        context="indicator_signal_execute",
        indicator_id=inst_id,
    )
    return payload


def runtime_input_plan_for_instance(
    inst_id: str,
    *,
    strategy_interval: str,
    start: str,
    end: str,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    record = load_indicator_record(inst_id, ctx=ctx)
    meta = build_meta_from_record(record, ctx=ctx)
    return ctx.factory.build_runtime_input_plan(
        meta,
        strategy_interval=strategy_interval,
        start=start,
        end=end,
    )


class IndicatorService:
    """Facade exposing indicator operations with injectable dependencies."""

    def __init__(self, ctx: Optional[IndicatorServiceContext] = None) -> None:
        self._ctx = ctx or IndicatorServiceContext.default()

    def list_types(self) -> List[str]:
        return list_types(ctx=self._ctx)

    def get_type_details(self, type_id: str) -> Dict[str, Any]:
        return get_type_details(type_id, ctx=self._ctx)

    def list_instances_meta(self) -> List[Dict[str, Any]]:
        return list_instances_meta(ctx=self._ctx)

    def get_instance_meta(self, inst_id: str) -> Dict[str, Any]:
        return get_instance_meta(inst_id, ctx=self._ctx)

    def list_indicator_strategies(self, inst_id: str) -> List[Dict[str, Any]]:
        return list_indicator_strategies(inst_id, ctx=self._ctx)

    def create_instance(
        self,
        type_str: str,
        name: Optional[str],
        params: Dict[str, Any],
        color: Optional[str] = None,
    ) -> Dict[str, Any]:
        return create_instance(type_str, name, params, color, ctx=self._ctx)

    def update_instance(
        self,
        inst_id: str,
        type_str: str,
        params: Dict[str, Any],
        name: Optional[str],
        *,
        color: Optional[str] = None,
        color_provided: bool = False,
    ) -> Dict[str, Any]:
        return update_instance(
            inst_id,
            type_str,
            params,
            name,
            color=color,
            color_provided=color_provided,
            ctx=self._ctx,
        )

    def delete_instance(self, inst_id: str) -> None:
        return delete_instance(inst_id, ctx=self._ctx)

    def duplicate_instance(self, inst_id: str, name: Optional[str] = None) -> Dict[str, Any]:
        return duplicate_instance(inst_id, name, ctx=self._ctx)

    def set_instance_enabled(self, inst_id: str, enabled: bool) -> Dict[str, Any]:
        return set_instance_enabled(inst_id, enabled, ctx=self._ctx)

    def bulk_set_enabled(self, inst_ids: Sequence[str], enabled: bool) -> List[Dict[str, Any]]:
        return bulk_set_enabled(inst_ids, enabled, ctx=self._ctx)

    def bulk_delete_instances(self, inst_ids: Sequence[str]) -> int:
        return bulk_delete_instances(inst_ids, ctx=self._ctx)

    def overlays_for_instance(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        *,
        overlay_options: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        return overlays_for_instance(
            inst_id,
            start,
            end,
            interval,
            symbol,
            datasource,
            exchange,
            overlay_options=overlay_options,
            ctx=self._ctx,
        )

    def generate_signals_for_instance(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return generate_signals_for_instance(
            inst_id,
            start,
            end,
            interval,
            symbol,
            datasource,
            exchange,
            config,
            ctx=self._ctx,
        )


default_service = IndicatorService(_context)

__all__ = [
    "IndicatorService",
    "IndicatorServiceContext",
    "create_instance",
    "update_instance",
    "delete_instance",
    "duplicate_instance",
    "set_instance_enabled",
    "bulk_set_enabled",
    "bulk_delete_instances",
    "clear_overlay_cache",
    "list_instances_meta",
    "get_instance_meta",
    "build_runtime_indicator_instance",
    "list_indicator_strategies",
    "overlays_for_instance",
    "generate_signals_for_instance",
    "get_type_details",
    "list_types",
    "default_service",
]
