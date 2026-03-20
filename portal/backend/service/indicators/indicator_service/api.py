from __future__ import annotations

import logging
import uuid
from copy import deepcopy
from time import perf_counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.config import IndicatorExecutionContext
from indicators.definition_contract import definition_supports_compute, definition_supports_runtime
from indicators.manifest import serialize_indicator_manifest
from indicators.registry import get_indicator_definition, get_indicator_manifest
from signals.overlays.transformers import apply_overlay_transform
from ..dependency_bindings import assert_indicator_delete_allowed
from .context import IndicatorServiceContext, _context
from .instances import IndicatorInstanceCreator, IndicatorInstanceUpdater
from .runtime_contract import assert_engine_signal_runtime_path
from .runtime_graph import (
    build_runtime_indicator_graph,
    build_runtime_indicator_instance,
)
from .signals import IndicatorSignalExecutor
from .utils import (
    build_meta_from_record,
    load_indicator_record,
    purge_overlay_cache,
)
from ..indicator_factory import INDICATOR_MAP as _INDICATOR_MAP
from ...market import candle_service

logger = logging.getLogger(__name__)

def list_types(*, ctx: IndicatorServiceContext = _context) -> List[str]:
    return list(_INDICATOR_MAP.keys())


def get_type_details(type_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    _ = ctx
    manifest = get_indicator_manifest(type_id)
    definition = get_indicator_definition(type_id)
    details = serialize_indicator_manifest(manifest)
    details["runtime_supported"] = definition_supports_runtime(definition)
    details["compute_supported"] = definition_supports_compute(definition)
    return details


def list_instances_meta(*, ctx: IndicatorServiceContext = _context) -> List[Dict[str, Any]]:
    records = ctx.repository.load()
    if not records:
        return []
    return [build_meta_from_record(record, ctx=ctx) for record in records]


def get_instance_meta(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    record = load_indicator_record(inst_id, ctx=ctx)
    return build_meta_from_record(record, ctx=ctx)


def list_indicator_strategies(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> List[Dict[str, Any]]:
    return ctx.repository.strategies_for_indicator(inst_id)


def delete_instance(
    inst_id: str,
    *,
    deleting_ids: Sequence[str] | None = None,
    ctx: IndicatorServiceContext = _context,
) -> None:
    load_indicator_record(inst_id, ctx=ctx)
    assert_indicator_delete_allowed(
        indicator_id=inst_id,
        deleting_ids=deleting_ids,
        ctx=ctx,
    )
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
    deleting_ids = [str(inst_id or "").strip() for inst_id in inst_ids if str(inst_id or "").strip()]
    for inst_id in deleting_ids:
        load_indicator_record(inst_id, ctx=ctx)
        assert_indicator_delete_allowed(
            indicator_id=inst_id,
            deleting_ids=deleting_ids,
            ctx=ctx,
        )
    removed = 0
    for inst_id in deleting_ids:
        try:
            delete_instance(inst_id, deleting_ids=deleting_ids, ctx=ctx)
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
    dependencies: Optional[Sequence[Dict[str, Any]]] = None,
    color: Optional[str] = None,
    *,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    creator = IndicatorInstanceCreator(ctx)
    return creator.create(type_str, name, params, dependencies, color)


def update_instance(
    inst_id: str,
    type_str: str,
    params: Dict[str, Any],
    name: Optional[str],
    dependencies: Optional[Sequence[Dict[str, Any]]] = None,
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
        dependencies,
        color=color,
        color_provided=color_provided,
    )


def _build_runtime_candles(df: pd.DataFrame) -> List[Candle]:
    import pandas as pd

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


def _collect_runtime_overlays(
    overlays: Mapping[str, Any],
    *,
    current_epoch: int,
) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    for overlay_key in sorted(overlays.keys()):
        runtime_overlay = overlays.get(overlay_key)
        if runtime_overlay is None or not getattr(runtime_overlay, "ready", False):
            continue
        indicator_id, _, overlay_name = str(overlay_key).partition(".")
        payload = dict(getattr(runtime_overlay, "value", {}) or {})
        transformed = apply_overlay_transform(payload, current_epoch=current_epoch)
        if transformed is None:
            continue
        payload = dict(transformed)
        payload.setdefault("overlay_id", overlay_key)
        payload.setdefault("indicator_id", indicator_id)
        payload.setdefault("overlay_name", overlay_name)
        collected.append(payload)
    return collected


def _configure_replay_window(indicators: Sequence[Any], *, history_bars: int) -> None:
    for indicator in indicators:
        configure = getattr(indicator, "configure_replay_window", None)
        if callable(configure):
            configure(history_bars=history_bars)


def _resolve_logged_source_timeframe(meta: Mapping[str, Any], interval: str) -> str:
    indicator_type = str(meta.get("type") or "").strip()
    if not indicator_type:
        return str(interval or "")
    manifest = get_indicator_manifest(indicator_type)
    if not manifest.runtime_inputs:
        return str(interval or "")
    runtime_input = manifest.runtime_inputs[0]
    source_timeframe = str(runtime_input.source_timeframe or "").strip()
    return source_timeframe or str(interval or "")


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
    t0 = perf_counter()
    meta = get_instance_meta(inst_id, ctx=ctx)
    if not bool(meta.get("runtime_supported")):
        raise RuntimeError(f"Indicator is not runtime-supported: {inst_id}")
    logged_source_timeframe = _resolve_logged_source_timeframe(meta, interval)

    resolved_symbol = str(symbol or "").strip()
    resolved_datasource = str(datasource or meta.get("datasource") or "").strip()
    resolved_exchange = exchange or meta.get("exchange")
    execution_context = IndicatorExecutionContext(
        symbol=resolved_symbol,
        start=start,
        end=end,
        interval=interval,
        datasource=resolved_datasource or None,
        exchange=resolved_exchange,
        instrument_id=instrument_id,
    )
    t_graph_start = perf_counter()

    _, indicators = build_runtime_indicator_graph(
        [inst_id],
        execution_context=execution_context,
        ctx=ctx,
        preloaded_metas={inst_id: meta},
    )
    engine = IndicatorExecutionEngine(indicators)
    graph_duration_ms = (perf_counter() - t_graph_start) * 1000.0

    t_fetch_start = perf_counter()
    if instrument_id:
        df = candle_service.fetch_ohlcv_by_instrument(
            instrument_id,
            start,
            end,
            interval,
        )
    else:
        if not resolved_symbol or not resolved_datasource:
            raise ValueError("Indicator overlay preview requires symbol and datasource.")
        df = candle_service.fetch_ohlcv(
            resolved_symbol,
            start,
            end,
            interval,
            datasource=resolved_datasource,
            exchange=resolved_exchange,
        )
    fetch_duration_ms = (perf_counter() - t_fetch_start) * 1000.0

    t_candles_start = perf_counter()
    candles = _build_runtime_candles(df)
    candle_build_duration_ms = (perf_counter() - t_candles_start) * 1000.0
    if candles:
        _configure_replay_window(indicators, history_bars=len(candles))
    if not candles:
        logger.info(
            "event=indicator_overlay_execute_complete indicator_id=%s indicator_type=%s symbol=%s timeframe=%s source_timeframe=%s bars=0 overlays=0 duration_total_ms=%.3f duration_graph_ms=%.3f duration_fetch_ms=%.3f duration_candle_build_ms=%.3f duration_engine_ms=0.000 duration_overlay_collect_ms=0.000",
            inst_id,
            meta.get("type"),
            resolved_symbol,
            interval,
            logged_source_timeframe,
            (perf_counter() - t0) * 1000.0,
            graph_duration_ms,
            fetch_duration_ms,
            candle_build_duration_ms,
        )
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
    t_engine_start = perf_counter()
    last_index = len(candles) - 1
    for index, candle in enumerate(candles):
        last_frame = engine.step(
            bar=candle,
            bar_time=candle.time,
            include_overlays=index == last_index,
        )
    engine_duration_ms = (perf_counter() - t_engine_start) * 1000.0

    t_collect_start = perf_counter()
    overlays = (
        [
            overlay
            for overlay in _collect_runtime_overlays(
            last_frame.overlays,
            current_epoch=int(candles[-1].time.timestamp()),
        )
            if str(overlay.get("indicator_id") or "") == inst_id
        ]
        if last_frame is not None
        else []
    )
    collect_duration_ms = (perf_counter() - t_collect_start) * 1000.0
    total_duration_ms = (perf_counter() - t0) * 1000.0
    logger.info(
        "event=indicator_overlay_execute_complete indicator_id=%s indicator_type=%s symbol=%s timeframe=%s source_timeframe=%s bars=%s overlays=%s duration_total_ms=%.3f duration_graph_ms=%.3f duration_fetch_ms=%.3f duration_candle_build_ms=%.3f duration_engine_ms=%.3f duration_overlay_collect_ms=%.3f",
        inst_id,
        meta.get("type"),
        resolved_symbol,
        interval,
        logged_source_timeframe,
        len(candles),
        len(overlays),
        total_duration_ms,
        graph_duration_ms,
        fetch_duration_ms,
        candle_build_duration_ms,
        engine_duration_ms,
        collect_duration_ms,
    )
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
        dependencies: Optional[Sequence[Dict[str, Any]]] = None,
        color: Optional[str] = None,
    ) -> Dict[str, Any]:
        return create_instance(type_str, name, params, dependencies, color, ctx=self._ctx)

    def update_instance(
        self,
        inst_id: str,
        type_str: str,
        params: Dict[str, Any],
        name: Optional[str],
        dependencies: Optional[Sequence[Dict[str, Any]]] = None,
        *,
        color: Optional[str] = None,
        color_provided: bool = False,
    ) -> Dict[str, Any]:
        return update_instance(
            inst_id,
            type_str,
            params,
            name,
            dependencies,
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
