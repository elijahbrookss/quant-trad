from __future__ import annotations

import inspect
import logging
import uuid
from copy import deepcopy
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .context import IndicatorServiceContext, _context
from .instances import IndicatorInstanceCreator, IndicatorInstanceUpdater
from .overlays import IndicatorOverlayBuilder
from .signals import BreakoutCacheContext, IndicatorSignalExecutor
from .utils import (
    build_indicator_instance,
    build_meta_from_record,
    ensure_color,
    load_indicator_record,
    purge_breakout_cache,
    purge_overlay_cache,
)
from ..indicator_factory import INDICATOR_MAP as _INDICATOR_MAP

logger = logging.getLogger(__name__)


def list_types(*, ctx: IndicatorServiceContext = _context) -> List[str]:
    return list(_INDICATOR_MAP.keys())


def get_type_details(type_id: str, *, ctx: IndicatorServiceContext = _context) -> Dict[str, Any]:
    Cls = _INDICATOR_MAP.get(type_id)
    if not Cls:
        raise KeyError(f"Unknown indicator type: {type_id}")

    sig = inspect.signature(Cls.__init__)
    required, defaults, field_types = [], {}, {}
    for name, param in sig.parameters.items():
        if name in ("self", "df"):
            continue
        anno = param.annotation
        if anno is inspect._empty:
            tname = "Any"
        elif isinstance(anno, type):
            tname = anno.__name__
        else:
            tname = str(anno)
        field_types[name] = tname
        if param.default is inspect._empty:
            required.append(name)
        else:
            defaults[name] = param.default
    indicator_name = getattr(Cls, "NAME", type_id)

    details = {
        "id": type_id,
        "name": indicator_name,
        "required_params": required,
        "default_params": defaults,
        "field_types": field_types,
    }

    rule_meta = ctx.signal_runner.build_signal_catalog(indicator_name)
    if rule_meta:
        details["signal_rules"] = rule_meta

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


def delete_instance(inst_id: str, *, ctx: IndicatorServiceContext = _context) -> None:
    load_indicator_record(inst_id, ctx=ctx)
    # Cache removed: no eviction needed
    purge_breakout_cache(inst_id, ctx=ctx)
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


def overlays_for_instance(
    inst_id: str,
    start: str,
    end: str,
    interval: str,
    symbol: Optional[str] = None,
    datasource: Optional[str] = None,
    exchange: Optional[str] = None,
    *,
    overlay_options: Optional[Mapping[str, Any]] = None,
    ctx: IndicatorServiceContext = _context,
) -> Dict[str, Any]:
    builder = IndicatorOverlayBuilder(ctx)
    return builder.build(
        inst_id,
        start,
        end,
        interval,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        overlay_options=overlay_options,
    )


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
    return executor.execute(
        inst_id,
        start,
        end,
        interval,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        config=config,
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
    "BreakoutCacheContext",
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
    "list_indicator_strategies",
    "overlays_for_instance",
    "generate_signals_for_instance",
    "get_type_details",
    "list_types",
    "default_service",
]
