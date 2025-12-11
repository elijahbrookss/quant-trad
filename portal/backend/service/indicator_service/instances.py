from __future__ import annotations

import inspect
import logging
import uuid
from typing import Any, Dict, Optional, Tuple

from data_providers.base_provider import DataSource
from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator

from ..indicator_factory import INDICATOR_MAP as _INDICATOR_MAP
from .context import IndicatorServiceContext, _context
from .utils import (
    build_meta_from_record,
    ensure_color,
    extract_ctor_params,
    load_indicator_record,
    normalize_color,
    pull_datasource_exchange,
    refresh_strategy_links,
    resolve_data_provider,
    scrub_runtime_params,
)

logger = logging.getLogger(__name__)


class IndicatorInstanceCreator:
    """Build and persist indicator instances in small, testable steps."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

    def create(
        self,
        type_str: str,
        name: Optional[str],
        params: Dict[str, Any],
        color: Optional[str] = None,
    ) -> Dict[str, Any]:
        Cls = self._resolve_type(type_str)
        params_copy = dict(params)
        data_ctx, ctor_params = self._pop_data_context(params_copy)
        datasource, exchange = pull_datasource_exchange(ctor_params, ctx=self._ctx)
        provider = self._resolve_provider(datasource, exchange)
        instance = self._build_instance(Cls, provider, data_ctx, ctor_params)
        runtime_params = self._capture_runtime_params(
            instance, datasource=datasource, exchange=exchange
        )
        meta = self._build_meta(type_str, name, runtime_params, color, datasource, exchange)
        persisted_meta = self._persist_and_cache(meta, instance)
        refresh_strategy_links(meta["id"], persisted_meta, ctx=self._ctx)
        return persisted_meta

    def _resolve_type(self, type_str: str):
        Cls = _INDICATOR_MAP.get(type_str)
        if not Cls:
            raise ValueError(f"Unknown indicator type: {type_str}")
        return Cls

    def _pop_data_context(self, params: Dict[str, Any]) -> Tuple[DataContext, Dict[str, Any]]:
        ctx_keys = ("symbol", "start", "end", "interval")
        try:
            ctx_kwargs = {k: params.pop(k) for k in ctx_keys}
        except KeyError as e:
            raise ValueError(f"Missing required context param: {e.args[0]}")
        data_ctx = DataContext(**ctx_kwargs)
        data_ctx.validate()
        return data_ctx, params

    def _resolve_provider(self, datasource: Optional[str], exchange: Optional[str]):
        return resolve_data_provider(datasource, exchange=exchange, ctx=self._ctx)

    def _build_instance(self, Cls, provider, data_ctx: DataContext, ctor_params: Dict[str, Any]):
        try:
            indicator_name = getattr(Cls, "NAME", Cls.__name__)
            logger.info("event=indicator_create type=%s params=%s", indicator_name, ctor_params)
            inst = Cls.from_context(provider=provider, ctx=data_ctx, **ctor_params)
        except Exception as exc:
            raise RuntimeError(f"Failed to instantiate indicator: {exc}")
        if isinstance(inst, MarketProfileIndicator):
            setattr(inst, "symbol", data_ctx.symbol)
        return inst

    def _capture_runtime_params(
        self,
        inst,
        *,
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> Dict[str, Any]:
        captured = extract_ctor_params(inst)
        runtime_params = dict(captured)
        if datasource:
            runtime_params["datasource"] = datasource
        if exchange:
            runtime_params["exchange"] = exchange
        return scrub_runtime_params(runtime_params)

    def _build_meta(
        self,
        type_str: str,
        name: Optional[str],
        runtime_params: Dict[str, Any],
        color: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> Dict[str, Any]:
        inst_id = str(uuid.uuid4())
        meta = {
            "id": inst_id,
            "type": type_str,
            "params": runtime_params,
            "enabled": True,
            "name": name or type_str.replace("_", " ").title(),
        }
        meta["datasource"] = datasource or DataSource.ALPACA.value
        if exchange:
            meta["exchange"] = exchange
        meta["color"] = normalize_color(color)
        return meta

    def _persist_and_cache(self, meta: Dict[str, Any], inst) -> Dict[str, Any]:
        self._ctx.repository.upsert(meta)
        persisted = self._ctx.repository.get(meta["id"])
        persisted_meta = (
            build_meta_from_record(persisted, ctx=self._ctx)
            if persisted
            else self._ctx.factory.ensure_color(meta)
        )
        self._ctx.cache_manager.cache_indicator(
            meta["id"], persisted_meta, inst, (persisted or {}).get("updated_at")
        )
        return persisted_meta


class IndicatorInstanceUpdater:
    """Handle indicator updates without long orchestration methods."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

    def update(
        self,
        inst_id: str,
        type_str: str,
        params: Dict[str, Any],
        name: Optional[str],
        *,
        color: Optional[str] = None,
        color_provided: bool = False,
    ) -> Dict[str, Any]:
        record = load_indicator_record(inst_id, ctx=self._ctx)
        meta = build_meta_from_record(record, ctx=self._ctx)
        if type_str != meta["type"]:
            raise ValueError("Cannot change indicator type; create a new instance instead")

        params_copy = dict(params)
        cached_inst = self._get_cached_instance(inst_id)
        self._maybe_strip_autosized_bin(params_copy, type_str, cached_inst)
        validated_params = self._ensure_ctor_params(type_str, params_copy)
        data_ctx, ctor_params = self._pop_data_context(validated_params)
        datasource, exchange = pull_datasource_exchange(
            ctor_params, fallback_meta=meta, ctx=self._ctx
        )
        provider = resolve_data_provider(datasource, exchange=exchange, ctx=self._ctx)
        instance = self._rebuild_instance(type_str, provider, data_ctx, ctor_params)
        runtime_params = self._capture_runtime_params(
            instance, datasource=datasource, exchange=exchange
        )
        meta_payload = self._refresh_meta(
            inst_id,
            meta,
            runtime_params,
            name=name,
            color=color,
            color_provided=color_provided,
            datasource=datasource,
            exchange=exchange,
        )
        persisted_meta = self._persist_and_cache(inst_id, instance, meta_payload)
        refresh_strategy_links(inst_id, persisted_meta, ctx=self._ctx)
        return persisted_meta

    def _get_cached_instance(self, inst_id: str):
        try:
            return self._ctx.cache_manager.get_entry(inst_id).instance
        except KeyError:
            return None

    def _maybe_strip_autosized_bin(self, params: Dict[str, Any], type_str: str, cached_inst) -> None:
        if (
            type_str == MarketProfileIndicator.NAME
            and isinstance(cached_inst, MarketProfileIndicator)
            and "bin_size" in params
            and not getattr(cached_inst, "_bin_size_locked", False)
        ):
            params.pop("bin_size", None)

    def _ensure_ctor_params(self, type_str: str, params: Dict[str, Any]) -> Dict[str, Any]:
        Cls = _INDICATOR_MAP.get(type_str)
        sig = inspect.signature(Cls.__init__)
        for pname, p in sig.parameters.items():
            if pname in ("self", "df"):
                continue
            if pname not in params:
                if p.default is inspect._empty:
                    raise ValueError(f"Missing required parameter: {pname}")
                params[pname] = p.default
        return params

    def _pop_data_context(self, params: Dict[str, Any]) -> Tuple[DataContext, Dict[str, Any]]:
        ctx_keys = ("symbol", "start", "end", "interval")
        try:
            ctx_kwargs = {k: params.pop(k) for k in ctx_keys}
        except KeyError as e:
            raise ValueError(f"Missing required context param: {e.args[0]}")
        data_ctx = DataContext(**ctx_kwargs)
        data_ctx.validate()
        return data_ctx, params

    def _rebuild_instance(self, type_str: str, provider, data_ctx: DataContext, ctor_params: Dict[str, Any]):
        Cls = _INDICATOR_MAP.get(type_str)
        try:
            return Cls.from_context(provider=provider, ctx=data_ctx, **ctor_params)
        except Exception as exc:
            raise RuntimeError(f"Failed to re-instantiate indicator: {exc}")

    def _capture_runtime_params(
        self,
        inst,
        *,
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> Dict[str, Any]:
        captured = extract_ctor_params(inst)
        runtime_params = dict(captured)
        if datasource:
            runtime_params["datasource"] = datasource
        if exchange:
            runtime_params["exchange"] = exchange
        return scrub_runtime_params(runtime_params)

    def _refresh_meta(
        self,
        inst_id: str,
        meta: Any,
        runtime_params: Dict[str, Any],
        *,
        name: Optional[str],
        color: Optional[str],
        color_provided: bool,
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> Dict[str, Any]:
        purge_breakout_cache = self._ctx.breakout_cache.purge_indicator
        purge_breakout_cache(inst_id)
        meta_payload = dict(meta)
        meta_payload["params"] = runtime_params
        if name:
            meta_payload["name"] = name
        if color_provided:
            meta_payload["color"] = normalize_color(color)
        meta_payload["datasource"] = datasource or DataSource.ALPACA.value
        if exchange:
            meta_payload["exchange"] = exchange
        elif "exchange" in meta_payload:
            meta_payload.pop("exchange", None)
        return ensure_color(meta_payload, ctx=self._ctx)

    def _persist_and_cache(self, inst_id: str, instance, meta_payload: Dict[str, Any]) -> Dict[str, Any]:
        self._ctx.repository.upsert(meta_payload)
        refreshed = self._ctx.repository.get(inst_id)
        persisted_meta = (
            build_meta_from_record(refreshed, ctx=self._ctx)
            if refreshed
            else meta_payload
        )
        self._ctx.cache_manager.cache_indicator(
            inst_id, persisted_meta, instance, (refreshed or {}).get("updated_at")
        )
        return persisted_meta


__all__ = ["IndicatorInstanceCreator", "IndicatorInstanceUpdater"]
