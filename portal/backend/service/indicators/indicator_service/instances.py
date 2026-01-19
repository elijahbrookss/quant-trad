from __future__ import annotations

import inspect
import logging
import uuid
from typing import Any, Dict, Optional, Tuple

from data_providers import DataSource
from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator

from ..indicator_factory import INDICATOR_MAP as _INDICATOR_MAP
from .context import IndicatorServiceContext, _context
from ...market import instrument_service
from .utils import (
    build_meta_from_record,
    ensure_color,
    extract_ctor_params,
    load_indicator_record,
    normalize_color,
    pull_datasource_exchange,
    # REMOVED: refresh_strategy_links - no longer needed
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
        data_ctx.instrument_id = instrument_service.require_instrument_id(
            datasource,
            exchange,
            data_ctx.symbol,
        )
        instance = self._build_instance(Cls, provider, data_ctx, ctor_params)
        runtime_params = self._capture_runtime_params(
            instance, datasource=datasource, exchange=exchange
        )
        meta = self._build_meta(type_str, name, runtime_params, color, datasource, exchange)
        persisted_meta = self._persist_and_cache(meta, instance)
        # REMOVED: refresh_strategy_links call - strategies load indicators fresh from DB
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
        instrument_id = params.pop("instrument_id", None)
        if instrument_id:
            ctx_kwargs["instrument_id"] = instrument_id
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

        # Validate all required params are present (fail-fast for indicators with REQUIRED_PARAMS)
        required_params = getattr(Cls, "REQUIRED_PARAMS", None)
        if required_params and isinstance(required_params, dict):
            missing = [k for k in required_params.keys() if ctor_params.get(k) is None]
            if missing:
                raise ValueError(
                    f"{indicator_name} indicator missing required params: {missing}. "
                    f"This should not happen after param enforcement."
                )

        return inst

    def _capture_runtime_params(
        self,
        inst,
        *,
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> Dict[str, Any]:
        captured = extract_ctor_params(inst)
        logger.debug(
            "event=extract_ctor_params_result indicator=%s extracted_keys=%s",
            getattr(inst, "NAME", inst.__class__.__name__),
            list(captured.keys()),
        )
        runtime_params = dict(captured)
        if datasource:
            runtime_params["datasource"] = datasource
        if exchange:
            runtime_params["exchange"] = exchange
        scrubbed = scrub_runtime_params(runtime_params)
        logger.debug(
            "event=scrub_runtime_params_result indicator=%s scrubbed_keys=%s",
            getattr(inst, "NAME", inst.__class__.__name__),
            list(scrubbed.keys()),
        )
        return scrubbed

    def _build_meta(
        self,
        type_str: str,
        name: Optional[str],
        runtime_params: Dict[str, Any],
        color: Optional[str],
        datasource: Optional[str],
        exchange: Optional[str],
    ) -> Dict[str, Any]:
        logger.debug(
            "event=build_meta_called type=%s runtime_params_keys=%s runtime_params=%s",
            type_str,
            list(runtime_params.keys()),
            runtime_params,
        )
        inst_id = str(uuid.uuid4())
        meta = {
            "id": inst_id,
            "type": type_str,
            "params": runtime_params,
            "enabled": True,
            "name": name or type_str.replace("_", " ").title(),
        }
        # Datasource/exchange are optional for indicators (compute-only)
        # They will be provided by instrument/strategy context at execution time
        if datasource:
            meta["datasource"] = datasource
        if exchange:
            meta["exchange"] = exchange
        meta["color"] = normalize_color(color)
        logger.debug(
            "event=build_meta_result type=%s meta_params_keys=%s",
            type_str,
            list(meta.get("params", {}).keys()),
        )
        return meta

    def _persist_and_cache(self, meta: Dict[str, Any], inst) -> Dict[str, Any]:
        logger.info(
            "event=persist_and_cache_called indicator_id=%s meta_params_keys=%s meta_params=%s",
            meta.get("id"),
            list(meta.get("params", {}).keys()),
            meta.get("params"),
        )
        self._ctx.repository.upsert(meta)
        logger.info(
            "event=after_upsert indicator_id=%s meta_params_keys=%s meta_params=%s",
            meta.get("id"),
            list(meta.get("params", {}).keys()),
            meta.get("params"),
        )
        persisted = self._ctx.repository.get(meta["id"])
        logger.info(
            "event=persisted_record_retrieved indicator_id=%s persisted_params_keys=%s persisted_params=%s",
            meta.get("id"),
            list((persisted or {}).get("params", {}).keys()),
            (persisted or {}).get("params"),
        )
        persisted_meta = (
            build_meta_from_record(persisted, ctx=self._ctx)
            if persisted
            else self._ctx.factory.ensure_color(meta)
        )
        # Cache removed: instances are now built fresh from DB on each access
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
        logger.info(
            "event=indicator_update_start indicator_id=%s type=%s",
            inst_id,
            type_str,
        )

        # Check if only color/name changed (no params change)
        # This avoids expensive rebuild when user just updates color
        # Note: scrub runtime params (datasource, exchange) before comparison since they're stored separately
        scrubbed_params = scrub_runtime_params(params)
        params_unchanged = scrubbed_params == meta.get("params", {})
        color_only_update = params_unchanged and (color_provided or name)

        if color_only_update:
            # Fast path: only update metadata without rebuilding indicator
            cached_inst = self._get_cached_instance(inst_id)
            if cached_inst is None:
                # If not cached, need full rebuild anyway
                color_only_update = False

        if color_only_update:
            # Update only color/name in metadata without rebuilding
            meta_payload = dict(meta)
            if name:
                meta_payload["name"] = name
            if color_provided:
                meta_payload["color"] = normalize_color(color)
            meta_payload = ensure_color(meta_payload, ctx=self._ctx)
            self._ctx.repository.upsert(meta_payload)
            refreshed = self._ctx.repository.get(inst_id)
            persisted_meta = (
                build_meta_from_record(refreshed, ctx=self._ctx)
                if refreshed
                else meta_payload
            )
            # Cache removed: instances are now built fresh from DB on each access
            logger.info(
                "event=indicator_update_complete indicator_id=%s type=%s mode=color_only",
                inst_id,
                type_str,
            )
            return persisted_meta

        # Full rebuild path: params changed
        params_copy = dict(params)
        cached_inst = self._get_cached_instance(inst_id)
        self._maybe_strip_autosized_bin(params_copy, type_str, cached_inst)
        validated_params = self._ensure_ctor_params(type_str, params_copy)
        data_ctx, ctor_params = self._pop_data_context(validated_params)
        datasource, exchange = pull_datasource_exchange(
            ctor_params, fallback_meta=meta, ctx=self._ctx
        )
        provider = resolve_data_provider(datasource, exchange=exchange, ctx=self._ctx)
        data_ctx.instrument_id = instrument_service.require_instrument_id(
            datasource,
            exchange,
            data_ctx.symbol,
        )
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
        # REMOVED: refresh_strategy_links call - strategies load indicators fresh from DB
        logger.info(
            "event=indicator_update_complete indicator_id=%s type=%s mode=rebuild",
            inst_id,
            type_str,
        )
        return persisted_meta

    def _get_cached_instance(self, inst_id: str):
        # Cache removed: always return None (no cached instances)
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
        """Ensure required params are present using class-declared defaults."""
        Cls = _INDICATOR_MAP.get(type_str)

        # Check if indicator class declares required params (modular approach)
        required_params = getattr(Cls, "REQUIRED_PARAMS", None)

        if required_params and isinstance(required_params, dict):
            for key, default_value in required_params.items():
                if key not in params:
                    params[key] = default_value
                    logger.info(
                        "event=indicator_param_default_applied type=%s param=%s value=%s",
                        getattr(Cls, "NAME", Cls.__name__),
                        key,
                        default_value
                    )

        # Fallback: inspect constructor signature for indicators without REQUIRED_PARAMS
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
        instrument_id = params.pop("instrument_id", None)
        if instrument_id:
            ctx_kwargs["instrument_id"] = instrument_id
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
        logger.debug(
            "event=extract_ctor_params_result_updater indicator=%s extracted_keys=%s",
            getattr(inst, "NAME", inst.__class__.__name__),
            list(captured.keys()),
        )
        runtime_params = dict(captured)
        if datasource:
            runtime_params["datasource"] = datasource
        if exchange:
            runtime_params["exchange"] = exchange
        scrubbed = scrub_runtime_params(runtime_params)
        logger.debug(
            "event=scrub_runtime_params_result_updater indicator=%s scrubbed_keys=%s",
            getattr(inst, "NAME", inst.__class__.__name__),
            list(scrubbed.keys()),
        )
        return scrubbed

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
        purge_overlay_cache = self._ctx.overlay_cache.purge_indicator
        purge_overlay_cache(inst_id)
        purge_incremental_cache = self._ctx.incremental_cache.purge_indicator
        purge_incremental_cache(inst_id)
        meta_payload = dict(meta)
        meta_payload["params"] = runtime_params
        if name:
            meta_payload["name"] = name
        if color_provided:
            meta_payload["color"] = normalize_color(color)
        # Datasource/exchange are optional for indicators (compute-only)
        # They will be provided by instrument/strategy context at execution time
        if datasource:
            meta_payload["datasource"] = datasource
        elif "datasource" in meta_payload:
            # Remove datasource if explicitly set to None
            meta_payload.pop("datasource", None)
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
        # Cache removed: instances are now built fresh from DB on each access
        return persisted_meta


__all__ = ["IndicatorInstanceCreator", "IndicatorInstanceUpdater"]
