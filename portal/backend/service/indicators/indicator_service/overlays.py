from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Mapping, Optional, Tuple

from engines.bot_runtime.core.domain import normalize_epoch
from indicators.config import DataContext
from indicators.runtime.incremental_cache_registry import is_incremental_cacheable
from signals.overlays.schema import build_overlay
from portal.backend.service.bots.bot_runtime.runtime.chart_state import ChartStateBuilder
from utils.log_context import build_log_context, with_log_context
from utils.perf_log import get_obs_enabled

from .context import IndicatorServiceContext, _context
from .incremental_overlays import build_incremental_overlay_indicator
from .runtime_projection import build_runtime_state_overlay
from ...market import instrument_service
from .utils import (
    get_indicator_entry,
    normalize_datasource,
    normalize_exchange,
    resolve_data_provider,
    sanitize_json,
    scrub_runtime_params,
)

logger = logging.getLogger(__name__)

_OVERLAY_VISIBILITY = ChartStateBuilder(
    normalise_epoch_fn=normalize_epoch,
    log_sequence_fn=lambda _kind, _strategy_id, _sequence: None,
    strategy_key_fn=lambda _series: "",
)


class IndicatorOverlayBuilder:
    """Compose overlay generation from small, reusable steps."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx
        self._obs_enabled = get_obs_enabled()

    def build(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        *,
        symbol: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        instrument_id: Optional[str] = None,
        overlay_options: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        logger.info(
            "event=overlay_build_start indicator_id=%s symbol=%s interval=%s start=%s end=%s instrument_id=%s",
            inst_id,
            symbol,
            interval,
            start,
            end,
            instrument_id,
        )
        entry = self._load_entry(inst_id, start, end, interval, symbol, datasource, exchange)
        logger.info(
            "event=overlay_entry_loaded indicator_id=%s indicator_type=%s",
            inst_id,
            entry.meta.get("type"),
        )
        effective_overlay_options = dict(overlay_options or {})
        sym = self._resolve_symbol(entry, symbol)
        provider, data_ctx, effective_datasource, effective_exchange, effective_interval = self._prepare_provider(
            entry.meta, sym, start, end, interval, datasource, exchange, instrument_id
        )
        cache_enabled = self._ctx.overlay_cache.is_enabled(entry.meta.get("type"))
        cached_payload = self._maybe_fetch_cached(
            inst_id,
            entry,
            sym,
            effective_interval,
            data_ctx.start,
            data_ctx.end,
            effective_datasource,
            effective_exchange,
            effective_overlay_options,
        )
        if cached_payload is not None:
            logger.info("event=overlay_cache_hit indicator_id=%s", inst_id)
            if isinstance(cached_payload, Mapping):
                payload_obj = cached_payload.get("payload")
                if isinstance(payload_obj, Mapping):
                    boxes = payload_obj.get("boxes")
                    sample_box = boxes[0] if isinstance(boxes, list) and boxes else None
                    logger.debug(
                        "event=overlay_cache_hit_payload indicator_id=%s boxes=%s sample_box=%s",
                        inst_id,
                        len(boxes) if isinstance(boxes, list) else 0,
                        sample_box,
                    )
            if isinstance(cached_payload, dict) and "type" in cached_payload and "payload" in cached_payload:
                try:
                    cached_overlay = self._apply_walk_forward_visibility(
                        cached_payload,
                        end=end,
                        overlay_options=effective_overlay_options,
                        indicator_id=inst_id,
                    )
                    payload = cached_overlay.get("payload") if isinstance(cached_overlay, Mapping) else None
                    if not isinstance(payload, Mapping):
                        raise LookupError("No overlays computed for given window")
                    self._validate_payload(dict(payload))
                    return dict(cached_overlay)
                except LookupError:
                    logger.warning(
                        "event=overlay_cache_stale indicator_id=%s message='cached overlay had no visible artifacts; recomputing'",
                        inst_id,
                    )
            logger.warning(
                "event=overlay_cache_payload_invalid indicator_id=%s cache_key=%s message='cached payload missing type/payload'",
                inst_id,
                self._cache_key(
                    inst_id,
                    entry,
                    sym,
                    effective_interval,
                    data_ctx.start,
                    data_ctx.end,
                    effective_datasource,
                    effective_exchange,
                    effective_overlay_options,
                ),
            )
        if cache_enabled:
            logger.info("event=overlay_cache_miss indicator_id=%s", inst_id)
        logger.info(
            "event=overlay_provider_prepared indicator_id=%s data_start=%s data_end=%s",
            inst_id,
            data_ctx.start,
            data_ctx.end,
        )
        df = self._load_candles(provider, data_ctx, inst_id, sym, interval)
        logger.info(
            "event=overlay_candles_loaded indicator_id=%s candles=%d",
            inst_id,
            len(df),
        )
        runtime_overlay = build_runtime_state_overlay(
            indicator_id=inst_id,
            meta=entry.meta,
            instance=entry.instance,
            df=df,
            symbol=sym,
            timeframe=interval,
            overlay_options=effective_overlay_options,
        )
        if runtime_overlay is not None:
            overlay = runtime_overlay
        else:
            overlay_indicator = self._build_overlay_indicator(
                entry.instance,
                df,
                inst_id,
                sym,
                effective_interval,
                effective_overlay_options,
                provider=provider,
                data_ctx=data_ctx,
                indicator_type=entry.meta.get("type"),
            )
            logger.info(
                "event=overlay_indicator_built indicator_id=%s indicator_type=%s",
                inst_id,
                type(overlay_indicator).__name__,
            )
            payload, raw_payload = self._serialize_payload(overlay_indicator, df)
            logger.info(
                "event=overlay_payload_serialized indicator_id=%s boxes=%d markers=%d price_lines=%d",
                inst_id,
                len(payload.get("boxes", [])),
                len(payload.get("markers", [])),
                len(payload.get("price_lines", [])),
            )
            self._validate_payload(payload)
            self._log_counts(inst_id, payload, raw_payload)
            overlay = build_overlay(str(entry.meta.get("type")), payload)
        overlay = self._apply_walk_forward_visibility(
            overlay,
            end=end,
            overlay_options=effective_overlay_options,
            indicator_id=inst_id,
        )
        self._maybe_store_cached(
            inst_id,
            entry,
            sym,
            effective_interval,
            data_ctx.start,
            data_ctx.end,
            effective_datasource,
            effective_exchange,
            effective_overlay_options,
            overlay,
        )
        logger.info(
            "event=overlay_build_complete indicator_id=%s",
            inst_id,
        )
        return overlay

    def _apply_walk_forward_visibility(
        self,
        overlay: Mapping[str, Any],
        *,
        end: str,
        overlay_options: Optional[Mapping[str, Any]],
        indicator_id: str,
    ) -> Dict[str, Any]:
        options = dict(overlay_options or {})
        visibility_epoch = options.get("visibility_epoch")
        current_epoch = normalize_epoch(visibility_epoch)
        if current_epoch is None:
            current_epoch = normalize_epoch(end)
        if current_epoch is None:
            logger.warning(
                "event=overlay_visibility_epoch_unresolved indicator_id=%s end=%s visibility_epoch=%s",
                indicator_id,
                end,
                visibility_epoch,
            )
            return dict(overlay)

        visible = _OVERLAY_VISIBILITY.visible_overlays(
            [overlay],
            status="running",
            current_epoch=current_epoch,
        )
        if not visible:
            raise LookupError("No overlays computed for given window")

        visible_overlay = visible[0]
        payload = visible_overlay.get("payload") if isinstance(visible_overlay, Mapping) else None
        if isinstance(payload, Mapping):
            visual_count = self._payload_visual_count(payload)
            logger.info(
                "event=overlay_visibility_applied indicator_id=%s visibility_epoch=%s visuals=%s boxes=%s markers=%s price_lines=%s segments=%s polylines=%s",
                indicator_id,
                current_epoch,
                visual_count,
                len(payload.get("boxes", []) if isinstance(payload.get("boxes"), list) else []),
                len(payload.get("markers", []) if isinstance(payload.get("markers"), list) else []),
                len(payload.get("price_lines", []) if isinstance(payload.get("price_lines"), list) else []),
                len(payload.get("segments", []) if isinstance(payload.get("segments"), list) else []),
                len(payload.get("polylines", []) if isinstance(payload.get("polylines"), list) else []),
            )
            if visual_count <= 0:
                logger.warning(
                    "event=overlay_visibility_empty indicator_id=%s visibility_epoch=%s payload_keys=%s",
                    indicator_id,
                    current_epoch,
                    list(payload.keys()),
                )
                raise LookupError("No overlays computed for given window")
        return dict(visible_overlay)

    @staticmethod
    def _payload_visual_count(payload: Mapping[str, Any]) -> int:
        count = 0
        for key in (
            "price_lines",
            "markers",
            "touchPoints",
            "touch_points",
            "boxes",
            "segments",
            "polylines",
            "bubbles",
            "regime_blocks",
            "regime_points",
        ):
            values = payload.get(key)
            if isinstance(values, list):
                count += len(values)
        return count

    def _load_entry(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: Optional[str],
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
    ):
        fb = {
            "symbol": symbol,
            "start": start,
            "end": end,
            "interval": interval,
        }
        if datasource is not None:
            fb["datasource"] = datasource
        if exchange is not None:
            fb["exchange"] = exchange

        return get_indicator_entry(
            inst_id,
            fallback_context=fb,
            persist_backfill=True,
            ctx=self._ctx,
        )

    def _resolve_symbol(self, entry, symbol: Optional[str]) -> str:
        base_params = entry.meta.get("params", {})
        sym = symbol or base_params.get("symbol")
        if not sym:
            raise ValueError("Stored indicator has no symbol and none was provided")
        return sym

    def _prepare_provider(
        self,
        meta: Mapping[str, Any],
        symbol: str,
        start: str,
        end: str,
        interval: str,
        datasource: Optional[str],
        exchange: Optional[str],
        instrument_id: Optional[str],
    ) -> tuple[Any, DataContext, Optional[str], Optional[str], str]:
        stored_params = meta.get("params", {})
        stored_datasource = normalize_datasource(
            meta.get("datasource") or stored_params.get("datasource"), ctx=self._ctx
        )
        stored_exchange = normalize_exchange(
            meta.get("exchange") or stored_params.get("exchange"), ctx=self._ctx
        )

        req_datasource = normalize_datasource(datasource, ctx=self._ctx)
        req_exchange = normalize_exchange(exchange, ctx=self._ctx)

        effective_datasource = req_datasource or stored_datasource
        effective_exchange = req_exchange or stored_exchange

        logger.info(
            "event=overlay_builder_prepare_provider indicator_id=%s symbol=%s "
            "req_datasource=%s req_exchange=%s stored_datasource=%s stored_exchange=%s "
            "effective_datasource=%s effective_exchange=%s",
            meta.get("id"),
            symbol,
            req_datasource,
            req_exchange,
            stored_datasource,
            stored_exchange,
            effective_datasource,
            effective_exchange,
        )

        # resolve_data_provider will raise ValueError if effective_datasource is None
        provider = resolve_data_provider(
            effective_datasource,
            exchange=effective_exchange,
            ctx=self._ctx,
        )

        # Resolve runtime input plan through indicator metadata/specs instead of
        # hard-coding per-indicator fetch window behavior in the service path.
        try:
            runtime_plan = self._ctx.factory.build_runtime_input_plan(
                meta,
                strategy_interval=interval,
                start=start,
                end=end,
            )
        except Exception:
            runtime_plan = {"start": start, "end": end, "source_timeframe": interval}
        effective_start = str(runtime_plan.get("start") or start)
        effective_end = str(runtime_plan.get("end") or end)
        effective_interval = str(runtime_plan.get("source_timeframe") or interval)

        resolved_instrument_id = instrument_id.strip() if isinstance(instrument_id, str) else instrument_id
        if not resolved_instrument_id:
            resolved_instrument_id = instrument_service.require_instrument_id(
                effective_datasource,
                effective_exchange,
                symbol,
            )
        logger.info(
            "event=overlay_instrument_resolved indicator_id=%s instrument_id=%s symbol=%s datasource=%s exchange=%s",
            meta.get("id"),
            resolved_instrument_id,
            symbol,
            effective_datasource,
            effective_exchange,
        )
        data_ctx = DataContext(
            symbol=symbol,
            start=effective_start,
            end=effective_end,
            interval=effective_interval,
            instrument_id=resolved_instrument_id,
        )
        return provider, data_ctx, effective_datasource, effective_exchange, effective_interval

    def _maybe_fetch_cached(
        self,
        inst_id: str,
        entry: Any,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        datasource: Optional[str],
        exchange: Optional[str],
        overlay_options: Optional[Mapping[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        cache = self._ctx.overlay_cache
        indicator_type = entry.meta.get("type") if entry else None
        if not cache or not cache.is_enabled(indicator_type):
            return None
        # NOTE: In-memory overlay cache (LRU). Key includes inst_id/type/symbol/interval/window/datasource/exchange/signature.
        # NOTE: Per-process cache; no cross-process sharing, eviction by max_entries only.
        signature = cache.build_signature(
            scrub_runtime_params(entry.meta.get("params") or {}),
            overlay_options,
        )
        cache_key = cache.build_cache_key(
            inst_id,
            str(indicator_type),
            symbol,
            interval,
            start,
            end,
            datasource=datasource,
            exchange=exchange,
            signature=signature,
            updated_at=getattr(entry, "updated_at", ""),
        )
        cache_key_summary = f"{symbol}:{interval}:{start}->{end}"
        get_started = time.perf_counter() if self._obs_enabled else 0.0
        cached = cache.get(cache_key)
        if self._obs_enabled:
            get_ms = (time.perf_counter() - get_started) * 1000.0
            base_context = build_log_context(
                cache_name="indicator_overlay_cache",
                cache_scope="process",
                cache_key_summary=cache_key_summary,
                time_taken_ms=get_ms,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
                symbol=symbol,
                timeframe=interval,
                datasource=datasource,
                exchange=exchange,
                indicator_id=inst_id,
            )
            logger.debug(
                with_log_context(
                    "cache.get",
                    build_log_context(event="cache.get", **base_context),
                )
            )
            hit_event = "cache.hit" if cached is not None else "cache.miss"
            logger.debug(
                with_log_context(
                    hit_event,
                    build_log_context(event=hit_event, **base_context),
                )
            )
        return cached

    def _maybe_store_cached(
        self,
        inst_id: str,
        entry: Any,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        datasource: Optional[str],
        exchange: Optional[str],
        overlay_options: Optional[Mapping[str, Any]],
        payload: Mapping[str, Any],
    ) -> None:
        cache = self._ctx.overlay_cache
        indicator_type = entry.meta.get("type") if entry else None
        if not cache or not cache.is_enabled(indicator_type):
            return
        signature = cache.build_signature(
            scrub_runtime_params(entry.meta.get("params") or {}),
            overlay_options,
        )
        cache_key = cache.build_cache_key(
            inst_id,
            str(indicator_type),
            symbol,
            interval,
            start,
            end,
            datasource=datasource,
            exchange=exchange,
            signature=signature,
            updated_at=getattr(entry, "updated_at", ""),
        )
        cache_key_summary = f"{symbol}:{interval}:{start}->{end}"
        set_started = time.perf_counter() if self._obs_enabled else 0.0
        cache.set(cache_key, payload)
        if self._obs_enabled:
            set_ms = (time.perf_counter() - set_started) * 1000.0
            base_context = build_log_context(
                cache_name="indicator_overlay_cache",
                cache_scope="process",
                cache_key_summary=cache_key_summary,
                time_taken_ms=set_ms,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
                symbol=symbol,
                timeframe=interval,
                datasource=datasource,
                exchange=exchange,
                indicator_id=inst_id,
            )
            logger.debug(
                with_log_context(
                    "cache.set",
                    build_log_context(event="cache.set", **base_context),
                )
            )

    def _load_candles(self, provider, data_ctx: DataContext, inst_id: str, symbol: str, interval: str):
        logger.info(
            "event=indicator_overlay_prepare indicator=%s symbol=%s interval=%s start=%s end=%s",
            inst_id,
            symbol,
            interval,
            data_ctx.start,
            data_ctx.end,
        )
        df = provider.get_ohlcv(data_ctx)
        if df is None or df.empty:
            raise LookupError("No candles available for given window")
        return df

    def _build_overlay_indicator(
        self,
        instance,
        df,
        inst_id: str,
        symbol: str,
        interval: str,
        overlay_options: Optional[Mapping[str, Any]],
        *,
        provider=None,
        data_ctx: Optional[DataContext] = None,
        indicator_type: Optional[str] = None,
    ):
        """
        Build an overlay-ready indicator instance.

        For incremental-cacheable indicators with provider/data_ctx, creates a fresh instance
        using cached data. Otherwise, returns the base instance.
        """
        options = dict(overlay_options or {})

        # Check if this indicator supports incremental caching and needs a fresh instance
        if (
            indicator_type
            and is_incremental_cacheable(indicator_type)
            and provider is not None
            and data_ctx is not None
        ):
            clone = build_incremental_overlay_indicator(
                indicator_type=str(indicator_type),
                instance=instance,
                df=df,
                inst_id=inst_id,
                symbol=symbol,
                interval=interval,
                overlay_options=options,
                provider=provider,
                data_ctx=data_ctx,
                context=self._ctx,
            )
            if clone is not None:
                logger.debug(
                    "event=indicator_overlay_runtime_clone indicator=%s symbol=%s interval=%s incremental_cacheable=True",
                    inst_id,
                    symbol,
                    interval,
                )
                return clone

        return instance

    def _serialize_payload(self, overlay_indicator, df) -> Tuple[Dict[str, Any], Any]:
        if hasattr(overlay_indicator, "to_lightweight"):
            payload = overlay_indicator.to_lightweight(df)
        elif hasattr(overlay_indicator, "to_overlays"):
            payload = overlay_indicator.to_overlays(df)
        else:
            raise RuntimeError("Indicator does not implement overlay serialization")
        return sanitize_json(payload), payload

    def _validate_payload(self, payload: Optional[Dict[str, Any]]) -> None:
        if not payload:
            raise LookupError("No overlays computed for given window")
        layers = ("price_lines", "markers", "boxes", "segments", "polylines")
        has_visuals = any(
            isinstance(payload.get(k), (list, tuple)) and len(payload.get(k)) > 0
            for k in layers
        )
        if not has_visuals:
            raise LookupError("No overlays computed for given window")

    def _log_counts(self, inst_id: str, payload: Dict[str, Any], raw_payload: Any) -> None:
        layers = ("price_lines", "markers", "boxes", "segments", "polylines")
        counts = {}
        if isinstance(payload, dict):
            counts = {
                k: len(payload.get(k) or [])
                for k in layers
                if isinstance(payload.get(k), (list, tuple))
            }
        logger.info(
            "event=indicator_overlay_result indicator=%s price_lines=%s markers=%s boxes=%s segments=%s polylines=%s",
            inst_id,
            counts.get("price_lines", 0),
            counts.get("markers", 0),
            counts.get("boxes", 0),
            counts.get("segments", 0),
            counts.get("polylines", 0),
        )
        boxes = []
        if isinstance(raw_payload, dict):
            boxes = raw_payload.get("boxes") or []
        if isinstance(boxes, list):
            for idx, box in enumerate(boxes):
                if not isinstance(box, dict):
                    continue
                logger.debug(
                    "event=indicator_overlay_box indicator=%s index=%d x1=%s x2=%s y1=%s y2=%s",
                    inst_id,
                    idx,
                    box.get("x1"),
                    box.get("x2"),
                    box.get("y1"),
                    box.get("y2"),
                )


__all__ = ["IndicatorOverlayBuilder"]
