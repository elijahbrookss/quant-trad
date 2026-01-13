from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Tuple

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator

from .context import IndicatorServiceContext, _context
from .utils import (
    get_indicator_entry,
    normalize_datasource,
    normalize_exchange,
    resolve_data_provider,
    sanitize_json,
    scrub_runtime_params,
)

logger = logging.getLogger(__name__)


class IndicatorOverlayBuilder:
    """Compose overlay generation from small, reusable steps."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

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
        overlay_options: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        logger.info(
            "event=overlay_build_start indicator_id=%s symbol=%s interval=%s start=%s end=%s",
            inst_id,
            symbol,
            interval,
            start,
            end,
        )
        entry = self._load_entry(inst_id, start, end, interval, symbol, datasource, exchange)
        logger.info(
            "event=overlay_entry_loaded indicator_id=%s indicator_type=%s",
            inst_id,
            entry.meta.get("type"),
        )
        sym = self._resolve_symbol(entry, symbol)
        provider, data_ctx, effective_datasource, effective_exchange = self._prepare_provider(
            entry.meta, sym, start, end, interval, datasource, exchange
        )
        cache_enabled = self._ctx.overlay_cache.is_enabled(entry.meta.get("type"))
        cached_payload = self._maybe_fetch_cached(
            inst_id,
            entry,
            sym,
            interval,
            data_ctx.start,
            data_ctx.end,
            effective_datasource,
            effective_exchange,
            overlay_options,
        )
        if cached_payload is not None:
            logger.info("event=overlay_cache_hit indicator_id=%s", inst_id)
            return cached_payload
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
        overlay_indicator = self._build_overlay_indicator(
            entry.instance,
            df,
            inst_id,
            sym,
            interval,
            overlay_options,
            provider=provider,
            data_ctx=data_ctx,
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
        self._maybe_store_cached(
            inst_id,
            entry,
            sym,
            interval,
            data_ctx.start,
            data_ctx.end,
            effective_datasource,
            effective_exchange,
            overlay_options,
            payload,
        )
        logger.info(
            "event=overlay_build_complete indicator_id=%s",
            inst_id,
        )
        return payload

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
    ):
        from datetime import datetime, timedelta

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

        # Extend time window for Market Profile indicators that need historical data
        effective_start = start
        if meta.get("type") == "market_profile":
            # days_back MUST be present in stored params (no fallbacks/defaults)
            days_back = stored_params.get("days_back")
            if days_back is None:
                raise ValueError(
                    "Market Profile indicator missing 'days_back' in stored params. "
                    "Indicator may need to be recreated with proper params."
                )
            try:
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                extended_start_dt = start_dt - timedelta(days=days_back)
                effective_start = extended_start_dt.isoformat()
                logger.info(
                    "event=market_profile_extended_window original_start=%s extended_start=%s days_back=%d",
                    start,
                    effective_start,
                    days_back,
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "event=market_profile_extend_failed start=%s days_back=%s error=%s",
                    start,
                    days_back,
                    e,
                )

        data_ctx = DataContext(symbol=symbol, start=effective_start, end=end, interval=interval)
        return provider, data_ctx, effective_datasource, effective_exchange

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
        return cache.get(cache_key)

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
        cache.set(cache_key, payload)

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
    ):
        options = dict(overlay_options or {})
        if isinstance(instance, MarketProfileIndicator) and hasattr(instance, "to_lightweight"):
            mp_overrides = self._market_profile_overrides(options)
            clone = self._ctx.breakout_cache.build_market_profile_overlay_indicator(
                instance,
                df,
                interval=interval,
                symbol=symbol,
                provider=provider,
                data_ctx=data_ctx,
                **mp_overrides,
            )
            logger.debug(
                "event=indicator_overlay_runtime_clone indicator=%s symbol=%s interval=%s",
                inst_id,
                symbol,
                interval,
            )
            return clone
        return instance

    @staticmethod
    def _market_profile_overrides(options: Mapping[str, Any]) -> Dict[str, Any]:
        """Extract Market Profile overlay overrides using single-key lookup (no dual-key fallbacks)."""
        overrides: Dict[str, Any] = {}

        # Single-key lookup only (no market_profile_ prefix fallbacks)
        if "use_merged_value_areas" in options:
            overrides["use_merged_value_areas"] = options["use_merged_value_areas"]

        if "merge_threshold" in options:
            overrides["merge_threshold"] = options["merge_threshold"]

        if "min_merge_sessions" in options:
            overrides["min_merge_sessions"] = options["min_merge_sessions"]

        if "extend_value_area_to_chart_end" in options:
            overrides["extend_value_area_to_chart_end"] = options["extend_value_area_to_chart_end"]

        return overrides

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
