from __future__ import annotations

"""Factory utilities for indicator instantiation and metadata shaping."""

import logging
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional, TYPE_CHECKING

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator
from indicators.pivot_level import PivotLevelIndicator
from indicators.trendline import TrendlineIndicator
from indicators.vwap import VWAPIndicator

from ..providers.data_provider_resolver import DataProviderResolver, default_resolver

if TYPE_CHECKING:
    from .indicator_service.context import IndicatorServiceContext

logger = logging.getLogger(__name__)


INDICATOR_MAP = {
    "vwap": VWAPIndicator,
    "pivot_level": PivotLevelIndicator,
    "trendline": TrendlineIndicator,
    "market_profile": MarketProfileIndicator,
}


class IndicatorFactory:
    """Build indicator metadata and runtime instances."""

    def __init__(
        self,
        resolver: Optional[DataProviderResolver] = None,
        ctx: Optional[IndicatorServiceContext] = None
    ) -> None:
        self._resolver = resolver or default_resolver()
        self._ctx = ctx  # Will be set by context during initialization

    def build_meta_from_record(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        from .indicator_service.utils import attach_signal_catalog

        meta = self._coerce_record_meta(record)
        meta = self._ensure_color(meta)

        # Attach signal catalog to enrich metadata with available signal rules
        if self._ctx:
            meta = attach_signal_catalog(meta, ctx=self._ctx)
            logger.debug(
                "build_meta_from_record | id=%s | type=%s | signal_rules_attached=%s",
                meta.get("id"),
                meta.get("type"),
                "signal_rules" in meta
            )
        else:
            logger.warning(
                "⚠ build_meta_from_record: No context available, signal catalog not attached | id=%s",
                record.get("id")
            )

        return meta

    def ensure_color(self, meta: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_color(meta)

    def build_indicator_instance(self, meta: Mapping[str, Any], *, datasource: Optional[str] = None, exchange: Optional[str] = None):
        inst_id = str(meta.get("id") or "").strip()
        type_str = str(meta.get("type") or "").strip()
        Cls = INDICATOR_MAP.get(type_str)
        if not inst_id or not Cls:
            raise KeyError(f"Unknown indicator: {inst_id}")

        params = deepcopy(meta.get("params") or {})
        ctx_kwargs: Dict[str, Any] = {}
        missing = []
        for key in ("symbol", "start", "end", "interval"):
            if key in params:
                ctx_kwargs[key] = params.pop(key)
            else:
                missing.append(key)
        if missing:
            raise ValueError(
                f"Indicator {inst_id} missing required context: {', '.join(missing)}"
            )

        # Allow caller to override provider selection with explicit datasource/exchange
        if datasource is None:
            datasource = self._resolver.normalize_datasource(meta.get("datasource"))
        else:
            datasource = self._resolver.normalize_datasource(datasource)

        if exchange is None:
            exchange = self._resolver.normalize_exchange(meta.get("exchange"))
        else:
            exchange = self._resolver.normalize_exchange(exchange)

        if exchange and not datasource:
            datasource = "CCXT"

        ctx = DataContext(**ctx_kwargs)
        ctx.validate()
        provider = self._resolver.resolve(datasource, exchange=exchange)
        inst = Cls.from_context(provider=provider, ctx=ctx, **params)
        if isinstance(inst, MarketProfileIndicator):
            setattr(inst, "symbol", ctx_kwargs.get("symbol"))
        return inst

    def _coerce_record_meta(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        inst_id = str(record.get("id") or "").strip()
        payload = {
            "id": inst_id,
            "type": record.get("type"),
            "name": record.get("name") or record.get("type") or inst_id or "Indicator",
            "params": deepcopy(record.get("params") or {}),
            "color": record.get("color"),
            "datasource": record.get("datasource"),
            "exchange": record.get("exchange"),
            "enabled": bool(record.get("enabled", True)),
        }
        return self._ensure_color(payload)

    def _normalize_color(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        color = str(value).strip()
        return color or None

    def _ensure_color(self, meta: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(meta)
        normalized["color"] = self._normalize_color(meta.get("color")) or "#4f46e5"
        return normalized


def default_factory() -> IndicatorFactory:
    return IndicatorFactory()
