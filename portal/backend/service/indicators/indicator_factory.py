from __future__ import annotations

"""Factory utilities for indicator instantiation and metadata shaping."""

import logging
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, TYPE_CHECKING

from data_providers.utils.ohlcv import interval_to_timedelta

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator
from indicators.pivot_level import PivotLevelIndicator
from indicators.trendline import TrendlineIndicator
from indicators.vwap import VWAPIndicator

from ..providers.data_provider_resolver import DataProviderResolver, default_resolver
from ..market import instrument_service

if TYPE_CHECKING:
    from .indicator_service.context import IndicatorServiceContext

logger = logging.getLogger(__name__)


INDICATOR_MAP = {
    "vwap": VWAPIndicator,
    "pivot_level": PivotLevelIndicator,
    "trendline": TrendlineIndicator,
    "market_profile": MarketProfileIndicator,
}


@dataclass(frozen=True)
class IndicatorRuntimeInputSpec:
    source_timeframe: Optional[str] = None
    source_timeframe_param: Optional[str] = None
    lookback_bars: Optional[int] = None
    lookback_bars_param: Optional[str] = None
    lookback_days: Optional[int] = None
    lookback_days_param: Optional[str] = None
    session_scope: str = "global"
    alignment: str = "closed_bar_only"
    normalization: str = "none"
    incremental_eval: bool = False


def _parse_utc(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _coerce_positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


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

    def get_runtime_input_specs_for_type(self, type_str: str) -> List[IndicatorRuntimeInputSpec]:
        indicator_cls = INDICATOR_MAP.get(type_str)
        if indicator_cls is None:
            return []
        raw_specs = getattr(indicator_cls, "RUNTIME_INPUT_SPECS", None) or []
        specs: List[IndicatorRuntimeInputSpec] = []
        for raw in raw_specs:
            if isinstance(raw, IndicatorRuntimeInputSpec):
                specs.append(raw)
                continue
            if not isinstance(raw, Mapping):
                continue
            specs.append(
                IndicatorRuntimeInputSpec(
                    source_timeframe=(str(raw.get("source_timeframe")).strip() or None) if raw.get("source_timeframe") is not None else None,
                    source_timeframe_param=(str(raw.get("source_timeframe_param")).strip() or None) if raw.get("source_timeframe_param") is not None else None,
                    lookback_bars=_coerce_positive_int(raw.get("lookback_bars")),
                    lookback_bars_param=(str(raw.get("lookback_bars_param")).strip() or None) if raw.get("lookback_bars_param") is not None else None,
                    lookback_days=_coerce_positive_int(raw.get("lookback_days")),
                    lookback_days_param=(str(raw.get("lookback_days_param")).strip() or None) if raw.get("lookback_days_param") is not None else None,
                    session_scope=str(raw.get("session_scope") or "global").strip() or "global",
                    alignment=str(raw.get("alignment") or "closed_bar_only").strip() or "closed_bar_only",
                    normalization=str(raw.get("normalization") or "none").strip() or "none",
                    incremental_eval=bool(raw.get("incremental_eval", False)),
                )
            )
        return specs

    def build_runtime_input_plan(
        self,
        meta: Mapping[str, Any],
        *,
        strategy_interval: str,
        start: str,
        end: str,
    ) -> Dict[str, Any]:
        indicator_type = str(meta.get("type") or "").strip()
        params = meta.get("params") or {}
        if not isinstance(params, Mapping):
            params = {}
        specs = self.get_runtime_input_specs_for_type(indicator_type)

        source_timeframe = strategy_interval
        lookback_bars: Optional[int] = None
        lookback_days: Optional[int] = None
        session_scope = "global"
        alignment = "closed_bar_only"
        normalization = "none"
        incremental_eval = False

        if specs:
            spec = specs[0]
            candidate_tf: Optional[str] = None
            if spec.source_timeframe_param:
                value = params.get(spec.source_timeframe_param)
                if value is not None:
                    candidate_tf = str(value).strip() or None
            if candidate_tf is None:
                candidate_tf = spec.source_timeframe
            if candidate_tf:
                source_timeframe = candidate_tf

            lookback_bars = spec.lookback_bars
            if spec.lookback_bars_param:
                param_bars = _coerce_positive_int(params.get(spec.lookback_bars_param))
                if param_bars is not None:
                    lookback_bars = param_bars

            lookback_days = spec.lookback_days
            if spec.lookback_days_param:
                param_days = _coerce_positive_int(params.get(spec.lookback_days_param))
                if param_days is not None:
                    lookback_days = param_days

            session_scope = spec.session_scope
            alignment = spec.alignment
            normalization = spec.normalization
            incremental_eval = bool(spec.incremental_eval)

        effective_start = _parse_utc(start)
        effective_end = _parse_utc(end)
        if effective_end < effective_start:
            raise ValueError(
                f"Invalid runtime input window for indicator '{meta.get('id')}' ({indicator_type}): end is before start"
            )

        if lookback_days is not None:
            candidate_start = effective_end - timedelta(days=lookback_days)
            if candidate_start < effective_start:
                effective_start = candidate_start

        lookback_seconds: Optional[int] = None
        if lookback_bars is not None:
            try:
                timeframe_seconds = int(interval_to_timedelta(source_timeframe).total_seconds())
            except Exception as exc:
                raise ValueError(
                    f"Invalid source timeframe '{source_timeframe}' for indicator '{meta.get('id')}' ({indicator_type})"
                ) from exc
            if timeframe_seconds <= 0:
                raise ValueError(
                    f"Non-positive timeframe '{source_timeframe}' for indicator '{meta.get('id')}' ({indicator_type})"
                )
            lookback_seconds = timeframe_seconds * lookback_bars
            candidate_start = effective_end - timedelta(seconds=lookback_seconds)
            if candidate_start < effective_start:
                effective_start = candidate_start

        plan = {
            "indicator_id": meta.get("id"),
            "indicator_type": indicator_type,
            "strategy_interval": strategy_interval,
            "source_timeframe": source_timeframe,
            "start": _iso_utc(effective_start),
            "end": _iso_utc(effective_end),
            "session_scope": session_scope,
            "alignment": alignment,
            "normalization": normalization,
            "incremental_eval": incremental_eval,
            "lookback_bars": lookback_bars,
            "lookback_days": lookback_days,
            "lookback_seconds": lookback_seconds,
        }
        logger.debug(
            "indicator_runtime_input_plan | indicator_id=%s indicator_type=%s strategy_interval=%s source_timeframe=%s start=%s end=%s lookback_bars=%s lookback_days=%s session_scope=%s alignment=%s normalization=%s incremental_eval=%s",
            plan["indicator_id"],
            indicator_type,
            strategy_interval,
            source_timeframe,
            plan["start"],
            plan["end"],
            lookback_bars,
            lookback_days,
            session_scope,
            alignment,
            normalization,
            incremental_eval,
        )
        return plan

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

        instrument_id = instrument_service.require_instrument_id(
            datasource,
            exchange,
            ctx_kwargs.get("symbol"),
        )
        ctx = DataContext(**ctx_kwargs, instrument_id=instrument_id)
        ctx.validate()
        provider = self._resolver.resolve(datasource, exchange=exchange)
        use_incremental_cache = (
            Cls is MarketProfileIndicator
            and self._ctx is not None
            and getattr(self._ctx, "incremental_cache", None) is not None
            and bool(inst_id)
        )
        if use_incremental_cache:
            inst = Cls.from_context_with_incremental_cache(
                provider=provider,
                ctx=ctx,
                cache=self._ctx.incremental_cache,
                inst_id=inst_id,
                **params,
            )
            logger.debug(
                "indicator_factory_instance_built_with_incremental_cache | indicator_id=%s type=%s symbol=%s interval=%s datasource=%s exchange=%s",
                inst_id,
                type_str,
                ctx_kwargs.get("symbol"),
                ctx_kwargs.get("interval"),
                datasource,
                exchange,
            )
        else:
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
