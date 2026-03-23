from __future__ import annotations

"""Factory utilities for indicator metadata shaping and runtime-input planning."""

import logging
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, TYPE_CHECKING

from data_providers.utils.ohlcv import interval_to_timedelta

from indicators.definition_contract import definition_supports_compute, definition_supports_runtime
from indicators.manifest import (
    manifest_overlay_catalog,
    manifest_runtime_input_specs,
    serialize_indicator_manifest,
)
from indicators.registry import INDICATOR_MAP, get_indicator_definition, get_indicator_manifest
from .output_prefs import typed_outputs_with_prefs

from ..providers.data_provider_resolver import DataProviderResolver, default_resolver

if TYPE_CHECKING:
    from .indicator_service.context import IndicatorServiceContext

logger = logging.getLogger(__name__)


def runtime_indicator_builder_for_type(indicator_type: str) -> Callable[..., Any]:
    definition = get_indicator_definition(indicator_type)
    builder = getattr(definition, "build_runtime_indicator", None)
    if not callable(builder):
        raise RuntimeError(
            f"Indicator type '{indicator_type}' does not declare a runtime indicator builder"
        )
    return builder


@dataclass(frozen=True)
class IndicatorRuntimeInputSpec:
    source_timeframe: Optional[str] = None
    source_timeframe_param: Optional[str] = None
    lookback_bars: Optional[int] = None
    lookback_bars_param: Optional[str] = None
    lookback_days: Optional[int] = None
    lookback_days_param: Optional[str] = None


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
    """Build indicator metadata and runtime input plans."""

    def __init__(
        self,
        resolver: Optional[DataProviderResolver] = None,
        ctx: Optional[IndicatorServiceContext] = None,
    ) -> None:
        self._resolver = resolver or default_resolver()
        self._ctx = ctx

    def build_meta_from_record(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        meta = self._coerce_record_meta(record)
        meta = self._ensure_color(meta)
        indicator_type = str(meta.get("type") or "").strip()
        if indicator_type:
            try:
                definition = get_indicator_definition(indicator_type)
                manifest = get_indicator_manifest(indicator_type)
            except Exception:
                return meta
            meta["manifest"] = serialize_indicator_manifest(manifest)
            typed_outputs, output_prefs = typed_outputs_with_prefs(
                manifest=manifest,
                output_prefs=meta.get("output_prefs"),
            )
            meta["typed_outputs"] = typed_outputs
            meta["output_prefs"] = output_prefs
            meta["overlay_outputs"] = manifest_overlay_catalog(manifest)
            meta["runtime_supported"] = definition_supports_runtime(definition)
            meta["compute_supported"] = definition_supports_compute(definition)
        return meta

    def ensure_color(self, meta: Dict[str, Any]) -> Dict[str, Any]:
        return self._ensure_color(meta)

    def get_runtime_input_specs_for_type(self, type_str: str) -> List[IndicatorRuntimeInputSpec]:
        try:
            manifest = get_indicator_manifest(type_str)
        except Exception:
            return []
        specs: List[IndicatorRuntimeInputSpec] = []
        for raw in manifest_runtime_input_specs(manifest):
            specs.append(
                IndicatorRuntimeInputSpec(
                    source_timeframe=(str(raw.source_timeframe).strip() or None)
                    if raw.source_timeframe is not None
                    else None,
                    source_timeframe_param=(
                        str(raw.source_timeframe_param).strip() or None
                    )
                    if raw.source_timeframe_param is not None
                    else None,
                    lookback_bars=_coerce_positive_int(raw.lookback_bars),
                    lookback_bars_param=(
                        str(raw.lookback_bars_param).strip() or None
                    )
                    if raw.lookback_bars_param is not None
                    else None,
                    lookback_days=_coerce_positive_int(raw.lookback_days),
                    lookback_days_param=(
                        str(raw.lookback_days_param).strip() or None
                    )
                    if raw.lookback_days_param is not None
                    else None,
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
        definition = get_indicator_definition(indicator_type)
        params = definition.resolve_config(meta.get("params"), strict_unknown=True)
        specs = self.get_runtime_input_specs_for_type(indicator_type)

        source_timeframe = strategy_interval
        lookback_bars: Optional[int] = None
        lookback_days: Optional[int] = None
        incremental_eval = bool(specs)

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
            "incremental_eval": incremental_eval,
            "lookback_bars": lookback_bars,
            "lookback_days": lookback_days,
            "lookback_seconds": lookback_seconds,
        }
        logger.debug(
            "indicator_runtime_input_plan | indicator_id=%s indicator_type=%s strategy_interval=%s source_timeframe=%s start=%s end=%s lookback_bars=%s lookback_days=%s incremental_eval=%s",
            plan["indicator_id"],
            indicator_type,
            strategy_interval,
            source_timeframe,
            plan["start"],
            plan["end"],
            lookback_bars,
            lookback_days,
            incremental_eval,
        )
        return plan

    def _coerce_record_meta(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        inst_id = str(record.get("id") or "").strip()
        return {
            "id": inst_id,
            "type": record.get("type"),
            "version": record.get("version") or "v1",
            "name": record.get("name") or record.get("type") or inst_id or "Indicator",
            "params": deepcopy(record.get("params") or {}),
            "dependencies": deepcopy(record.get("dependencies") or []),
            "color": record.get("color"),
            "datasource": record.get("datasource"),
            "exchange": record.get("exchange"),
            "enabled": bool(record.get("enabled", True)),
        }

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
