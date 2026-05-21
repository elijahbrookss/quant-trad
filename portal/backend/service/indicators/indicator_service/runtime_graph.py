from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, Mapping, Optional, Sequence

from indicators.config import IndicatorExecutionContext
from indicators.registry import get_indicator_manifest

from ...market import candle_service
from ..dependency_bindings import validate_dependency_bindings
from ..indicator_factory import INDICATOR_MAP as _INDICATOR_MAP, runtime_indicator_builder_for_type
from .context import IndicatorServiceContext, _context
from .utils import build_meta_from_record, load_indicator_record

logger = logging.getLogger(__name__)


def build_runtime_indicator_instance(
    indicator_id: str,
    *,
    meta: Mapping[str, Any],
    strategy_indicator_metas: Mapping[str, Mapping[str, Any]] | None = None,
    execution_context: IndicatorExecutionContext | None = None,
    ctx: IndicatorServiceContext = _context,
) -> Any:
    started_at = perf_counter()
    indicator_type = str(meta.get("type") or "").strip()
    indicator_cls = _INDICATOR_MAP.get(indicator_type)
    if indicator_cls is None:
        raise KeyError(f"Unknown indicator type: {indicator_type}")
    builder = runtime_indicator_builder_for_type(indicator_type)
    resolve_started_at = perf_counter()
    resolved_params = indicator_cls.resolve_config(
        meta.get("params"),
        strict_unknown=True,
    )
    resolve_duration_ms = (perf_counter() - resolve_started_at) * 1000.0
    request_duration_ms = 0.0
    fetch_duration_ms = 0.0
    source_fact_duration_ms = 0.0
    source_rows = 0
    source_facts = None
    request_builder = getattr(indicator_cls, "build_runtime_data_request", None)
    if callable(request_builder):
        if execution_context is None:
            raise RuntimeError(
                "indicator_runtime_build_failed: execution context required "
                f"indicator_id={indicator_id} indicator_type={indicator_type}"
            )
        request_started_at = perf_counter()
        data_request = request_builder(
            resolved_params=resolved_params,
            execution_context=execution_context,
        )
        request_duration_ms = (perf_counter() - request_started_at) * 1000.0
        if data_request is not None:
            resolved_datasource = execution_context.datasource or meta.get("datasource")
            resolved_exchange = execution_context.exchange or meta.get("exchange")
            fetch_started_at = perf_counter()
            source_frame = candle_service.fetch_ohlcv_for_context(
                data_request,
                datasource=str(resolved_datasource or "").strip() or None,
                exchange=str(resolved_exchange or "").strip() or None,
            )
            fetch_duration_ms = (perf_counter() - fetch_started_at) * 1000.0
            if source_frame is None or getattr(source_frame, "empty", False):
                raise LookupError(
                    "indicator_runtime_source_data_missing "
                    f"indicator_id={indicator_id} indicator_type={indicator_type}"
                )
            if hasattr(source_frame, "index"):
                source_rows = int(len(source_frame.index))
            else:
                source_rows = int(getattr(source_frame, "rows", 0) or 0)
            source_facts_builder = getattr(indicator_cls, "build_runtime_source_facts", None)
            if callable(source_facts_builder):
                source_facts_started_at = perf_counter()
                source_facts = source_facts_builder(
                    resolved_params=resolved_params,
                    execution_context=execution_context,
                    source_frame=source_frame,
                )
                source_fact_duration_ms = (perf_counter() - source_facts_started_at) * 1000.0
    build_started_at = perf_counter()
    indicator = builder(
        indicator_id=indicator_id,
        meta=meta,
        resolved_params=resolved_params,
        strategy_indicator_metas=strategy_indicator_metas or {},
        execution_context=execution_context,
        source_facts=source_facts,
    )
    build_duration_ms = (perf_counter() - build_started_at) * 1000.0
    logger.info(
        "event=indicator_runtime_instance_built indicator_id=%s indicator_type=%s symbol=%s timeframe=%s duration_total_ms=%.3f duration_resolve_ms=%.3f duration_request_ms=%.3f duration_fetch_ms=%.3f duration_source_facts_ms=%.3f duration_build_ms=%.3f source_rows=%s has_source_facts=%s",
        indicator_id,
        indicator_type,
        execution_context.symbol if execution_context is not None else None,
        execution_context.interval if execution_context is not None else None,
        (perf_counter() - started_at) * 1000.0,
        resolve_duration_ms,
        request_duration_ms,
        fetch_duration_ms,
        source_fact_duration_ms,
        build_duration_ms,
        source_rows,
        source_facts is not None,
    )
    return indicator


def collect_runtime_indicator_metas(
    indicator_ids: Sequence[str],
    *,
    ctx: IndicatorServiceContext = _context,
    preloaded_metas: Mapping[str, Mapping[str, Any]] | None = None,
) -> Dict[str, Dict[str, Any]]:
    resolved: Dict[str, Dict[str, Any]] = {}
    visiting: set[str] = set()
    available = {
        str(key): dict(value)
        for key, value in (preloaded_metas or {}).items()
        if str(key).strip() and isinstance(value, Mapping)
    }

    def visit(indicator_id: str) -> None:
        normalized_id = str(indicator_id or "").strip()
        if not normalized_id:
            raise RuntimeError("indicator_runtime_graph_invalid: indicator id required")
        if normalized_id in resolved:
            return
        if normalized_id in visiting:
            raise RuntimeError(
                f"indicator_runtime_graph_invalid: dependency cycle at indicator_id={normalized_id}"
            )
        visiting.add(normalized_id)
        meta = dict(
            available.get(normalized_id)
            or build_meta_from_record(load_indicator_record(normalized_id, ctx=ctx), ctx=ctx)
        )
        if not bool(meta.get("runtime_supported")):
            raise RuntimeError(f"Indicator is not runtime-supported: {normalized_id}")
        indicator_type = str(meta.get("type") or "").strip()
        manifest = get_indicator_manifest(indicator_type)
        resolved_dependencies = validate_dependency_bindings(
            manifest=manifest,
            bindings=meta.get("dependencies"),
            ctx=ctx,
            indicator_id=normalized_id,
        )
        meta["dependencies"] = resolved_dependencies
        for dependency in resolved_dependencies:
            visit(str(dependency.get("indicator_id") or ""))
        resolved[normalized_id] = meta
        visiting.remove(normalized_id)

    for indicator_id in indicator_ids:
        visit(str(indicator_id or ""))
    return resolved


def build_runtime_indicator_graph(
    indicator_ids: Sequence[str],
    *,
    execution_context: IndicatorExecutionContext,
    ctx: IndicatorServiceContext = _context,
    preloaded_metas: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[Dict[str, Dict[str, Any]], list[Any]]:
    metas = collect_runtime_indicator_metas(
        indicator_ids,
        ctx=ctx,
        preloaded_metas=preloaded_metas,
    )
    indicators = [
        build_runtime_indicator_instance(
            indicator_id,
            meta=meta,
            strategy_indicator_metas=metas,
            execution_context=execution_context,
            ctx=ctx,
        )
        for indicator_id, meta in metas.items()
    ]
    return metas, indicators


__all__ = [
    "build_runtime_indicator_graph",
    "build_runtime_indicator_instance",
    "collect_runtime_indicator_metas",
]
