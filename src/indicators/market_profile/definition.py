"""Market profile indicator definition contract."""

from __future__ import annotations

from datetime import timedelta
from math import isfinite
from typing import Any, Mapping

from indicators.config import DataContext, IndicatorExecutionContext
from indicators.manifest import resolve_manifest_params

from .manifest import MANIFEST, FIRST_RETURN_MANIFEST, CAUSAL_MANIFEST, OBSERVED_MANIFEST


class MarketProfileIndicator:
    NAME = MANIFEST.type
    MANIFEST = MANIFEST
    MANIFEST_VERSIONS = {"v1": MANIFEST, "v2": FIRST_RETURN_MANIFEST, "v3": CAUSAL_MANIFEST, "v4": OBSERVED_MANIFEST}

    @classmethod
    def resolve_config(
        cls,
        params: Mapping[str, Any] | None,
        *,
        strict_unknown: bool = False,
    ) -> dict[str, Any]:
        return resolve_manifest_params(
            cls.MANIFEST,
            params,
            strict_unknown=strict_unknown,
        )

    @classmethod
    def build_runtime_data_request(
        cls,
        *,
        resolved_params: Mapping[str, Any],
        execution_context: IndicatorExecutionContext,
    ) -> DataContext:
        execution_context.validate()
        end_date = execution_context.data_context().end_utc()
        days_back = int(resolved_params.get("days_back") or 0)
        start_date = end_date - timedelta(days=days_back)
        runtime_input = cls.MANIFEST.runtime_inputs[0] if cls.MANIFEST.runtime_inputs else None
        source_timeframe = (
            str(runtime_input.source_timeframe or "").strip()
            if runtime_input is not None
            else ""
        ) or "30m"
        return execution_context.data_context(
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            interval=source_timeframe,
        )

    @classmethod
    def build_runtime_indicator(
        cls,
        *,
        indicator_id: str,
        meta: Mapping[str, Any],
        resolved_params: Mapping[str, Any],
        strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
        execution_context: IndicatorExecutionContext | None = None,
        source_facts: Any = None,
    ) -> Any:
        from .runtime.typed_indicator import TypedMarketProfileIndicator

        _ = strategy_indicator_metas
        if execution_context is None:
            raise RuntimeError(
                f"market_profile_runtime_build_failed: execution context required indicator_id={indicator_id}"
            )
        if source_facts is None:
            raise RuntimeError(
                f"market_profile_runtime_build_failed: source_facts required indicator_id={indicator_id}"
            )
        return TypedMarketProfileIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or cls.MANIFEST.version),
            params=dict(resolved_params),
            source_facts=source_facts,
        )

    @classmethod
    def build_runtime_source_facts(
        cls,
        *,
        resolved_params: Mapping[str, Any],
        execution_context: IndicatorExecutionContext,
        source_frame: Any,
        source_diagnostics: Any = None,
        version: str = "v1",
    ) -> Any:
        from .compute.engine import MarketProfileIndicator as ComputeMarketProfileIndicator
        from indicators.registry import get_indicator_manifest

        get_indicator_manifest(cls.NAME, version)
        if version in {"v3", "v4"}:
            bin_size = float(resolved_params.get("bin_size") or 0)
            if not isfinite(bin_size) or bin_size <= 0:
                raise ValueError(f"market_profile_source_contract_invalid: {version} requires a fixed positive bin_size")

        if source_frame is None:
            raise RuntimeError(
                "market_profile_runtime_build_failed: source_frame required"
            )
        compute_indicator = ComputeMarketProfileIndicator(
            source_frame,
            bin_size=resolved_params.get("bin_size"),
            use_merged_value_areas=bool(resolved_params.get("use_merged_value_areas")),
            merge_threshold=float(resolved_params.get("merge_threshold")),
            min_merge_sessions=int(resolved_params.get("min_merge_sessions")),
            extend_value_area_to_chart_end=bool(resolved_params.get("extend_value_area_to_chart_end")),
            days_back=int(resolved_params.get("days_back")),
            symbol=execution_context.symbol,
        )
        source_facts = compute_indicator.build_runtime_source_facts(
            params=dict(resolved_params),
            symbol=execution_context.symbol,
            chart_timeframe=execution_context.interval,
            causal_availability=version == "v3",
            source_candle_continuity=(
                source_diagnostics.get("source_candle_continuity")
                if isinstance(source_diagnostics, Mapping)
                else None
            ),
        )
        if version == "v4":
            from .compute.internal.observed_sessions import observed_session_events

            source_facts["profiles"] = observed_session_events(
                source_frame,
                bin_size=compute_indicator.bin_size,
                precision=compute_indicator.price_precision,
                chart_timeframe=execution_context.interval,
            )
            source_facts["profile_boundary_semantics"] = "observed_sessions.v4"
            source_facts["profile_availability"] = {
                "contract": "observed_sessions.v4",
                "policy": "closed_session_observed_subset",
                "closure_evidence_consumed": False,
                "predictive_qualification": False,
            }
        return source_facts


__all__ = ["MarketProfileIndicator"]
