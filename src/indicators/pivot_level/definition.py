"""Pivot level indicator definition contract."""

from __future__ import annotations

from typing import Any, Mapping

from indicators.config import IndicatorExecutionContext
from indicators.manifest import resolve_manifest_params

from .manifest import MANIFEST


class PivotLevelIndicatorDefinition:
    NAME = MANIFEST.type
    MANIFEST = MANIFEST

    @classmethod
    def resolve_config(
        cls,
        params: Mapping[str, Any] | None,
        *,
        strict_unknown: bool = False,
    ) -> dict[str, Any]:
        resolved = resolve_manifest_params(
            cls.MANIFEST,
            params,
            strict_unknown=strict_unknown,
        )
        lookbacks = resolved.get("lookbacks")
        if isinstance(lookbacks, list):
            resolved["lookbacks"] = tuple(int(value) for value in lookbacks)
        return resolved

    @classmethod
    def build_compute_data_request(
        cls,
        *,
        resolved_params: Mapping[str, Any],
        execution_context: IndicatorExecutionContext,
    ):
        execution_context.validate()
        return execution_context.data_context(interval=str(resolved_params.get("timeframe") or execution_context.interval))

    @classmethod
    def build_compute_indicator(
        cls,
        *,
        source_frame: Any,
        resolved_params: Mapping[str, Any],
        execution_context: IndicatorExecutionContext,
    ) -> Any:
        from .compute import PivotLevelIndicator as ComputePivotLevelIndicator

        _ = execution_context
        return ComputePivotLevelIndicator(df=source_frame, **dict(resolved_params))


__all__ = ["PivotLevelIndicatorDefinition"]
