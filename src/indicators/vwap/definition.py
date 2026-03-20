"""VWAP indicator definition contract."""

from __future__ import annotations

from typing import Any, Mapping

from indicators.config import IndicatorExecutionContext
from indicators.manifest import resolve_manifest_params

from .manifest import MANIFEST


class VWAPIndicatorDefinition:
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
        reset_by = resolved.get("reset_by")
        if str(reset_by).strip().lower() == "cumulative":
            resolved["reset_by"] = "cumulative"
        return resolved

    @classmethod
    def build_compute_data_request(
        cls,
        *,
        resolved_params: Mapping[str, Any],
        execution_context: IndicatorExecutionContext,
    ):
        _ = resolved_params
        execution_context.validate()
        return execution_context.data_context()

    @classmethod
    def build_compute_indicator(
        cls,
        *,
        source_frame: Any,
        resolved_params: Mapping[str, Any],
        execution_context: IndicatorExecutionContext,
    ) -> Any:
        from .compute import VWAPIndicator as ComputeVWAPIndicator

        _ = execution_context
        return ComputeVWAPIndicator(df=source_frame, **dict(resolved_params))


__all__ = ["VWAPIndicatorDefinition"]
