"""Candle stats indicator definition contract."""

from __future__ import annotations

from typing import Any, Mapping

from indicators.manifest import resolve_manifest_params

from .manifest import MANIFEST
from .runtime import TypedCandleStatsIndicator


class CandleStatsIndicator:
    NAME = MANIFEST.type
    MANIFEST = MANIFEST

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
    def build_runtime_indicator(
        cls,
        *,
        indicator_id: str,
        meta: Mapping[str, Any],
        resolved_params: Mapping[str, Any],
        strategy_indicator_metas: Mapping[str, Mapping[str, Any]],
        execution_context: Any = None,
        source_facts: Any = None,
    ) -> TypedCandleStatsIndicator:
        _ = strategy_indicator_metas, execution_context, source_facts
        return TypedCandleStatsIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or cls.MANIFEST.version),
            params=dict(resolved_params),
        )


__all__ = ["CandleStatsIndicator"]
