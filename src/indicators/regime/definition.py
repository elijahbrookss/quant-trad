"""Regime indicator definition contract."""

from __future__ import annotations

from typing import Any, Mapping

from indicators.manifest import resolve_manifest_params

from .manifest import MANIFEST


class RegimeIndicator:
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
    ) -> "TypedRegimeIndicator":
        from .runtime import TypedRegimeIndicator, resolve_regime_dependency

        _ = execution_context, source_facts
        dependency_indicator_id = resolve_regime_dependency(
            indicator_id=indicator_id,
            meta=meta,
            strategy_indicator_metas=strategy_indicator_metas,
        )
        return TypedRegimeIndicator(
            indicator_id=indicator_id,
            version=str(meta.get("version") or cls.MANIFEST.version),
            params=dict(resolved_params),
            candle_stats_indicator_id=dependency_indicator_id,
        )


__all__ = ["RegimeIndicator"]
