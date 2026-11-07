"""Factory for the single candle VWAP runtime timeline."""

from indicators.manifest import resolve_manifest_params

from .manifest import MANIFEST, NATIVE_TIMEFRAMES


class CandleVWAPIndicator:
    NAME = MANIFEST.type
    MANIFEST = MANIFEST

    @classmethod
    def resolve_config(cls, params, *, strict_unknown=False):
        resolved = resolve_manifest_params(cls.MANIFEST, params, strict_unknown=strict_unknown)
        if not isinstance(resolved["source_timeframe"], str) or resolved["source_timeframe"] not in NATIVE_TIMEFRAMES:
            raise ValueError("candle_vwap_config_invalid: supported native intraday source_timeframe required")
        return resolved

    @classmethod
    def build_runtime_indicator(cls, *, indicator_id, meta, resolved_params,
                                strategy_indicator_metas, execution_context=None, source_facts=None):
        from .runtime import TypedCandleVWAPIndicator

        params = cls.resolve_config(resolved_params, strict_unknown=True)
        if execution_context is None:
            raise ValueError(f"candle_vwap_source_invalid: execution context required indicator_id={indicator_id}")
        execution_context.validate()
        if execution_context.interval != params["source_timeframe"]:
            raise ValueError(f"candle_vwap_source_invalid: primary native timeframe must equal source_timeframe indicator_id={indicator_id}")
        version = str(meta.get("version") or cls.MANIFEST.version)
        if version != cls.MANIFEST.version:
            raise ValueError("candle_vwap_config_invalid: unsupported runtime version")
        return TypedCandleVWAPIndicator(indicator_id=indicator_id, version=version, params=params)
