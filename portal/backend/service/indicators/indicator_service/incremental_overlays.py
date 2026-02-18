from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


def build_incremental_overlay_indicator(
    *,
    indicator_type: str,
    instance: Any,
    df: Any,
    inst_id: str,
    symbol: str,
    interval: str,
    overlay_options: Mapping[str, Any],
    provider: Any,
    data_ctx: Any,
    context: Any,
) -> Optional[Any]:
    builder = _BUILDERS.get(str(indicator_type or "").strip().lower())
    if builder is None:
        return None
    return builder(
        instance=instance,
        df=df,
        inst_id=inst_id,
        symbol=symbol,
        interval=interval,
        overlay_options=overlay_options,
        provider=provider,
        data_ctx=data_ctx,
        context=context,
    )


def _market_profile_incremental_overlay_indicator(
    *,
    instance: Any,
    df: Any,
    inst_id: str,
    symbol: str,
    interval: str,
    overlay_options: Mapping[str, Any],
    provider: Any,
    data_ctx: Any,
    context: Any,
) -> Any:
    overrides = _market_profile_overrides(overlay_options)
    return context.breakout_cache.build_market_profile_overlay_indicator(
        instance,
        df,
        interval=interval,
        symbol=symbol,
        provider=provider,
        data_ctx=data_ctx,
        profile_cache=context.incremental_cache,
        inst_id=inst_id,
        **overrides,
    )


def _market_profile_overrides(options: Mapping[str, Any]) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    if "use_merged_value_areas" in options:
        overrides["use_merged_value_areas"] = options["use_merged_value_areas"]
    if "merge_threshold" in options:
        overrides["merge_threshold"] = options["merge_threshold"]
    if "min_merge_sessions" in options:
        overrides["min_merge_sessions"] = options["min_merge_sessions"]
    if "extend_value_area_to_chart_end" in options:
        overrides["extend_value_area_to_chart_end"] = options["extend_value_area_to_chart_end"]
    return overrides


_BUILDERS = {
    "market_profile": _market_profile_incremental_overlay_indicator,
}

