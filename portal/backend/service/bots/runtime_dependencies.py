"""Portal-side construction of explicit runtime dependencies."""

from __future__ import annotations

from typing import Any

from engines.bot_runtime.deps import BotRuntimeDeps

from .runtime_derived_state import build_runtime_series_derived_state
from .strategy_loader import StrategyLoader
from ..indicators.indicator_service import (
    IndicatorServiceContext,
    get_instance_meta,
    overlays_for_instance,
    runtime_input_plan_for_instance,
)
from ..market.candle_service import fetch_ohlcv
from ..market.instrument_service import resolve_instrument
from ..reports.report_service import record_run_report
from ..storage.storage import (
    record_bot_run_steps_batch,
    record_bot_runtime_event,
    record_bot_runtime_events_batch,
    record_bot_trade,
    record_bot_trade_event,
    update_bot_run_artifact,
)
from ..strategies.strategy_service.facade import evaluate, generate_strategy_signals


def _build_indicator_context(bot_id: str, overlay_cache: Any) -> Any:
    runtime_indicator_ctx = IndicatorServiceContext.for_bot_runtime(cache_scope_id=bot_id)
    return IndicatorServiceContext.fork_with_overlay_cache(runtime_indicator_ctx, overlay_cache)


def _get_indicator_instance_meta(indicator_id: str, *, ctx: Any = None) -> dict[str, Any]:
    if ctx is None:
        return get_instance_meta(indicator_id)
    return get_instance_meta(indicator_id, ctx=ctx)


def _runtime_input_plan_for_indicator(
    indicator_id: str,
    *,
    strategy_interval: str,
    start: str,
    end: str,
    ctx: Any = None,
) -> dict[str, Any]:
    if ctx is None:
        return runtime_input_plan_for_instance(
            indicator_id,
            strategy_interval=strategy_interval,
            start=start,
            end=end,
        )
    return runtime_input_plan_for_instance(
        indicator_id,
        strategy_interval=strategy_interval,
        start=start,
        end=end,
        ctx=ctx,
    )


def _indicator_overlays_for_instance(
    indicator_id: str,
    *,
    start: str,
    end: str,
    interval: str,
    symbol: str | None,
    datasource: str | None,
    exchange: str | None,
    overlay_options: Any = None,
    ctx: Any = None,
) -> dict[str, Any]:
    kwargs = dict(
        start=start,
        end=end,
        interval=interval,
        symbol=symbol,
        datasource=datasource,
        exchange=exchange,
        overlay_options=overlay_options,
    )
    if ctx is not None:
        kwargs["ctx"] = ctx
    return overlays_for_instance(indicator_id, **kwargs)


def build_bot_runtime_deps() -> BotRuntimeDeps:
    return BotRuntimeDeps(
        fetch_strategy=StrategyLoader.fetch_strategy,
        fetch_ohlcv=fetch_ohlcv,
        resolve_instrument=resolve_instrument,
        strategy_evaluate=evaluate,
        strategy_generate_signals=generate_strategy_signals,
        indicator_get_instance_meta=_get_indicator_instance_meta,
        indicator_runtime_input_plan_for_instance=_runtime_input_plan_for_indicator,
        indicator_overlays_for_instance=_indicator_overlays_for_instance,
        build_indicator_context=_build_indicator_context,
        build_runtime_series_derived_state=build_runtime_series_derived_state,
        record_bot_runtime_event=record_bot_runtime_event,
        record_bot_runtime_events_batch=record_bot_runtime_events_batch,
        record_bot_trade=record_bot_trade,
        record_bot_trade_event=record_bot_trade_event,
        record_bot_run_steps_batch=record_bot_run_steps_batch,
        update_bot_run_artifact=update_bot_run_artifact,
        record_run_report=record_run_report,
    )


__all__ = ["build_bot_runtime_deps"]
