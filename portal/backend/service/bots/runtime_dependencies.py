"""Portal-side construction of explicit runtime dependencies."""

from __future__ import annotations

from typing import Any

from engines.bot_runtime.deps import BotRuntimeDeps

from .strategy_loader import StrategyLoader
from ..indicators.indicator_service import (
    IndicatorServiceContext,
    build_runtime_indicator_graph,
    build_runtime_indicator_instance,
    get_instance_meta,
    runtime_input_plan_for_instance,
)
from ..market.candle_service import fetch_ohlcv
from ..market.instrument_service import resolve_instrument
from ..reports.artifacts import build_run_artifact_bundle
from ..storage.storage import (
    record_bot_run_steps_batch,
    record_bot_runtime_event,
    record_bot_runtime_events_batch,
    record_bot_trade,
    record_bot_trade_event,
    update_bot_run_artifact,
)
from ..strategies.strategy_service.facade import run_strategy_preview
from .botlens_canonical_facts import append_botlens_canonical_fact_batch


def _build_indicator_context(bot_id: str, overlay_cache: Any) -> Any:
    runtime_indicator_ctx = IndicatorServiceContext.for_bot_runtime(cache_scope_id=bot_id)
    return IndicatorServiceContext.fork_with_overlay_cache(runtime_indicator_ctx, overlay_cache)


def _get_indicator_instance_meta(indicator_id: str, *, ctx: Any = None) -> dict[str, Any]:
    if ctx is None:
        return get_instance_meta(indicator_id)
    return get_instance_meta(indicator_id, ctx=ctx)


def _build_runtime_indicator_instance(
    indicator_id: str,
    *,
    meta: dict[str, Any],
    strategy_indicator_metas: dict[str, dict[str, Any]] | None = None,
    execution_context: Any = None,
) -> Any:
    return build_runtime_indicator_instance(
        indicator_id,
        meta=meta,
        strategy_indicator_metas=strategy_indicator_metas or {},
        execution_context=execution_context,
    )


def _build_runtime_indicator_graph(
    indicator_ids: list[str],
    *,
    strategy_indicator_metas: dict[str, dict[str, Any]] | None = None,
    execution_context: Any = None,
    ctx: Any = None,
) -> tuple[dict[str, dict[str, Any]], list[Any]]:
    if ctx is None:
        return build_runtime_indicator_graph(
            indicator_ids,
            preloaded_metas=strategy_indicator_metas or {},
            execution_context=execution_context,
        )
    return build_runtime_indicator_graph(
        indicator_ids,
        preloaded_metas=strategy_indicator_metas or {},
        execution_context=execution_context,
        ctx=ctx,
    )


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


def build_bot_runtime_deps() -> BotRuntimeDeps:
    return BotRuntimeDeps(
        fetch_strategy=StrategyLoader.fetch_strategy,
        fetch_ohlcv=fetch_ohlcv,
        resolve_instrument=resolve_instrument,
        strategy_evaluate=run_strategy_preview,
        strategy_run_preview=run_strategy_preview,
        indicator_get_instance_meta=_get_indicator_instance_meta,
        indicator_build_runtime_graph=_build_runtime_indicator_graph,
        indicator_build_runtime_instance=_build_runtime_indicator_instance,
        indicator_runtime_input_plan_for_instance=_runtime_input_plan_for_indicator,
        build_indicator_context=_build_indicator_context,
        record_bot_runtime_event=record_bot_runtime_event,
        record_bot_runtime_events_batch=record_bot_runtime_events_batch,
        append_botlens_canonical_fact_batch=append_botlens_canonical_fact_batch,
        record_bot_trade=record_bot_trade,
        record_bot_trade_event=record_bot_trade_event,
        record_bot_run_steps_batch=record_bot_run_steps_batch,
        update_bot_run_artifact=update_bot_run_artifact,
        build_run_artifact_bundle=build_run_artifact_bundle,
    )


__all__ = ["build_bot_runtime_deps"]
