"""Portal-side construction of explicit runtime dependencies."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Callable, Optional

from engines.bot_runtime.deps import BotRuntimeDeps

from .strategy_loader import StrategyLoader
from ..indicators.indicator_service import (
    IndicatorServiceContext,
    build_runtime_indicator_graph,
    build_runtime_indicator_instance,
    collect_runtime_indicator_diagnostics,
    get_instance_meta,
    runtime_input_plan_for_instance,
)
from ..market.candle_service import (
    fetch_ohlcv,
    fetch_ohlcv_by_instrument,
    market_data_read_scope,
)
from ..market.instrument_service import get_instrument_record, resolve_instrument
from ..reports.artifacts import build_run_artifact_bundle
from ..storage.repos.runtime_events import (
    record_bot_run_steps_batch,
    record_bot_runtime_event,
    record_bot_runtime_events_batch,
)
from ..storage.repos.trades import (
    record_bot_trade,
    record_bot_trade_event,
)
from ..storage.repos.observability import record_observability_events_batch
from ..strategies.strategy_service.facade import run_strategy_preview
from .botlens_canonical_facts import append_botlens_canonical_fact_batch, append_botlens_canonical_fact_batches


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


def _record_bot_runtime_diagnostic_event(payload: Any) -> None:
    if not isinstance(payload, dict):
        return
    record_observability_events_batch([payload])


def build_bot_runtime_deps(
    *, market_data_as_of_commit_seq: Optional[int] = None
) -> BotRuntimeDeps:
    """Build runtime dependencies, optionally pinned to one market commit."""

    def scoped(callable_: Callable[..., Any]) -> Callable[..., Any]:
        def invoke(*args: Any, **kwargs: Any) -> Any:
            context = (
                market_data_read_scope(
                    as_of_commit_seq=int(market_data_as_of_commit_seq)
                )
                if market_data_as_of_commit_seq is not None
                else nullcontext()
            )
            with context:
                return callable_(*args, **kwargs)

        return invoke

    return BotRuntimeDeps(
        fetch_strategy=StrategyLoader.fetch_strategy,
        fetch_ohlcv=scoped(fetch_ohlcv),
        fetch_ohlcv_by_instrument=scoped(fetch_ohlcv_by_instrument),
        get_instrument_record=get_instrument_record,
        resolve_instrument=resolve_instrument,
        strategy_evaluate=scoped(run_strategy_preview),
        strategy_run_preview=scoped(run_strategy_preview),
        indicator_get_instance_meta=_get_indicator_instance_meta,
        indicator_build_runtime_graph=_build_runtime_indicator_graph,
        indicator_build_runtime_instance=_build_runtime_indicator_instance,
        indicator_collect_runtime_diagnostics=collect_runtime_indicator_diagnostics,
        indicator_runtime_input_plan_for_instance=_runtime_input_plan_for_indicator,
        build_indicator_context=_build_indicator_context,
        record_bot_runtime_event=record_bot_runtime_event,
        record_bot_runtime_events_batch=record_bot_runtime_events_batch,
        append_botlens_canonical_fact_batch=append_botlens_canonical_fact_batch,
        append_botlens_canonical_fact_batches=append_botlens_canonical_fact_batches,
        record_bot_trade=record_bot_trade,
        record_bot_trade_event=record_bot_trade_event,
        record_bot_run_steps_batch=record_bot_run_steps_batch,
        build_run_artifact_bundle=build_run_artifact_bundle,
        record_bot_runtime_diagnostic_event=_record_bot_runtime_diagnostic_event,
    )


__all__ = ["build_bot_runtime_deps"]
