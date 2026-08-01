"""Portal-side construction of explicit runtime dependencies."""

from __future__ import annotations

from contextlib import nullcontext
from copy import copy, deepcopy
from dataclasses import is_dataclass, replace
from typing import Any, Callable, Mapping, Optional

from engines.bot_runtime.deps import BotRuntimeDeps
from market_data.backtest import (
    bound_instrument_for_id,
    bound_instrument_for_symbol,
    build_backtest_execution_config_hash,
    normalize_backtest_dataset_binding,
)

from .strategy_loader import StrategyLoader
from ..indicators.indicator_service import (
    IndicatorServiceContext,
    build_runtime_indicator_graph,
    build_runtime_indicator_instance,
    collect_runtime_indicator_diagnostics,
    get_instance_meta,
    runtime_input_plan_for_instance,
)
from ..market.backtest_dataset_service import (
    resolve_backtest_strategy_identity,
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
    *, dataset_binding: Optional[Mapping[str, Any]] = None
) -> BotRuntimeDeps:
    """Build runtime dependencies, optionally bound to one frozen dataset."""

    normalized_binding = (
        normalize_backtest_dataset_binding(dataset_binding)
        if dataset_binding is not None
        else None
    )

    def scoped(callable_: Callable[..., Any]) -> Callable[..., Any]:
        def invoke(*args: Any, **kwargs: Any) -> Any:
            context = (
                market_data_read_scope(dataset_binding=normalized_binding)
                if normalized_binding is not None
                else nullcontext()
            )
            with context:
                return callable_(*args, **kwargs)

        return invoke

    def bound_strategy_instruments(strategy: Any) -> Any:
        if normalized_binding is None:
            return strategy
        links = list(getattr(strategy, "instrument_links", None) or [])
        linked_ids = [
            str(getattr(link, "instrument_id", "") or "").strip()
            for link in links
        ]
        expected_ids = [
            str(row["instrument_id"])
            for row in normalized_binding["instruments"]
        ]
        if sorted(linked_ids) != sorted(expected_ids) or any(
            not value for value in linked_ids
        ):
            raise RuntimeError(
                "backtest_strategy_substitution_forbidden: strategy instrument links "
                f"differ expected={sorted(expected_ids)} actual={sorted(linked_ids)}"
            )
        rebound_links = []
        for link in links:
            instrument_id = str(getattr(link, "instrument_id", "") or "").strip()
            snapshot = bound_instrument_for_id(normalized_binding, instrument_id)
            if is_dataclass(link):
                rebound_links.append(
                    replace(link, instrument_snapshot=deepcopy(snapshot))
                )
            else:
                rebound = copy(link)
                setattr(rebound, "instrument_snapshot", deepcopy(snapshot))
                rebound_links.append(rebound)
        if is_dataclass(strategy):
            return replace(strategy, instrument_links=rebound_links)
        rebound_strategy = copy(strategy)
        setattr(rebound_strategy, "instrument_links", rebound_links)
        return rebound_strategy

    def fetch_strategy(strategy_id: str, runtime_config: dict | None = None) -> Any:
        strategy = StrategyLoader.fetch_strategy(strategy_id, runtime_config)
        if normalized_binding is None:
            return strategy
        identity = resolve_backtest_strategy_identity(strategy)
        disagreements: list[str] = []
        for field in (
            "strategy_id",
            "strategy_hash",
            "indicator_config_hash",
            "execution_policy_hash",
        ):
            if str(identity.get(field) or "") != str(
                normalized_binding.get(field) or ""
            ):
                disagreements.append(field)
        expected_effective = str(
            normalized_binding.get("effective_strategy_config_hash") or ""
        ).strip()
        if expected_effective and expected_effective != str(
            identity.get("effective_strategy_config_hash") or ""
        ).strip():
            disagreements.append("effective_strategy_config_hash")
        actual_execution_config_hash = build_backtest_execution_config_hash(
            bot=dict(runtime_config or {}),
            strategy_identity=identity,
            instrument_config_hash=str(
                normalized_binding["instrument_config_hash"]
            ),
        )
        if actual_execution_config_hash != str(
            normalized_binding["execution_config_hash"]
        ):
            disagreements.append("execution_config_hash")
        if disagreements:
            raise RuntimeError(
                "backtest_strategy_substitution_forbidden: admitted execution "
                f"identity differs fields={','.join(disagreements)}"
            )
        return bound_strategy_instruments(strategy)

    def runtime_get_instrument_record(instrument_id: str) -> dict[str, Any]:
        if normalized_binding is None:
            return get_instrument_record(instrument_id)
        return deepcopy(bound_instrument_for_id(normalized_binding, instrument_id))

    def runtime_resolve_instrument(
        datasource: Optional[str],
        exchange: Optional[str],
        symbol: str,
    ) -> Optional[dict[str, Any]]:
        if normalized_binding is None:
            return resolve_instrument(datasource, exchange, symbol)
        return deepcopy(
            bound_instrument_for_symbol(
                normalized_binding,
                datasource=datasource,
                exchange=exchange,
                symbol=symbol,
            )
        )

    return BotRuntimeDeps(
        fetch_strategy=fetch_strategy,
        fetch_ohlcv=scoped(fetch_ohlcv),
        fetch_ohlcv_by_instrument=scoped(fetch_ohlcv_by_instrument),
        get_instrument_record=runtime_get_instrument_record,
        resolve_instrument=runtime_resolve_instrument,
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
