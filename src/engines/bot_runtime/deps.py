"""Explicit runtime dependency bundle for real cross-boundary collaborators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence


@dataclass(frozen=True)
class BotRuntimeDeps:
    """Concrete runtime collaborators supplied by the portal composition layer."""

    fetch_strategy: Callable[[str], Any]
    fetch_ohlcv: Callable[..., Any]
    resolve_instrument: Callable[[Optional[str], Optional[str], Optional[str]], Optional[Dict[str, Any]]]
    strategy_evaluate: Callable[..., Dict[str, Any]]
    strategy_run_preview: Callable[..., Dict[str, Any]]
    indicator_get_instance_meta: Callable[..., Dict[str, Any]]
    indicator_build_runtime_graph: Callable[..., tuple[dict[str, dict[str, Any]], list[Any]]]
    indicator_build_runtime_instance: Callable[..., Any]
    indicator_collect_runtime_diagnostics: Callable[[Sequence[Any]], list[Dict[str, Any]]]
    indicator_runtime_input_plan_for_instance: Callable[..., Dict[str, Any]]
    build_indicator_context: Callable[[str, Any], Any]
    record_bot_runtime_event: Callable[[Mapping[str, Any]], None]
    record_bot_runtime_events_batch: Callable[[list[dict[str, Any]]], int]
    record_bot_trade: Callable[[Mapping[str, Any]], None]
    record_bot_trade_event: Callable[[Mapping[str, Any]], None]
    record_bot_run_steps_batch: Callable[[list[dict[str, Any]]], int]
    build_run_artifact_bundle: Callable[[str, str, Mapping[str, Any], Sequence[Any]], Any]
    fetch_ohlcv_by_instrument: Optional[Callable[..., Any]] = None
    get_instrument_record: Optional[Callable[[str], Dict[str, Any]]] = None
    append_botlens_canonical_fact_batch: Optional[Callable[..., Mapping[str, Any]]] = None
    append_botlens_canonical_fact_batches: Optional[Callable[..., Mapping[str, Any]]] = None
    record_bot_runtime_diagnostic_event: Optional[Callable[[Mapping[str, Any]], None]] = None

__all__ = ["BotRuntimeDeps"]
