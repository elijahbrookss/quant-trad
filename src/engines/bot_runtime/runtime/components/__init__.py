from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .chart_state import ChartStateBuilder
    from .event_sink import InMemoryEventSink, RuntimeEventSink
    from .intrabar import IntrabarManager
    from .persistence_buffer import TradePersistenceBuffer
    from .run_context import RunContext
    from .runtime_policy import RuntimeModePolicy
    from .series_runner import InlineSeriesRunner, PoolSeriesRunner, SeriesRunnerContext
    from .settlement import SettlementApplier
    from .signal_consumption import SignalConsumption, consume_signals
    from .step_trace_buffer import StepTracePersistenceBuffer

__all__ = [
    "ChartStateBuilder",
    "InMemoryEventSink",
    "RuntimeEventSink",
    "IntrabarManager",
    "TradePersistenceBuffer",
    "RunContext",
    "RuntimeModePolicy",
    "InlineSeriesRunner",
    "PoolSeriesRunner",
    "SeriesRunnerContext",
    "SettlementApplier",
    "StepTracePersistenceBuffer",
    "SignalConsumption",
    "consume_signals",
]


def __getattr__(name: str):
    if name == "ChartStateBuilder":
        from .chart_state import ChartStateBuilder

        return ChartStateBuilder
    if name in {"InMemoryEventSink", "RuntimeEventSink"}:
        from .event_sink import InMemoryEventSink, RuntimeEventSink

        return InMemoryEventSink if name == "InMemoryEventSink" else RuntimeEventSink
    if name == "IntrabarManager":
        from .intrabar import IntrabarManager

        return IntrabarManager
    if name == "TradePersistenceBuffer":
        from .persistence_buffer import TradePersistenceBuffer

        return TradePersistenceBuffer
    if name == "RunContext":
        from .run_context import RunContext

        return RunContext
    if name == "RuntimeModePolicy":
        from .runtime_policy import RuntimeModePolicy

        return RuntimeModePolicy
    if name in {"InlineSeriesRunner", "PoolSeriesRunner", "SeriesRunnerContext"}:
        from .series_runner import InlineSeriesRunner, PoolSeriesRunner, SeriesRunnerContext

        if name == "InlineSeriesRunner":
            return InlineSeriesRunner
        return PoolSeriesRunner if name == "PoolSeriesRunner" else SeriesRunnerContext
    if name == "SettlementApplier":
        from .settlement import SettlementApplier

        return SettlementApplier
    if name == "StepTracePersistenceBuffer":
        from .step_trace_buffer import StepTracePersistenceBuffer

        return StepTracePersistenceBuffer
    if name in {"SignalConsumption", "consume_signals"}:
        from .signal_consumption import SignalConsumption, consume_signals

        return SignalConsumption if name == "SignalConsumption" else consume_signals
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
