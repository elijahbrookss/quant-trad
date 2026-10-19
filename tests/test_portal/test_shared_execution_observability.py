"""Shared operational observations never enter scientific output material."""
from contextlib import contextmanager
import json
import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from core.execution_control import (
    ExecutionBudgetExceededError, ExecutionCancelledError, ExecutionControl,
    controlled_execution, current_execution_control, measure_execution_stage,
    observe_execution_size,
)


@pytest.mark.parametrize("error,status", [
    (None, "completed"), (ValueError("bad"), "failed"),
    (ExecutionCancelledError("stop"), "cancelled"),
    (ExecutionBudgetExceededError("limit"), "budget_exceeded"),
])
def test_shared_stage_records_outcome_and_preserves_exception(error, status):
    control = ExecutionControl()
    with controlled_execution(control):
        if error is None:
            with measure_execution_stage("evaluation"):
                pass
        else:
            with pytest.raises(type(error)) as caught:
                with measure_execution_stage("evaluation"):
                    raise error
            assert caught.value is error
    metrics = control.snapshot()
    assert metrics["stage_seconds"]["evaluation"] >= 0
    assert metrics["stage_outcomes"]["evaluation"][status] == 1
    assert sum(metrics["stage_outcomes"]["evaluation"].values()) == 1


def test_check_phases_sizes_are_shared_and_do_not_inspect_lazy_inputs(monkeypatch):
    from portal.backend.service.research import execution
    class Lazy:
        def __iter__(self):
            raise AssertionError("metrics enumerated input")
        def __len__(self):
            raise AssertionError("metrics counted lazy input")
    expected = {"status": "completed", "events": [{"key": "example"}]}
    evaluator = SimpleNamespace(evaluate=lambda **kwargs: expected)
    monkeypatch.setattr(execution.CHECK_REGISTRY, "resolve_evaluator", lambda definition: evaluator)
    frame = pd.DataFrame({"close": [1., 2.]})
    monkeypatch.setattr(execution.candle_service, "fetch_ohlcv_by_instrument", lambda *a, **k: frame)
    plan = SimpleNamespace(
        materialization_range={"start": "2025-01-01T00:00Z", "end_exclusive": "2025-01-01T01:00Z"},
        market_data_requirements=[{"alias": "primary_bars", "instrument_id": "example"}],
        quality_evidence=[], gap_policy="reject", missing_coverage=[], indicator_graph=[], execution={},
    )
    request = SimpleNamespace(scope={"timeframe": "15m"}, parameters={})
    control = ExecutionControl()
    def forbidden_encoding(*args, **kwargs):
        raise AssertionError("metrics serialized a payload")
    monkeypatch.setattr(json.JSONEncoder, "iterencode", forbidden_encoding)
    with controlled_execution(control):
        inputs = execution._load_market_inputs(None, request, plan, resolver=SimpleNamespace(dataset_binding=None))
        result = execution._evaluate(None, request, plan, inputs)
        assert control.snapshot()["observed_sizes"]["check_input_candle_rows"] == 2
        lazy_result = execution._evaluate(None, request, plan, {"candles": Lazy()})
        observe_execution_size("known_empty", 0)
    assert result == lazy_result
    assert result["events"] == expected["events"]
    assert "observed_sizes" not in result and "stage_outcomes" not in result
    metrics = control.snapshot()
    assert metrics["stage_outcomes"]["input_preparation"]["completed"] == 1
    assert metrics["stage_outcomes"]["evaluation"]["completed"] == 2
    assert metrics["observed_sizes"]["check_input_candle_rows"] is None
    assert metrics["observed_sizes"]["check_evaluator_result_fields"] == len(expected)
    assert metrics["observed_sizes"]["known_empty"] == 0


def test_worker_owned_control_logs_one_outer_operation_and_restores_context(monkeypatch, caplog):
    from portal.backend.service.research import execution_limits as limits
    @contextmanager
    def scope(**kwargs):
        yield current_execution_control()
    monkeypatch.setattr(limits, "research_execution_scope", scope)
    @limits.bounded_research_execution
    def inner():
        raise ValueError("preserved")
    @limits.bounded_research_execution
    def outer():
        inner()
    control = ExecutionControl()
    with caplog.at_level(logging.INFO, logger=limits.__name__):
        with controlled_execution(control):
            with pytest.raises(ValueError, match="preserved"):
                outer()
            assert not limits._METRICS_OPERATION_ACTIVE.get()
    records = [r.getMessage() for r in caplog.records if "research_execution_metrics |" in r.getMessage()]
    assert len(records) == 1
    assert "operation=outer status=failed error_type=ValueError" in records[0]
    assert "preserved" not in records[0]  # No exception payload copied into telemetry.


@pytest.mark.parametrize("when", ["entry", "exit"])
def test_outer_observation_includes_scope_entry_and_unwind_failure(monkeypatch, caplog, when):
    from portal.backend.service.research import execution_limits as limits
    from core.execution_control import ExecutionStopUncertainError
    error = limits.ResearchAdmissionError("private admission detail") if when == "entry" else ExecutionStopUncertainError("private shutdown detail")
    seen = []
    @contextmanager
    def scope(**kwargs):
        if when == "entry":
            raise error
        control = ExecutionControl()
        def unsafe_snapshot():
            pytest.fail("uncertain shutdown must not acquire the control snapshot lock")
        monkeypatch.setattr(control, "snapshot", unsafe_snapshot)
        yield control
        seen.append("scope_exit")
        raise error
    monkeypatch.setattr(limits, "research_execution_scope", scope)
    @limits.bounded_research_execution
    def operation():
        seen.append("body")
        return {}
    with caplog.at_level(logging.INFO, logger=limits.__name__):
        with pytest.raises(type(error)) as caught:
            operation()
    assert caught.value is error
    assert seen == ([] if when == "entry" else ["body", "scope_exit"])
    records = [r.getMessage() for r in caplog.records if "research_execution_metrics |" in r.getMessage()]
    assert len(records) == 1
    assert "status=failed" in records[0]
    assert "error_type=" + type(error).__name__ in records[0]
    assert "private" not in records[0]
    assert "metrics=null" in records[0]
    assert not limits._METRICS_OPERATION_ACTIVE.get()
