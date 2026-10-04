from __future__ import annotations

import threading

import pytest

from core.execution_control import (
    ExecutionCancelledError, ExecutionControl, controlled_execution, execution_checkpoint,
    ExecutionBudgetExceededError, consume_execution_resource,
)
from portal.backend.service.research import async_dispatch


def test_cancellation_is_scoped_and_checks_before_and_after_execution():
    control = ExecutionControl()
    callbacks = []
    with pytest.raises(ExecutionCancelledError):
        with controlled_execution(control):
            control.register("sql", lambda: callbacks.append("interrupted"))
            control.stop(ExecutionCancelledError("job stopped"))
            control.unregister("sql")
    assert callbacks == ["interrupted"]
    execution_checkpoint()
    with pytest.raises(ExecutionCancelledError):
        with controlled_execution(control):
            pytest.fail("already cancelled execution was admitted")


def test_running_cancellation_is_not_a_result():
    payload = async_dispatch._job_payload({
        "id": "job", "status": "running", "heartbeat_at": "2026-10-04T00:00:00Z",
        "result": {"schema_version": "async_job_cancellation.v1", "execution_stopped": False},
    }, include_result=True)
    assert payload["result_available"] is False
    assert payload["cancellation"]["execution_stopped"] is False
    assert "result" not in payload and "result_summary" not in payload


def test_interrupt_registration_does_not_cancel_a_released_resource():
    control = ExecutionControl()
    seen = []
    control.register("sql", lambda: seen.append("cancelled"))
    control.unregister("sql")
    thread = threading.Thread(target=control.stop, args=(ExecutionCancelledError("stop"),))
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert seen == []


def test_cancel_route_preserves_acknowledgement_state(monkeypatch):
    from fastapi.testclient import TestClient
    from portal.backend.main import app
    monkeypatch.setattr(async_dispatch, "cancel_research_job", lambda job_id: {
        "job_id": job_id, "status": "running",
        "cancellation": {"execution_stopped": False},
    })
    response = TestClient(app).post("/api/research/jobs/job-1/cancel")
    assert response.status_code == 200
    assert response.json()["cancellation"]["execution_stopped"] is False


def test_cancellation_cli_uses_shared_adapter(monkeypatch, capsys):
    from cli import main
    from cli.research_operations import ResearchOperations
    seen = []
    monkeypatch.setattr(ResearchOperations, "cancel_job", lambda self, job_id: seen.append(job_id) or {
        "job_id": job_id, "status": "running", "cancellation": {"execution_stopped": False},
    })
    assert main.main(["research", "jobs", "cancel", "job-1"]) == 0
    assert seen == ["job-1"]
    assert "stop not yet acknowledged" in capsys.readouterr().out


def test_nested_research_calls_share_the_budget_and_release_admission(monkeypatch):
    from portal.backend.service.research import execution_limits as limits
    from threading import BoundedSemaphore
    monkeypatch.setattr(limits, "_SYNC_SLOTS", BoundedSemaphore(1))
    monkeypatch.setattr(limits, "research_execution_limits", lambda: {"seconds": 10, "input_rows": 3})

    @limits.bounded_research_execution
    def inner():
        consume_execution_resource("input_rows", 2)
        return {}

    @limits.bounded_research_execution
    def outer():
        inner()
        inner()

    with pytest.raises(ExecutionBudgetExceededError, match="input_rows"):
        outer()
    assert inner() == {}
    assert limits._SYNC_SLOTS.acquire(blocking=False)
    try:
        with pytest.raises(limits.ResearchAdmissionError):
            inner()
    finally:
        limits._SYNC_SLOTS.release()


def test_deadline_interrupts_registered_io_without_waiting_for_a_checkpoint():
    control = ExecutionControl()
    control.limit(seconds=0.05)
    interrupted = threading.Event()
    with pytest.raises(ExecutionBudgetExceededError, match="elapsed_seconds"):
        with controlled_execution(control):
            control.register("blocked-io", interrupted.set)
            assert interrupted.wait(2)
            control.unregister("blocked-io")


def test_sync_evidence_owns_one_transaction_and_checks_before_commit(monkeypatch):
    from contextlib import contextmanager
    from portal.backend.service.research import service, execution_limits
    from core.execution_control import current_execution_control

    seen = []
    @contextmanager
    def transaction():
        try:
            yield "owned-session"
            execution_checkpoint()
            seen.append("commit")
        except Exception:
            seen.append("rollback")
            raise

    monkeypatch.setattr(service.db, "session", transaction)
    monkeypatch.setattr(service, "build_research_check_evidence", lambda _: {})
    def publish(built, *, session):
        assert session == "owned-session"
        assert current_execution_control() is not None
        seen.append("persist")
        return {"result": "x" * 100}
    monkeypatch.setattr(service, "persist_built_research_check_evidence", publish)
    monkeypatch.setattr(execution_limits, "research_execution_limits", lambda: {"seconds": 60, "result_bytes": 10})
    with pytest.raises(ExecutionBudgetExceededError, match="result_bytes"):
        service.run_research_check({})
    assert seen == ["persist", "rollback"]


def test_confirmed_publication_does_not_fail_on_a_later_deadline():
    control = ExecutionControl()
    with controlled_execution(control, check_on_exit=False):
        # Models a confirmed commit whose return path crosses the deadline.
        control.stop(ExecutionBudgetExceededError("after confirmed commit"))
