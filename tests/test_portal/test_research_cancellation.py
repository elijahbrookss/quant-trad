from __future__ import annotations

import threading

import pytest

from core.execution_control import (
    ExecutionCancelledError, ExecutionControl, controlled_execution, execution_checkpoint,
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
