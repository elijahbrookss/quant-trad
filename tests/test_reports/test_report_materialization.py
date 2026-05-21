from __future__ import annotations

import pytest

from portal.backend.service.reports import materialization


def _status(status: str = "not_started", *, can_view: bool = False) -> dict:
    return {
        "run_id": "run-1",
        "status": status,
        "contract_version": "run_report_v2",
        "artifact_id": None,
        "artifact_path": None,
        "built_at": None,
        "started_at": None,
        "duration_ms": None,
        "error": None,
        "stale_reason": None,
        "cache_key": None,
        "can_view": can_view,
        "can_build": status in {"not_started", "failed", "stale"},
        "can_retry": status == "failed",
    }


def test_terminal_run_builds_report_without_changing_run_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    stored: dict = {}

    monkeypatch.setattr(materialization.report_data, "get_run", lambda run_id: {"run_id": run_id, "status": "completed"})
    monkeypatch.setattr(materialization.report_data, "get_report_materialization_status", lambda run_id: _status())
    monkeypatch.setattr(
        materialization.report_data,
        "claim_report_materialization_build",
        lambda run_id, **_kwargs: (_status("building"), True, False),
    )
    monkeypatch.setattr(
        materialization,
        "build_run_report",
        lambda run_id: {"contract_version": "run_report_v2", "schema_version": "run_report.v2", "run_id": run_id},
    )

    def store(run_id: str, payload: dict, **kwargs: object) -> dict:
        stored["payload"] = payload
        return _status("ready", can_view=True) | {"duration_ms": kwargs.get("duration_ms")}

    monkeypatch.setattr(materialization.report_data, "store_materialized_run_report", store)

    result = materialization.ensure_report_materialization("run-1", async_build=False)

    assert result["report_status"]["status"] == "ready"
    assert result["report_status"]["can_view"] is True
    assert stored["payload"]["contract_version"] == "run_report_v2"


def test_active_run_does_not_materialize_report(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(materialization.report_data, "get_run", lambda run_id: {"run_id": run_id, "status": "running"})

    with pytest.raises(materialization.RunReportMaterializationNotTerminal):
        materialization.ensure_report_materialization("run-1", async_build=False)


def test_duplicate_report_build_joins_existing_build(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"builder": 0}

    monkeypatch.setattr(materialization.report_data, "get_run", lambda run_id: {"run_id": run_id, "status": "completed"})
    monkeypatch.setattr(materialization.report_data, "get_report_materialization_status", lambda run_id: _status("building"))
    monkeypatch.setattr(
        materialization.report_data,
        "claim_report_materialization_build",
        lambda run_id, **_kwargs: (_status("building"), False, True),
    )

    def fail_if_called(run_id: str) -> dict:
        called["builder"] += 1
        raise AssertionError("duplicate build started")

    monkeypatch.setattr(materialization, "build_run_report", fail_if_called)

    result = materialization.ensure_report_materialization("run-1", async_build=False)

    assert result["report_status"]["status"] == "building"
    assert called["builder"] == 0


def test_report_build_failure_is_recorded_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(materialization.report_data, "get_run", lambda run_id: {"run_id": run_id, "status": "completed"})
    monkeypatch.setattr(materialization.report_data, "get_report_materialization_status", lambda run_id: _status())
    monkeypatch.setattr(
        materialization.report_data,
        "claim_report_materialization_build",
        lambda run_id, **_kwargs: (_status("building"), True, False),
    )
    monkeypatch.setattr(materialization, "build_run_report", lambda _run_id: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(
        materialization.report_data,
        "mark_report_materialization_failed",
        lambda run_id, **kwargs: _status("failed") | {"error": kwargs.get("error")},
    )

    result = materialization.ensure_report_materialization("run-1", async_build=False)

    assert result["report_status"]["status"] == "failed"
    assert "boom" in result["report_status"]["error"]
