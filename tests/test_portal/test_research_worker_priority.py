import logging

import pytest

from portal.backend.workers import research_worker as worker


def test_research_worker_lowers_priority_and_reports_actual_value(monkeypatch, caplog):
    calls = []
    monkeypatch.setattr(worker.os, "nice", lambda delta: calls.append(delta) or 10, raising=False)
    with caplog.at_level(logging.INFO, logger=worker.__name__):
        worker._lower_process_priority()
    assert calls == [10]
    assert "research_worker_priority_applied | nice=10" in caplog.text


def test_unsupported_process_priority_is_visible_without_claiming_success(monkeypatch, caplog):
    monkeypatch.delattr(worker.os, "nice", raising=False)
    with caplog.at_level(logging.WARNING, logger=worker.__name__):
        worker._lower_process_priority()
    assert "research_worker_priority_unavailable | reason=os_nice_unsupported" in caplog.text
    assert "priority_applied" not in caplog.text


def test_process_priority_os_failure_is_visible_without_stopping_worker(monkeypatch, caplog):
    def denied(delta):
        raise PermissionError("private operating-system diagnostic")
    monkeypatch.setattr(worker.os, "nice", denied, raising=False)
    with caplog.at_level(logging.WARNING, logger=worker.__name__):
        worker._lower_process_priority()
    assert "research_worker_priority_unavailable | reason=PermissionError" in caplog.text
    assert "priority_applied" not in caplog.text
    assert "private operating-system diagnostic" not in caplog.text


def test_priority_is_applied_before_startup_or_job_admission(monkeypatch):
    order = []
    monkeypatch.setattr(worker, "_configure_logging", lambda: None)
    monkeypatch.setattr(worker, "_lower_process_priority", lambda: order.append("priority"))
    monkeypatch.setattr(worker, "ensure_builtin_overlays_registered", lambda: order.append("overlays"))
    monkeypatch.setattr(worker.signal, "signal", lambda *args: None)
    monkeypatch.setattr(worker, "wait_for_database_ready", lambda **kw: order.append("database") or False)
    monkeypatch.setattr(worker, "start_observability_exporter", lambda: order.append("exporter_started"))
    monkeypatch.setattr(worker, "stop_observability_exporter", lambda: order.append("exporter_stopped"))
    assert worker.main() == 2
    assert order == ["priority", "overlays", "database"]


@pytest.mark.parametrize("execution_fails", [False, True])
def test_worker_owns_exporter_start_and_drain_even_when_execution_fails(monkeypatch, execution_fails):
    order = []
    monkeypatch.setattr(worker, "_configure_logging", lambda: None)
    monkeypatch.setattr(worker, "_lower_process_priority", lambda: None)
    monkeypatch.setattr(worker, "ensure_builtin_overlays_registered", lambda: None)
    monkeypatch.setattr(worker.signal, "signal", lambda *args: None)
    monkeypatch.setattr(worker, "wait_for_database_ready", lambda **kw: True)
    monkeypatch.setattr(worker, "_worker_identity", lambda: ("fixture-worker", 0, 1))
    monkeypatch.setattr(worker, "start_observability_exporter", lambda: order.append("exporter_started"))
    monkeypatch.setattr(worker, "stop_observability_exporter", lambda: order.append("exporter_drained"))
    def run(**identity):
        assert identity == {"worker_id": "fixture-worker", "partition_index": 0, "partition_total": 1}
        order.append("worker")
        if execution_fails:
            raise ValueError("execution failed")
        return 0
    monkeypatch.setattr(worker, "_run_worker", run)
    if execution_fails:
        with pytest.raises(ValueError, match="execution failed"):
            worker.main()
    else:
        assert worker.main() == 0
    assert order == ["exporter_started", "worker", "exporter_drained"]


def test_exporter_shutdown_failure_remains_visible(monkeypatch):
    monkeypatch.setattr(worker, "_configure_logging", lambda: None)
    monkeypatch.setattr(worker, "_lower_process_priority", lambda: None)
    monkeypatch.setattr(worker, "ensure_builtin_overlays_registered", lambda: None)
    monkeypatch.setattr(worker.signal, "signal", lambda *args: None)
    monkeypatch.setattr(worker, "wait_for_database_ready", lambda **kw: True)
    monkeypatch.setattr(worker, "start_observability_exporter", lambda: None)
    monkeypatch.setattr(worker, "_run_worker", lambda **kw: 0)
    def stop():
        raise RuntimeError("exporter did not drain")
    monkeypatch.setattr(worker, "stop_observability_exporter", stop)
    with pytest.raises(RuntimeError, match="exporter did not drain"):
        worker.main()
