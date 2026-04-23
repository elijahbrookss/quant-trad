from __future__ import annotations

from portal.backend.service.bots.bot_run_diagnostics_projection import project_bot_run_diagnostics
from portal.backend.service.bots.startup_lifecycle import terminal_status_after_supervision


def _event(
    seq: int,
    phase: str,
    status: str,
    owner: str,
    message: str,
    *,
    metadata=None,
    failure=None,
    at: str | None = None,
):
    timestamp = at or f"2026-04-09T01:56:{seq:02d}Z"
    return {
        "event_id": f"event-{seq}",
        "run_id": "run-1",
        "bot_id": "bot-1",
        "seq": seq,
        "phase": phase,
        "status": status,
        "owner": owner,
        "message": message,
        "metadata": dict(metadata or {}),
        "failure": dict(failure or {}),
        "checkpoint_at": timestamp,
        "created_at": timestamp,
    }


def _startup_failure_events():
    return [
        _event(1, "start_requested", "starting", "backend", "Backend accepted bot start request."),
        _event(2, "container_launched", "starting", "backend", "Runtime container launched successfully."),
        _event(3, "container_booting", "starting", "container", "Container process booting."),
        _event(
            4,
            "spawning_series_workers",
            "starting",
            "container",
            "Spawning workers.",
            metadata={
                "series_progress": {
                    "workers_planned": 3,
                    "workers_spawned": 3,
                    "failed_series": [],
                    "live_series": [],
                    "series": {
                        "BTC": {"status": "spawned", "worker_id": "worker-1"},
                        "ETH": {"status": "spawned", "worker_id": "worker-2"},
                        "XRP": {"status": "spawned", "worker_id": "worker-3"},
                    },
                }
            },
        ),
        _event(
            5,
            "warming_up_runtime",
            "starting",
            "runtime",
            "Worker warming runtime state.",
            metadata={
                "series_progress": {
                    "workers_planned": 3,
                    "workers_spawned": 3,
                    "failed_series": [],
                    "live_series": [],
                    "series": {
                        "BTC": {"status": "warming_up", "worker_id": "worker-1"},
                        "ETH": {"status": "warming_up", "worker_id": "worker-2"},
                        "XRP": {"status": "warming_up", "worker_id": "worker-3"},
                    },
                }
            },
        ),
        _event(
            6,
            "startup_failed",
            "startup_failed",
            "runtime",
            "Rule contract missing for BTC worker.",
            metadata={
                "series_progress": {
                    "workers_planned": 3,
                    "workers_spawned": 3,
                    "failed_series": ["BTC"],
                    "live_series": [],
                    "series": {
                        "BTC": {"status": "failed", "worker_id": "worker-1"},
                        "ETH": {"status": "warming_up", "worker_id": "worker-2"},
                        "XRP": {"status": "warming_up", "worker_id": "worker-3"},
                    },
                }
            },
            failure={
                "type": "worker_exception",
                "reason_code": "runtime_worker_exception",
                "message": "Rule 7a282500-3502-49ab-acf9-89512316cd05 is missing trigger/guards contract",
                "phase": "startup_failed",
                "owner": "runtime",
                "at": "2026-04-09T01:56:36Z",
                "worker_id": "worker-1",
                "symbol": "BTC",
                "exception_type": "ValueError",
                "traceback": "Traceback ... ValueError",
            },
            at="2026-04-09T01:56:36Z",
        ),
        _event(
            7,
            "startup_failed",
            "startup_failed",
            "runtime",
            "Worker worker-2 exited with code 1",
            metadata={
                "series_progress": {
                    "workers_planned": 3,
                    "workers_spawned": 3,
                    "failed_series": ["BTC", "ETH"],
                    "live_series": [],
                    "series": {
                        "BTC": {"status": "failed", "worker_id": "worker-1"},
                        "ETH": {"status": "failed", "worker_id": "worker-2"},
                        "XRP": {"status": "warming_up", "worker_id": "worker-3"},
                    },
                }
            },
            failure={
                "type": "worker_exit",
                "reason_code": "worker_exit_non_zero",
                "message": "Worker worker-2 exited with code 1",
                "phase": "startup_failed",
                "owner": "runtime",
                "at": "2026-04-09T01:56:37Z",
                "worker_id": "worker-2",
                "symbol": "ETH",
                "exit_code": 1,
            },
            at="2026-04-09T01:56:37Z",
        ),
        _event(
            8,
            "startup_failed",
            "startup_failed",
            "container",
            "Container runtime supervision completed.",
            metadata={
                "series_progress": {
                    "workers_planned": 3,
                    "workers_spawned": 3,
                    "failed_series": ["BTC", "ETH", "XRP"],
                    "live_series": [],
                    "series": {
                        "BTC": {"status": "failed", "worker_id": "worker-1"},
                        "ETH": {"status": "failed", "worker_id": "worker-2"},
                        "XRP": {"status": "failed", "worker_id": "worker-3"},
                    },
                }
            },
            at="2026-04-09T01:56:38Z",
        ),
        _event(
            9,
            "crashed",
            "crashed",
            "watchdog",
            "Bot marked crashed by watchdog: container_not_running:quant-trad-bots-bot-1",
            failure={
                "type": "watchdog_crash",
                "reason_code": "container_not_running",
                "message": "Bot marked crashed by watchdog: container_not_running:quant-trad-bots-bot-1",
                "phase": "crashed",
                "owner": "watchdog",
                "at": "2026-04-09T01:57:07Z",
            },
            at="2026-04-09T01:57:07Z",
        ),
    ]


def test_project_bot_run_diagnostics_marks_completed_and_failed_checkpoints():
    diagnostics = project_bot_run_diagnostics(
        run_id="run-1",
        lifecycle={"run_id": "run-1", "phase": "crashed", "status": "crashed", "owner": "watchdog"},
        events=_startup_failure_events(),
    )

    events = diagnostics["events"]
    assert diagnostics["run_status"] == "crashed"
    assert events[0]["checkpoint_status"] == "completed"
    assert events[1]["checkpoint_status"] == "completed"
    assert events[5]["checkpoint_status"] == "failed"
    assert events[-1]["checkpoint_status"] == "failed"
    assert all("run_status" not in event for event in events[:-1])


def test_project_bot_run_diagnostics_summarizes_startup_failure_before_live():
    diagnostics = project_bot_run_diagnostics(
        run_id="run-1",
        lifecycle={"run_id": "run-1", "phase": "crashed", "status": "crashed", "owner": "watchdog"},
        events=_startup_failure_events(),
    )

    summary = diagnostics["summary"]
    assert summary["root_failure_phase"] == "startup_failed"
    assert summary["root_failure_owner"] == "runtime"
    assert "missing trigger/guards contract" in summary["root_failure_message"]
    assert summary["container_launched"] is True
    assert summary["container_booted"] is True
    assert summary["workers_planned"] == 3
    assert summary["workers_spawned"] == 3
    assert summary["workers_live"] == 0
    assert summary["workers_failed"] == 3
    assert summary["failed_symbols"] == ["BTC", "ETH", "XRP"]
    assert summary["crash_before_any_series_live"] is True


def test_project_bot_run_diagnostics_identifies_first_worker_and_last_success():
    diagnostics = project_bot_run_diagnostics(
        run_id="run-1",
        lifecycle={"run_id": "run-1", "phase": "crashed", "status": "crashed", "owner": "watchdog"},
        events=_startup_failure_events(),
    )

    summary = diagnostics["summary"]
    assert summary["first_failed_worker_id"] == "worker-1"
    assert summary["first_failed_symbol"] == "BTC"
    assert summary["failed_worker_count"] == 3
    assert summary["last_successful_checkpoint"]["phase"] == "warming_up_runtime"
    assert summary["last_successful_checkpoint"]["owner"] == "runtime"


def test_project_bot_run_diagnostics_keeps_last_success_before_later_terminal_observations():
    events = _startup_failure_events()
    events.insert(
        -1,
        _event(
            8,
            "degraded",
            "degraded",
            "container",
            "Container runtime supervision completed.",
            metadata=events[-2]["metadata"],
            at="2026-04-09T01:56:39Z",
        ),
    )
    events[-1]["seq"] = 10

    diagnostics = project_bot_run_diagnostics(
        run_id="run-1",
        lifecycle={"run_id": "run-1", "phase": "crashed", "status": "crashed", "owner": "watchdog"},
        events=events,
    )

    assert diagnostics["summary"]["last_successful_checkpoint"]["phase"] == "warming_up_runtime"


def test_project_bot_run_diagnostics_includes_runtime_state_progress_and_pressure() -> None:
    diagnostics = project_bot_run_diagnostics(
        run_id="run-1",
        lifecycle={"run_id": "run-1", "phase": "degraded", "status": "degraded", "owner": "runtime"},
        events=_startup_failure_events(),
        run_health={
            "status": "degraded",
            "phase": "degraded",
            "runtime_state": "degraded",
            "progress_state": "churning",
            "last_useful_progress_at": "2026-04-09T01:56:35Z",
            "degraded": {
                "active": True,
                "started_at": "2026-04-09T01:56:36Z",
                "reason_code": "subscriber_gap",
            },
            "churn": {
                "active": True,
                "detected_at": "2026-04-09T01:56:39Z",
            },
            "pressure": {
                "top_pressure": {
                    "reason_code": "telemetry_backpressure",
                    "value": 0.75,
                    "unit": "ratio",
                }
            },
            "recent_transitions": [
                {
                    "from_state": "live",
                    "to_state": "degraded",
                    "transition_reason": "continuity_gap:subscriber_gap",
                    "source_component": "worker_bridge",
                    "timestamp": "2026-04-09T01:56:36Z",
                }
            ],
        },
    )

    assert diagnostics["runtime"]["state"] == "degraded"
    assert diagnostics["runtime"]["progress_state"] == "churning"
    assert diagnostics["runtime"]["degraded"]["started_at"] == "2026-04-09T01:56:36Z"
    assert diagnostics["runtime"]["top_pressure"]["reason_code"] == "telemetry_backpressure"
    assert diagnostics["summary"]["runtime_state"] == "degraded"
    assert diagnostics["summary"]["is_churning"] is True


def test_project_bot_run_diagnostics_surfaces_structured_root_failure_details() -> None:
    events = [
        _event(1, "container_launched", "starting", "backend", "Runtime container launched."),
        _event(
            2,
            "degraded",
            "degraded",
            "runtime",
            "Artifact finalization failed for worker.",
            failure={
                "type": "worker_exception",
                "reason_code": "artifact_cleanup_race",
                "message": "Run artifact spool cleanup raced with another worker finalizer.",
                "phase": "degraded",
                "owner": "runtime",
                "at": "2026-04-09T01:56:36Z",
                "worker_id": "worker-3",
                "symbol": "XPP",
                "exception_type": "OSError",
                "component": "report_artifacts",
                "operation": "spool_cleanup",
                "path": "indicators",
                "errno": 39,
            },
            at="2026-04-09T01:56:36Z",
        ),
        _event(
            3,
            "degraded",
            "degraded",
            "container",
            "At least one worker reported degraded terminal state.",
            at="2026-04-09T01:56:37Z",
        ),
    ]

    diagnostics = project_bot_run_diagnostics(
        run_id="run-1",
        lifecycle={"run_id": "run-1", "phase": "degraded", "status": "degraded", "owner": "container"},
        events=events,
    )

    summary = diagnostics["summary"]
    assert summary["root_failure_message"] == "Run artifact spool cleanup raced with another worker finalizer."
    assert summary["root_failure_reason_code"] == "artifact_cleanup_race"
    assert summary["root_failure_exception_type"] == "OSError"
    assert summary["root_failure_worker_id"] == "worker-3"
    assert summary["root_failure"]["component"] == "report_artifacts"
    assert summary["root_failure"]["operation"] == "spool_cleanup"


def test_terminal_status_after_supervision_marks_startup_failed_before_live():
    phase, status = terminal_status_after_supervision(
        startup_live_emitted=False,
        degraded_symbols_count=3,
        telemetry_degraded=True,
    )

    assert phase == "startup_failed"
    assert status == "startup_failed"

    live_phase, live_status = terminal_status_after_supervision(
        startup_live_emitted=True,
        degraded_symbols_count=1,
        telemetry_degraded=False,
    )
    assert live_phase == "degraded"
    assert live_status == "degraded"


def test_terminal_status_after_supervision_requires_explicit_completed_worker_reports():
    phase, status = terminal_status_after_supervision(
        startup_live_emitted=True,
        degraded_symbols_count=0,
        telemetry_degraded=False,
        expected_worker_count=2,
        worker_terminal_statuses={"worker-1": "completed", "worker-2": "completed"},
    )

    assert phase == "completed"
    assert status == "completed"


def test_terminal_status_after_supervision_marks_crashed_when_worker_terminal_reports_are_missing():
    phase, status = terminal_status_after_supervision(
        startup_live_emitted=True,
        degraded_symbols_count=0,
        telemetry_degraded=False,
        expected_worker_count=2,
        worker_terminal_statuses={"worker-1": "completed"},
    )

    assert phase == "crashed"
    assert status == "crashed"


def test_terminal_status_after_supervision_prefers_stopped_over_completed_mix():
    phase, status = terminal_status_after_supervision(
        startup_live_emitted=True,
        degraded_symbols_count=0,
        telemetry_degraded=False,
        expected_worker_count=2,
        worker_terminal_statuses={"worker-1": "completed", "worker-2": "stopped"},
    )

    assert phase == "stopped"
    assert status == "stopped"
