from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import bot_watchdog as watchdog_module


def _utc_iso(delta: timedelta = timedelta()) -> str:
    return (datetime.now(timezone.utc) + delta).isoformat().replace("+00:00", "Z")


def _lease(
    *,
    bot_id: str = "bot-1",
    run_id: str = "run-1",
    runner_id: str = "backend.quanttrad",
    expires_delta: timedelta = timedelta(seconds=60),
) -> dict:
    return {
        "run_id": run_id,
        "bot_id": bot_id,
        "runner_id": runner_id,
        "status": "active",
        "renewed_at": _utc_iso(timedelta(seconds=-5)),
        "expires_at": _utc_iso(expires_delta),
        "released_at": None,
    }


def _patch_run_context(
    monkeypatch: pytest.MonkeyPatch,
    *,
    bot_id: str = "bot-1",
    run_id: str = "run-1",
    status: str = "starting",
    phase: str = "launching_container",
    started_at: str | None = None,
) -> None:
    start = started_at or _utc_iso(timedelta(seconds=-5))
    monkeypatch.setattr(
        watchdog_module,
        "get_bot_run",
        lambda _run_id: {
            "run_id": run_id,
            "bot_id": bot_id,
            "status": status,
            "started_at": start,
            "created_at": start,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "get_bot_run_lifecycle",
        lambda _run_id: {
            "run_id": run_id,
            "bot_id": bot_id,
            "status": status,
            "phase": phase,
            "checkpoint_at": start,
            "updated_at": start,
        },
    )


def test_tick_all_prunes_terminal_run_registrations_without_writing_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    watchdog.register_bot("bot-1", run_id="run-terminal")
    watchdog.register_bot("bot-1", run_id="run-active")
    watchdog.register_bot("bot-2")
    monkeypatch.setattr(
        watchdog_module,
        "list_active_bot_run_leases",
        lambda: [_lease(bot_id="bot-1", run_id="run-active")],
    )

    watchdog.tick_all()

    status = watchdog.status()
    assert status["registered_bots"] == 1
    assert status["registered_bot_ids"] == ["bot-1"]
    assert status["registered_runs"] == [
        {"bot_id": "bot-1", "run_id": "run-active"}
    ]


def test_verify_container_ownership_respects_startup_grace_for_missing_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []

    monkeypatch.setattr(watchdog_module, "list_active_bot_run_leases", lambda: [_lease()])
    _patch_run_context(monkeypatch, status="starting", phase="launching_container")
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id, **_kwargs: {
            "name": "quant-trad-bots-bot-1-run-1",
            "status": "missing",
            "running": False,
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None, run_id=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_does_not_fail_new_run_for_old_exited_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []

    monkeypatch.setattr(watchdog_module, "list_active_bot_run_leases", lambda: [_lease(run_id="new-run")])
    _patch_run_context(monkeypatch, run_id="new-run", status="starting", phase="container_launched")
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id, **_kwargs: {
            "name": "quant-trad-bots-bot-1-new-run",
            "status": "exited",
            "running": False,
            "runtime_run_id": "old-run",
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None, run_id=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_marks_confirmed_owned_container_after_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str, dict]] = []
    old_start = _utc_iso(timedelta(seconds=-120))

    monkeypatch.setattr(watchdog_module, "list_active_bot_run_leases", lambda: [_lease()])
    _patch_run_context(monkeypatch, status="starting", phase="launching_container", started_at=old_start)
    monkeypatch.setattr(watchdog_module, "get_bot_run_lease", lambda _run_id: _lease())
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id, **_kwargs: {
            "name": "quant-trad-bots-bot-1-run-1",
            "status": "exited",
            "running": False,
            "runtime_run_id": "run-1",
            "error": None,
        },
    )
    monkeypatch.setattr(watchdog_module, "latest_docker_lifecycle_event_for_bot", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None, run_id=None: marked.append((bot_id, reason, dict(diagnostics or {}))) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == ["bot-1"]
    assert marked[0][0] == "bot-1"
    assert marked[0][1] == "container_not_running:quant-trad-bots-bot-1-run-1"
    assert marked[0][2]["run_id"] == "run-1"
    assert marked[0][2]["run_lease"]["runner_id"] == "backend.quanttrad"


def test_verify_container_ownership_skips_expired_lease_for_stale_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []

    monkeypatch.setattr(watchdog_module, "list_active_bot_run_leases", lambda: [_lease(expires_delta=timedelta(seconds=-60))])
    _patch_run_context(monkeypatch, status="running", phase="live")
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None, run_id=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_scan_expired_run_leases_persists_runner_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    watchdog = watchdog_module.BotWatchdog()
    watchdog._runner_id = "current-runner"
    marked: list[tuple[str, str, dict]] = []
    expired = _lease(expires_delta=timedelta(seconds=-125))

    monkeypatch.setattr(
        watchdog_module,
        "find_expired_bot_run_leases",
        lambda stale_threshold_seconds, runner_id=None: [expired],
    )
    monkeypatch.setattr(
        watchdog_module,
        "latest_runner_clock_gap",
        lambda runner_id=None, max_age_seconds=900.0: {
            "runner_id": "backend.quanttrad",
            "gap_seconds": 3672.0,
            "detected_at": "2026-05-19T07:57:54Z",
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "latest_docker_lifecycle_event_for_bot",
        lambda bot_id, max_age_seconds=900.0: {
            "bot_id": bot_id,
            "action": "die",
            "exit_code": 137,
            "observed_at": "2026-05-19T13:43:23Z",
        },
    )
    _patch_run_context(monkeypatch)
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None, run_id=None: marked.append((bot_id, reason, dict(diagnostics or {}))) or True,
    )

    crashed = watchdog.scan_expired_run_leases()

    assert crashed == ["bot-1"]
    assert marked[0][0] == "bot-1"
    assert marked[0][1] == "stale_run_lease:prev=backend.quanttrad"
    assert marked[0][2]["detected_runner_id"] == "current-runner"
    assert marked[0][2]["previous_runner"] == "backend.quanttrad"
    assert marked[0][2]["run_id"] == "run-1"
    assert marked[0][2]["lease_expired_age_seconds"] >= 120.0
    assert marked[0][2]["runner_clock_gap"]["gap_seconds"] == 3672.0
    assert marked[0][2]["docker_lifecycle"]["action"] == "die"


def test_recover_local_orphans_uses_expired_leases_for_this_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    watchdog = watchdog_module.BotWatchdog()
    watchdog._runner_id = "current-runner"
    expired = _lease(runner_id="current-runner", expires_delta=timedelta(seconds=-5))
    marked: list[tuple[str, str, dict]] = []

    monkeypatch.setattr(
        watchdog_module,
        "find_expired_bot_run_leases",
        lambda stale_threshold_seconds, runner_id=None: [expired],
    )
    monkeypatch.setattr(watchdog_module, "latest_runner_clock_gap", lambda *args, **kwargs: None)
    monkeypatch.setattr(watchdog_module, "latest_docker_lifecycle_event_for_bot", lambda *args, **kwargs: None)
    _patch_run_context(monkeypatch)
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None, run_id=None: marked.append((bot_id, reason, dict(diagnostics or {}))) or True,
    )

    crashed = watchdog.recover_local_orphans()

    assert crashed == ["bot-1"]
    assert marked[0][1] == "server_restart:current-runner"
    assert marked[0][2]["run_lease"]["runner_id"] == "current-runner"
