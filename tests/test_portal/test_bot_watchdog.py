from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.bots import bot_watchdog as watchdog_module


def test_verify_container_ownership_does_not_fail_starting_bot_without_confirmed_container_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    callbacks: list[tuple[str, dict]] = []

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "starting",
                "heartbeat_at": None,
                "last_run_at": "2026-04-09T04:21:37Z",
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "exited",
            "running": False,
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )
    watchdog.set_orphan_callback(lambda bot_id, bot: callbacks.append((bot_id, dict(bot))))

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []
    assert callbacks == []


def test_verify_container_ownership_respects_startup_grace_for_missing_container_without_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    recent_start = (datetime.utcnow() - timedelta(seconds=5)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "starting",
                "heartbeat_at": None,
                "last_run_at": recent_start,
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "missing",
            "running": False,
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_uses_startup_artifact_time_for_launch_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    recent_start = (datetime.utcnow() - timedelta(seconds=5)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "starting",
                "heartbeat_at": None,
                "last_run_at": "2026-04-09T04:21:37Z",
                "last_run_artifact": {"startup": {"run_id": "run-1", "at": recent_start}},
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "missing",
            "running": False,
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_respects_startup_grace_with_stale_prior_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    recent_start = (datetime.utcnow() - timedelta(seconds=5)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "starting",
                "heartbeat_at": "2026-04-09T04:21:37Z",
                "last_run_at": "2026-04-09T04:21:37Z",
                "last_run_artifact": {"startup": {"run_id": "new-run", "at": recent_start}},
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "exited",
            "running": False,
            "runtime_run_id": "old-run",
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_does_not_fail_new_run_for_old_exited_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    recent_start = (datetime.utcnow() - timedelta(seconds=5)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "starting",
                "heartbeat_at": None,
                "last_run_at": recent_start,
                "last_run_artifact": {"startup": {"run_id": "new-run"}},
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "exited",
            "running": False,
            "runtime_run_id": "old-run",
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_does_not_fail_degraded_startup_without_confirmed_container_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    old_start = (datetime.utcnow() - timedelta(seconds=120)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "degraded",
                "heartbeat_at": "2026-04-09T04:21:37Z",
                "last_run_at": "2026-04-09T04:21:37Z",
                "last_run_artifact": {"startup": {"run_id": "new-run", "at": old_start}},
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "missing",
            "running": False,
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == []
    assert marked == []


def test_verify_container_ownership_marks_confirmed_owned_container_after_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watchdog = watchdog_module.BotWatchdog()
    marked: list[tuple[str, str]] = []
    old_start = (datetime.utcnow() - timedelta(seconds=120)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "load_bots",
        lambda: [
            {
                "id": "bot-1",
                "status": "starting",
                "heartbeat_at": None,
                "last_run_at": old_start,
                "last_run_artifact": {"startup": {"run_id": "run-1"}},
            }
        ],
    )
    monkeypatch.setattr(
        watchdog_module.DockerBotRunner,
        "inspect_bot_container",
        lambda _bot_id: {
            "name": "quant-trad-bots-bot-1",
            "status": "exited",
            "running": False,
            "runtime_run_id": "run-1",
            "error": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == ["bot-1"]
    assert marked == [("bot-1", "container_not_running:quant-trad-bots-bot-1")]


def test_scan_stale_heartbeats_persists_runner_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    watchdog = watchdog_module.BotWatchdog()
    watchdog._runner_id = "current-runner"
    marked: list[tuple[str, str, dict]] = []
    stale_heartbeat = (datetime.utcnow() - timedelta(seconds=125)).isoformat() + "Z"

    monkeypatch.setattr(
        watchdog_module,
        "find_orphaned_bots",
        lambda stale_threshold_seconds, runner_id=None: [
            {
                "id": "bot-1",
                "runner_id": "backend.quanttrad",
                "heartbeat_at": stale_heartbeat,
                "last_run_artifact": {"startup": {"run_id": "run-1"}},
            }
        ],
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
    monkeypatch.setattr(watchdog_module, "get_bot_run_lease", lambda _run_id: None)
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason, dict(diagnostics or {}))) or True,
    )

    crashed = watchdog.scan_stale_heartbeats()

    assert crashed == ["bot-1"]
    assert marked[0][0] == "bot-1"
    assert marked[0][1] == "stale_heartbeat:prev=backend.quanttrad"
    assert marked[0][2]["detected_runner_id"] == "current-runner"
    assert marked[0][2]["previous_runner"] == "backend.quanttrad"
    assert marked[0][2]["run_id"] == "run-1"
    assert marked[0][2]["stale_age_seconds"] >= 120.0
    assert marked[0][2]["runner_clock_gap"]["gap_seconds"] == 3672.0
    assert marked[0][2]["docker_lifecycle"]["action"] == "die"


def test_scan_stale_heartbeats_skips_when_run_lease_is_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    watchdog = watchdog_module.BotWatchdog()
    watchdog._runner_id = "current-runner"
    marked: list[tuple[str, str]] = []

    monkeypatch.setattr(
        watchdog_module,
        "find_orphaned_bots",
        lambda stale_threshold_seconds, runner_id=None: [
            {
                "id": "bot-1",
                "runner_id": "backend.quanttrad",
                "heartbeat_at": "2026-05-19T00:00:00Z",
                "last_run_artifact": {"startup": {"run_id": "run-1"}},
            }
        ],
    )
    monkeypatch.setattr(watchdog_module, "latest_runner_clock_gap", lambda *args, **kwargs: None)
    monkeypatch.setattr(watchdog_module, "latest_docker_lifecycle_event_for_bot", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        watchdog_module,
        "get_bot_run_lease",
        lambda _run_id: {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "runner_id": "backend.quanttrad",
            "status": "active",
            "expires_at": (datetime.utcnow() + timedelta(seconds=60)).isoformat() + "Z",
            "released_at": None,
        },
    )
    monkeypatch.setattr(
        watchdog_module,
        "mark_bot_crashed",
        lambda bot_id, reason="", diagnostics=None: marked.append((bot_id, reason)) or True,
    )

    crashed = watchdog.scan_stale_heartbeats()

    assert crashed == []
    assert marked == []
