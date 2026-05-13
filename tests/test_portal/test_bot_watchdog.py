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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
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
        lambda bot_id, reason="": marked.append((bot_id, reason)) or True,
    )

    failed = watchdog.verify_container_ownership()

    assert failed == ["bot-1"]
    assert marked == [("bot-1", "container_not_running:quant-trad-bots-bot-1")]
