from __future__ import annotations

from portal.backend.service.bots.bot_state_projection import project_bot_state
from portal.backend.service.bots.runner import DockerBotRunner


def test_project_bot_state_is_pure_and_preserves_lifecycle_metadata(monkeypatch):
    monkeypatch.setattr(
        "portal.backend.service.bots.runner.DockerBotRunner.inspect_bot_container",
        lambda _bot_id: (_ for _ in ()).throw(AssertionError("inspect should not be called by pure projection")),
    )

    projected = project_bot_state(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "status": "starting",
            "runner_id": "runner-test",
        },
        run={"run_id": "run-1", "status": "starting", "started_at": "2026-01-01T00:00:00Z"},
        lifecycle={
            "run_id": "run-1",
            "phase": "awaiting_container_boot",
            "status": "starting",
            "owner": "backend",
            "message": "Awaiting container bootstrap checkpoints.",
            "metadata": {
                "series_progress": {
                    "total_series": 2,
                    "workers_planned": 2,
                    "workers_spawned": 1,
                    "series": {
                        "BTCUSDT": {"status": "spawned"},
                        "ETHUSDT": {"status": "planned"},
                    },
                }
            },
        },
        view_row=None,
        container_state={
            "name": "quant-trad-bots-bot-1",
            "status": "running",
            "running": True,
            "id": "container-1",
            "started_at": "2026-01-01T00:00:01Z",
            "finished_at": None,
            "exit_code": None,
            "error": None,
        },
    )

    assert projected["active_run_id"] == "run-1"
    assert projected["lifecycle"]["phase"] == "awaiting_container_boot"
    assert projected["lifecycle"]["message"] == "Awaiting container bootstrap checkpoints."
    assert projected["lifecycle"]["metadata"]["series_progress"]["workers_spawned"] == 1
    assert projected["runtime"]["phase"] == "awaiting_container_boot"


def test_runner_runtime_env_includes_backend_owned_run_id(monkeypatch):
    monkeypatch.setattr(
        "portal.backend.service.bots.runner._DATABASE_SETTINGS",
        type("DB", (), {"dsn": "postgresql://example"})(),
    )
    monkeypatch.setattr(
        "portal.backend.service.bots.runner._SECURITY_SETTINGS",
        type("Sec", (), {"provider_credential_key": "secret-key"})(),
    )

    env = DockerBotRunner._runtime_process_env("bot-1", "run-1")

    assert env["QT_BOT_RUNTIME_BOT_ID"] == "bot-1"
    assert env["QT_BOT_RUNTIME_RUN_ID"] == "run-1"
    assert env["PG_DSN"] == "postgresql://example"
    assert env["QT_SECURITY_PROVIDER_CREDENTIAL_KEY"] == "secret-key"
