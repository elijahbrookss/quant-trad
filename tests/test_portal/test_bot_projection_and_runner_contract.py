from __future__ import annotations

from types import SimpleNamespace

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


def test_project_bot_state_prefers_active_runtime_over_stale_startup_failed_lifecycle():
    run_snapshot = SimpleNamespace(
        seq=7,
        health=SimpleNamespace(
            to_dict=lambda: {
                "status": "running",
                "warning_count": 0,
                "warnings": [],
                "worker_count": 2,
                "active_workers": 1,
                "last_event_at": "2026-01-01T00:00:10Z",
            }
        ),
        symbol_catalog=SimpleNamespace(entries={"instrument-btc|1m": {"symbol_key": "instrument-btc|1m"}}),
        open_trades=SimpleNamespace(entries={}),
    )
    projected = project_bot_state(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "status": "telemetry_degraded",
            "runner_id": "runner-test",
            "heartbeat_at": "2026-01-01T00:00:02Z",
        },
        run={"run_id": "run-1", "status": "running", "started_at": "2026-01-01T00:00:00Z"},
        lifecycle={
            "run_id": "run-1",
            "phase": "startup_failed",
            "status": "startup_failed",
            "owner": "runtime",
            "message": "Worker failed before initial lifecycle reconciliation.",
        },
        run_snapshot=run_snapshot,
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

    assert projected["status"] == "running"
    assert projected["runtime"]["status"] == "running"
    assert projected["runtime"]["phase"] == "live"
    assert projected["lifecycle"]["status"] == "running"
    assert projected["lifecycle"]["phase"] == "live"


def test_project_bot_state_keeps_terminal_run_available_without_active_run_id():
    projected = project_bot_state(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "status": "canceled",
            "runner_id": None,
            "heartbeat_at": None,
        },
        run={"run_id": "run-canceled-1", "status": "canceled", "started_at": "2026-01-01T00:00:00Z"},
        lifecycle={
            "run_id": "run-canceled-1",
            "phase": "canceled",
            "status": "canceled",
            "owner": "backend",
            "message": "Bot cancel completed; runtime container stopped.",
        },
        container_state={
            "name": "quant-trad-bots-bot-1",
            "status": "missing",
            "running": False,
            "id": None,
            "started_at": None,
            "finished_at": None,
            "exit_code": None,
            "error": None,
        },
    )

    assert projected["status"] == "canceled"
    assert projected["active_run_id"] is None
    assert projected["latest_run_id"] == "run-canceled-1"
    assert projected["runtime"]["run_id"] == "run-canceled-1"
    assert projected["lifecycle"]["telemetry"]["run_id"] == "run-canceled-1"
    assert projected["lifecycle"]["reason"] == "run_canceled"
    assert projected["controls"]["can_start"] is True
    assert projected["controls"]["can_stop"] is False


def test_project_bot_state_marks_runtime_telemetry_unavailable_without_snapshot():
    projected = project_bot_state(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "status": "running",
            "runner_id": "runner-test",
            "heartbeat_at": "2026-01-01T00:00:02Z",
        },
        run={"run_id": "run-1", "status": "running", "started_at": "2026-01-01T00:00:00Z"},
        lifecycle={
            "run_id": "run-1",
            "phase": "live",
            "status": "running",
            "owner": "runtime",
            "message": "Runtime is live.",
        },
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

    assert projected["status"] == "running"
    assert projected["runtime"]["phase"] == "live"
    assert projected["runtime"]["engine_status"] is None
    assert projected["runtime"]["seq"] is None
    assert projected["lifecycle"]["telemetry"]["available"] is False
    assert projected["lifecycle"]["telemetry"]["reason"] == "snapshot_unavailable"
    assert projected["lifecycle"]["telemetry"]["worker_count"] is None
    assert projected["lifecycle"]["telemetry"]["series_count"] is None


def test_project_bot_state_aggregates_runtime_symbol_stats_for_fleet_payload():
    run_snapshot = SimpleNamespace(
        seq=42,
        health=SimpleNamespace(
            to_dict=lambda: {
                "status": "running",
                "warning_count": 0,
                "warnings": [],
                "worker_count": 3,
                "active_workers": 3,
                "last_event_at": "2026-04-01T00:00:00Z",
            }
        ),
        symbol_catalog=SimpleNamespace(
            entries={
                "instrument-bip|1h": {
                    "symbol_key": "instrument-bip|1h",
                    "symbol": "BIP",
                    "last_activity_at": "2026-03-31T00:00:00Z",
                    "stats": {
                        "wins": 2,
                        "losses": 1,
                        "net_pnl": 25.0,
                        "gross_pnl": 30.0,
                        "fees_paid": 5.0,
                        "total_fees": 5.0,
                        "total_trades": 3,
                        "completed_trades": 3,
                        "quote_currency": "USD",
                    },
                },
                "instrument-etp|1h": {
                    "symbol_key": "instrument-etp|1h",
                    "symbol": "ETP",
                    "last_activity_at": "2026-04-01T00:00:00Z",
                    "stats": {
                        "wins": 1,
                        "losses": 1,
                        "net_pnl": -10.0,
                        "gross_pnl": -6.0,
                        "fees_paid": 4.0,
                        "total_fees": 4.0,
                        "total_trades": 2,
                        "completed_trades": 2,
                        "quote_currency": "USD",
                    },
                },
            }
        ),
        open_trades=SimpleNamespace(entries={}),
    )

    projected = project_bot_state(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "status": "running",
            "wallet_config": {"balances": {"USD": 1000.0}},
            "backtest_start": "2026-01-01T00:00:00Z",
        },
        run={"run_id": "run-1", "status": "running", "started_at": "2026-01-01T00:00:00Z"},
        lifecycle={"run_id": "run-1", "phase": "live", "status": "running"},
        run_snapshot=run_snapshot,
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

    stats = projected["runtime"]["stats"]
    assert stats["stats_source"] == "botlens_symbol_catalog"
    assert stats["total_trades"] == 5
    assert stats["completed_trades"] == 5
    assert stats["wins"] == 3
    assert stats["losses"] == 2
    assert stats["net_pnl"] == 15.0
    assert stats["fees_paid"] == 9.0
    assert stats["win_rate"] == 0.6
    assert stats["equity_start"] == 1000.0
    assert stats["equity_end"] == 1015.0
    assert projected["runtime"]["equity_curve"] == [
        {"time": "2026-01-01T00:00:00Z", "value": 1000.0},
        {"time": "2026-04-01T00:00:00Z", "value": 1015.0},
    ]
