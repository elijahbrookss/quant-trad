from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.bots import bot_service


def test_exact_run_inspection_falls_back_to_persisted_identity_when_definition_missing(monkeypatch) -> None:
    run = {
        "run_id": "historical-run",
        "bot_id": "deleted-bot",
        "bot_name": "Persisted bot",
        "strategy_id": "strategy-1",
        "strategy_name": "Persisted strategy",
        "run_type": "backtest",
        "execution_mode": "full",
        "status": "completed",
        "updated_at": "2026-08-02T12:00:00Z",
    }
    storage = SimpleNamespace(
        get_bot_run=lambda _run_id: run,
        get_bot_run_lifecycle=lambda _run_id: {
            "run_id": "historical-run",
            "phase": "completed",
            "status": "completed",
        },
        get_bot_run_lease=lambda _run_id: {},
    )
    config_service = SimpleNamespace(
        get_bot=lambda _bot_id: (_ for _ in ()).throw(KeyError("definition missing")),
    )
    monkeypatch.setattr(
        bot_service,
        "_composition",
        lambda: SimpleNamespace(storage=storage, config_service=config_service),
    )
    monkeypatch.setattr(
        bot_service,
        "_telemetry_hub",
        lambda: SimpleNamespace(get_run_snapshot=lambda **_kwargs: None),
    )
    monkeypatch.setattr(bot_service, "_report_status", lambda _run_id: {})

    payload = bot_service.get_bot_run_inspection("historical-run")

    assert payload["run"]["run_id"] == "historical-run"
    assert payload["run"]["runtime_status"] == "completed"
    assert payload["definition"] == {
        "available": False,
        "id": "deleted-bot",
        "name": "Persisted bot",
        "strategy_id": "strategy-1",
        "strategy_variant_id": None,
        "strategy_variant_name": "Persisted strategy",
        "run_type": "backtest",
        "execution_mode": "full",
    }
