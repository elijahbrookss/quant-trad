from __future__ import annotations

from portal.backend.service.bots import botlens_bootstrap_service


def test_botlens_run_meta_exposes_execution_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        botlens_bootstrap_service,
        "get_bot_run",
        lambda _run_id: {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "status": "running",
            "config_snapshot": {"execution_mode": "full"},
        },
    )

    meta = botlens_bootstrap_service._run_meta(
        run_id="run-1",
        projected_bot={"id": "bot-1"},
        health_state={"status": "running"},
    )

    assert meta["execution_mode"] == "full"
    assert meta["intrabar_execution"] is True
