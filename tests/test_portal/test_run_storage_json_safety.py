from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

import pytest
import uuid

from portal.backend.service.storage.repos import runs
from portal.backend.service.reports import artifacts, run_research_dataset


class _FakeSession:
    def __init__(self) -> None:
        self.record = None

    def get(self, _model, _run_id):
        return self.record

    def add(self, record) -> None:
        self.record = record


class _FakeDb:
    available = True

    def __init__(self) -> None:
        self.session_instance = _FakeSession()

    @contextmanager
    def session(self) -> Iterator[_FakeSession]:
        yield self.session_instance


def test_upsert_bot_run_json_sanitizes_nested_runtime_snapshot(monkeypatch) -> None:
    fake_db = _FakeDb()
    monkeypatch.setattr(runs, "db", fake_db)

    result = runs.upsert_bot_run(
        {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "bot_name": "Bot 1",
            "strategy_id": "strategy-1",
            "run_type": "backtest",
            "symbols": ["BTCUSD"],
            "config_snapshot": {
                "started_at": datetime(2026, 5, 17, 7, 45, tzinfo=timezone.utc),
                "bot": {"updated_at": datetime(2026, 5, 17, 7, 46)},
            },
        }
    )

    assert result["config_snapshot"]["started_at"] == "2026-05-17T07:45:00Z"
    assert result["config_snapshot"]["bot"]["updated_at"] == "2026-05-17T07:46:00Z"
    assert result["config_hash"]
    assert result["material_config_hash"]
    assert "decision_ledger" not in result


def test_upsert_bot_run_rejects_lifecycle_owned_fields(monkeypatch) -> None:
    monkeypatch.setattr(runs, "db", _FakeDb())

    with pytest.raises(ValueError, match="lifecycle fields are ledger-owned"):
        runs.upsert_bot_run(
            {
                "run_id": "run-1",
                "status": "completed",
                "started_at": "2026-05-17T07:45:00Z",
                "ended_at": "2026-05-17T07:46:00Z",
            }
        )


def test_report_gateway_preserves_admitted_execution_identity(monkeypatch) -> None:
    """Finalization must not replace admission with its reduced report projection."""
    fake_db = _FakeDb()
    monkeypatch.setattr(runs, "db", fake_db)
    admitted = {
        "bot": {"id": "bot-1", "execution_mode": "backtest"},
        "start_request": {"dataset_id": "mds_frozen"},
        "execution_assumptions": {"execution_model": "causal_spot_resting_bar.v1"},
        "simulation_fee_schedules": {"ETH-USD": {"maker": 0.005}},
        "resolved_execution_context_bundle": {"bundle_hash": "original-context"},
    }
    before = runs.upsert_bot_run({"run_id": "run-1", "config_snapshot": admitted})
    projection = {
        "strategies": [{"id": "strategy-1", "atm_template": {"execution_mode": "market"}}],
        "wallet_start": {"balances": {"USD": 10000}},
        "backtest_warmup_evidence": [{"loaded_bars": 100}],
        "indicator_source_diagnostics": [],
        "runtime_warnings": [{"code": "fixture_warning"}],
    }
    result = artifacts.RepositoryArtifactStorage().upsert_bot_run({
        "run_id": "run-1",
        "summary": {"net_pnl": -293.3532},
        "config_snapshot": {
            **projection,
            "execution_assumptions": {"execution_model": "must-not-replace-admission"},
            "dataset_binding": {"dataset_id": "must-not-add-execution-authority"},
        },
        "config_hash": "projection-hash-must-not-replace-admission",
        "material_config_hash": "projection-material-hash",
    })
    assert result["config_snapshot"] == {**admitted, **projection}
    assert result["config_hash"] == before["config_hash"]
    assert result["material_config_hash"] == before["material_config_hash"]
    assert result["summary"] == {"net_pnl": -293.3532}
    metadata = run_research_dataset._metadata(result)
    assert metadata.starting_capital == 10000
    assert metadata.configuration["atm"]["template"]["execution_mode"] == "market"
    assert metadata.configuration["data"]["backtest_warmup_evidence"] == projection["backtest_warmup_evidence"]
    assert metadata.configuration["config_hash"] == metadata.config_hash == before["config_hash"]
    assert metadata.configuration["material_config_hash"] == before["material_config_hash"]
    assert run_research_dataset._indicator_source_diagnostics_dataset(result)["available"] is True
    assert run_research_dataset._execution_assumption_manifest(result) == admitted["execution_assumptions"]


def test_report_gateway_retains_legacy_projection_behavior(monkeypatch) -> None:
    monkeypatch.setattr(runs, "db", _FakeDb())
    before = runs.upsert_bot_run({"run_id": "legacy", "config_snapshot": {"strategies": []}})
    projection = {"strategies": [{"id": "strategy-1"}], "wallet_start": {"USD": 10000}}
    result = artifacts.RepositoryArtifactStorage().upsert_bot_run({
        "run_id": "legacy", "config_snapshot": projection,
    })
    assert result["config_snapshot"] == projection
    assert result["config_hash"] != before["config_hash"]


@pytest.mark.db
def test_report_finalization_preserves_admission_in_disposable_database():
    from sqlalchemy import delete
    from portal.backend.db.models import BotRunRecord
    run_id = str(uuid.uuid4())
    admitted = {
        'bot': {'id': str(uuid.uuid4()), 'execution_mode': 'backtest'},
        'start_request': {'dataset_id': 'disposable-frozen-fixture'},
        'execution_assumptions': {'model_version': 'causal_spot_resting_bar.v2'},
        'simulation_fee_schedules': {'ETH-USD': {'maker_rate': .005, 'taker_rate': .009}},
        'resolved_execution_context_bundle': {'bundle_hash': 'fixture-context-identity'},
    }
    try:
        before = runs.upsert_bot_run({'run_id': run_id, 'config_snapshot': admitted})
        projection = {'strategies': [], 'wallet_start': {'balances': {'USD': 1000}},
                      'indicator_source_diagnostics': [], 'backtest_warmup_evidence': []}
        artifacts.RepositoryArtifactStorage().upsert_bot_run({
            'run_id': run_id, 'summary': {'net_pnl': -1.25},
            'config_snapshot': projection,
        })
        actual = runs.get_bot_run(run_id)
        assert actual['config_snapshot'] == {**admitted, **projection}
        assert actual['config_hash'] == before['config_hash']
        assert actual['material_config_hash'] == before['material_config_hash']
        assert actual['summary'] == {'net_pnl': -1.25}
    finally:
        with runs.db.session() as session:
            session.execute(delete(BotRunRecord).where(BotRunRecord.run_id == run_id))
