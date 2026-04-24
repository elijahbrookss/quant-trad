from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from portal.backend.service.reports import artifacts


class _FakeStorage:
    def __init__(self) -> None:
        self.upserts: list[dict] = []

    def get_indicator(self, indicator_id: str) -> dict:
        return {
            "id": indicator_id,
            "type": "test_indicator",
            "version": "v1",
            "name": indicator_id,
            "params": {},
            "enabled": True,
        }

    def list_bot_trades_for_run(self, run_id: str) -> list[dict]:
        return []

    def list_bot_trade_events_for_trades(self, trade_ids: list[str]) -> list[dict]:
        return []

    def upsert_bot_run(self, payload: dict) -> None:
        self.upserts.append(dict(payload))


def _configure_artifact_settings(monkeypatch, tmp_path: Path, storage: _FakeStorage) -> None:
    settings = SimpleNamespace(
        enabled=True,
        capture_backtest=True,
        capture_live=True,
        root_dir=str(tmp_path),
        output_format="csv",
        include_candles=True,
        include_runtime_events=True,
        include_indicator_outputs=False,
        include_overlays=False,
        include_decision_trace=True,
        include_trades=False,
        include_trade_events=False,
        compress_zip_on_finalize=False,
    )
    monkeypatch.setattr(artifacts, "_ARTIFACT_SETTINGS", settings)
    monkeypatch.setattr(
        artifacts,
        "get_settings",
        lambda: SimpleNamespace(reports=SimpleNamespace(artifacts=settings)),
    )
    monkeypatch.setattr(artifacts, "_storage", lambda: storage)
    monkeypatch.setattr(
        artifacts,
        "_report_helpers",
        lambda: (
            lambda trades: list(trades),
            lambda closed_trades, config_snapshot, start_time=None, end_time=None: {
                "net_pnl": 0,
                "total_return": 0,
                "sharpe": 0,
                "max_drawdown_pct": 0,
                "total_trades": len(closed_trades),
            },
            lambda value: value,
        ),
    )


def _series(symbol: str, strategy_id: str, indicator_id: str):
    candle = SimpleNamespace(time="2026-01-01T00:00:00Z", open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
    return SimpleNamespace(
        strategy_id=strategy_id,
        name=f"{symbol}-strategy",
        symbol=symbol,
        timeframe="1h",
        datasource="COINBASE",
        exchange="coinbase_direct",
        window_start="2026-01-01T00:00:00Z",
        window_end="2026-01-02T00:00:00Z",
        candles=[candle],
        meta={"indicator_links": [{"indicator_id": indicator_id}]},
    )


def _worker_config(worker_id: str) -> dict:
    return {
        "name": "bot-1",
        "run_type": "backtest",
        "worker_id": worker_id,
        "report_artifact_role": "worker",
        "wallet_config": {"balances": {"USD": 1000}},
        "risk": {},
        "backtest_start": "2026-01-01T00:00:00Z",
        "backtest_end": "2026-01-02T00:00:00Z",
    }


def _worker_artifact(run_id: str, bot_id: str, worker_id: str) -> dict:
    return {
        "run_id": run_id,
        "bot_id": bot_id,
        "started_at": "2026-01-01T00:00:00Z",
        "ended_at": "2026-01-01T00:05:00Z",
        "status": "completed",
        "wallet_start": {"balances": {"USD": 1000}},
        "runtime_event_stream": [],
        "wallet_end": {"balances": {"USD": 1000}},
        "wallet_state": {"balances": {"USD": 1000}},
        "wallet_ledger": [],
        "decision_trace": [{"worker_id": worker_id, "event_subtype": "signal"}],
        "decision_artifacts": [],
        "rejection_artifacts": [],
    }


def test_finalize_run_artifact_bundle_from_workers_aggregates_worker_outputs(monkeypatch, tmp_path: Path) -> None:
    storage = _FakeStorage()
    _configure_artifact_settings(monkeypatch, tmp_path, storage)
    bot_id = "bot-1"
    run_id = "run-1"

    worker_1 = artifacts.RunArtifactBundle(
        bot_id=bot_id,
        run_id=run_id,
        config=_worker_config("worker-1"),
        series=[_series("BTC", "strategy-1", "indicator-1")],
    )
    worker_1.start(started_at="2026-01-01T00:00:00Z")
    worker_1.record_runtime_event(serialized={"event_id": "evt-1", "event_name": "signal_emitted"})
    worker_1.finalize(runtime_status="completed", artifact=_worker_artifact(run_id, bot_id, "worker-1"))

    worker_2 = artifacts.RunArtifactBundle(
        bot_id=bot_id,
        run_id=run_id,
        config=_worker_config("worker-2"),
        series=[_series("ETH", "strategy-1", "indicator-2")],
    )
    worker_2.start(started_at="2026-01-01T00:00:00Z")
    worker_2.record_runtime_event(serialized={"event_id": "evt-2", "event_name": "signal_emitted"})
    worker_2.finalize(runtime_status="completed", artifact=_worker_artifact(run_id, bot_id, "worker-2"))

    run_dir = tmp_path / "bot_id=bot-1" / "run_id=run-1"
    assert (run_dir / ".spool").exists()

    artifacts.finalize_run_artifact_bundle_from_workers(
        bot_id=bot_id,
        run_id=run_id,
        config=_worker_config("worker-1"),
        runtime_status="completed",
    )

    assert not (run_dir / ".spool").exists()
    assert (run_dir / "execution" / "runtime_events.jsonl").exists()
    assert (run_dir / "execution" / "decision_trace.csv").exists()
    assert (run_dir / "run" / "runtime_artifact.json").exists()
    assert (run_dir / "summary" / "summary.json").exists()
    assert (run_dir / "series" / "symbol=BTC" / "timeframe=1h" / "candles.csv").exists()
    assert (run_dir / "series" / "symbol=ETH" / "timeframe=1h" / "candles.csv").exists()

    manifest = artifacts._read_json(run_dir / "manifest.json")
    assert manifest["status"] == "completed"
    assert len(artifacts._read_jsonl(run_dir / "execution" / "runtime_events.jsonl")) == 2
    series_snapshot = artifacts._read_json(run_dir / "run" / "series.json")
    assert len(series_snapshot["series"]) == 2
    assert len(storage.upserts) == 1
