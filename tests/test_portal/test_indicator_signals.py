from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.main import app


class _DummyFrame:
    def __init__(self, timestamps):
        self._index = tuple(timestamps)
        self.empty = len(self._index) == 0

    def copy(self):
        return _DummyFrame(self._index)

    @property
    def index(self):
        return self._index

    def __len__(self):
        return len(self._index)


class _DummyIndicator:
    NAME = "DummySignalIndicator"

    def __init__(self, symbol: str):
        self.symbol = symbol


def _build_dataframe() -> _DummyFrame:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    timestamps = [start + timedelta(hours=i) for i in range(3)]
    return _DummyFrame(timestamps)


@pytest.fixture
def signal_test_env(monkeypatch):
    from portal.backend.service import indicator_service as svc
    from signals.engine import signal_generator as engine

    def _setup(df: _DummyFrame):
        indicator = _DummyIndicator(symbol="ES")
        inst_id = "test-inst"

        class DummyProvider:
            def __init__(self, frame: _DummyFrame):
                self._frame = frame

            def get_ohlcv(self, ctx):
                return self._frame.copy()

        monkeypatch.setattr(svc, "_REGISTRY", {inst_id: {"meta": {"params": {"symbol": "ES"}}, "instance": indicator}})
        monkeypatch.setattr(svc, "AlpacaProvider", lambda: DummyProvider(df))

        engine_registry = dict(engine._REGISTRY)

        def dummy_rule(context, payload):
            return [
                {
                    "type": "breakout",
                    "symbol": context["symbol"],
                    "time": context["df"].index[-1],
                    "confirmation": context.get("pivot_breakout_confirmation_bars"),
                }
            ]

        def dummy_overlay(signals, plot_df, **kwargs):
            return [
                {
                    "kind": "dummy",
                    "signals": len(signals),
                    "bars": len(plot_df),
                    "confirmation": kwargs.get("pivot_breakout_confirmation_bars"),
                }
            ]

        monkeypatch.setattr(engine, "_REGISTRY", engine_registry)
        engine.register_indicator_rules(_DummyIndicator.NAME, [dummy_rule], overlay_adapter=dummy_overlay)

        client = TestClient(app)
        return client, inst_id

    return _setup


def test_generate_signals_success(signal_test_env):
    client, inst_id = signal_test_env(_build_dataframe())

    payload = {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-01T02:00:00Z",
        "interval": "1h",
    }

    response = client.post(f"/api/indicators/{inst_id}/signals", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["signals"], "Expected at least one signal"
    assert body["signals"][0]["metadata"]["confirmation"] == 1
    assert body["overlays"][0]["confirmation"] == 1


def test_generate_signals_indicator_missing(signal_test_env):
    client, _ = signal_test_env(_build_dataframe())

    payload = {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-01T02:00:00Z",
        "interval": "1h",
    }

    response = client.post("/api/indicators/missing-id/signals", json=payload)
    assert response.status_code == 404


def test_generate_signals_no_candles(signal_test_env):
    empty_df = _DummyFrame(())
    client, inst_id = signal_test_env(empty_df)

    payload = {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-01T02:00:00Z",
        "interval": "1h",
    }

    response = client.post(f"/api/indicators/{inst_id}/signals", json=payload)
    assert response.status_code == 404
    assert response.json()["detail"] == "No candles available for given window"
