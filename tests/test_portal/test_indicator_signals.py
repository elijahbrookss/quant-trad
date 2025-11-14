from datetime import datetime, timedelta, timezone
import importlib

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from portal.backend.main import app
from signals.engine import signal_generator as engine
from signals.rules.pivot import PivotLevelIndicator


def test_indicator_service_registers_pivot_rules():
    module = importlib.import_module("portal.backend.service.indicator_service")
    original_registry = dict(engine._REGISTRY)

    try:
        engine._REGISTRY.clear()
        importlib.reload(module)

        assert PivotLevelIndicator.NAME in engine._REGISTRY
    finally:
        engine._REGISTRY.clear()
        engine._REGISTRY.update(original_registry)


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

    @classmethod
    def from_context(cls, provider, ctx, **kwargs):  # noqa: D401 - test helper
        return cls(symbol=ctx.symbol)


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

        record = {
            "id": inst_id,
            "name": "Test indicator",
            "type": _DummyIndicator.NAME,
            "params": {
                "symbol": "ES",
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-01T02:00:00Z",
                "interval": "1h",
            },
            "color": "#60a5fa",
            "datasource": "ALPACA",
            "exchange": None,
            "enabled": True,
            "updated_at": "2024-01-01T00:00:00Z",
        }
        cache_entry = svc.IndicatorCacheEntry(
            meta=svc._build_meta_from_record(record),
            instance=indicator,
            updated_at=record["updated_at"],
        )
        def _raise_missing():  # noqa: D401 - closure used above
            raise KeyError("Indicator not found")

        monkeypatch.setattr(svc, "_INSTANCE_CACHE", {inst_id: cache_entry})
        monkeypatch.setattr(svc, "_load_indicator_record", lambda req_id: record if req_id == inst_id else (_raise_missing()))
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
