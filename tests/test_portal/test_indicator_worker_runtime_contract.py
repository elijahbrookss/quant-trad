from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.indicators.indicator_service.runtime_contract import (
    SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
)
from portal.backend.workers import indicator_worker


def test_worker_process_signals_rejects_non_engine_runtime_path(monkeypatch) -> None:
    monkeypatch.setattr(
        indicator_worker,
        "generate_signals_for_instance",
        lambda **kwargs: {"machine": {"signals": []}, "ui": {"overlays": []}, "runtime_path": "legacy"},
    )

    with pytest.raises(RuntimeError, match="runtime_path_mismatch"):
        indicator_worker._process_signals(
            {
                "inst_id": "ind-1",
                "start": "2026-02-01T00:00:00Z",
                "end": "2026-02-01T01:00:00Z",
                "interval": "1h",
                "symbol": "ES",
                "datasource": "ALPACA",
                "exchange": None,
                "instrument_id": "instrument-1",
                "config": {},
            },
            ctx=None,  # unused by monkeypatched generator
        )


def test_worker_process_signals_accepts_engine_runtime_path(monkeypatch) -> None:
    captured = {}

    def _fake_generate_signals_for_instance(**kwargs):
        captured.update(kwargs)
        return {
            "machine": {"signals": []},
            "ui": {"overlays": [{"type": "indicator_signal", "source": "signal", "payload": {"bubbles": []}}]},
            "runtime_path": SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
        }

    monkeypatch.setattr(
        indicator_worker,
        "generate_signals_for_instance",
        _fake_generate_signals_for_instance,
    )

    payload = indicator_worker._process_signals(
        {
            "inst_id": "ind-1",
            "start": "2026-02-01T00:00:00Z",
            "end": "2026-02-01T01:00:00Z",
            "interval": "1h",
            "symbol": "ES",
            "datasource": "ALPACA",
            "exchange": None,
            "instrument_id": "instrument-1",
            "config": {},
        },
        ctx=None,  # unused by monkeypatched generator
    )
    assert payload.get("runtime_path") == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
    assert payload.get("machine") == {"signals": []}
    assert payload.get("ui") == {"overlays": [{"type": "indicator_signal", "source": "signal", "payload": {"bubbles": []}}]}
    assert captured["instrument_id"] == "instrument-1"
