from __future__ import annotations

import pytest

from portal.backend.service.indicators.indicator_service.runtime_contract import (
    SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
)
from portal.backend.workers import quantlab_worker


def test_worker_process_signals_rejects_non_engine_runtime_path(monkeypatch) -> None:
    monkeypatch.setattr(
        quantlab_worker,
        "generate_signals_for_instance",
        lambda **kwargs: {"signals": [], "runtime_path": "legacy"},
    )

    with pytest.raises(RuntimeError, match="runtime_path_mismatch"):
        quantlab_worker._process_signals(
            {
                "inst_id": "ind-1",
                "start": "2026-02-01T00:00:00Z",
                "end": "2026-02-01T01:00:00Z",
                "interval": "1h",
                "symbol": "ES",
                "datasource": "ALPACA",
                "exchange": None,
                "config": {},
            },
            ctx=None,  # unused by monkeypatched generator
        )


def test_worker_process_signals_accepts_engine_runtime_path(monkeypatch) -> None:
    monkeypatch.setattr(
        quantlab_worker,
        "generate_signals_for_instance",
        lambda **kwargs: {"signals": [], "runtime_path": SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT},
    )

    payload = quantlab_worker._process_signals(
        {
            "inst_id": "ind-1",
            "start": "2026-02-01T00:00:00Z",
            "end": "2026-02-01T01:00:00Z",
            "interval": "1h",
            "symbol": "ES",
            "datasource": "ALPACA",
            "exchange": None,
            "config": {},
        },
        ctx=None,  # unused by monkeypatched generator
    )
    assert payload.get("runtime_path") == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT
