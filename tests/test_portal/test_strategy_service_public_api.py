from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")

from portal.backend.service.strategies import strategy_service
from portal.backend.service.strategies.strategy_service import facade


class _StubRegistry:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def evaluate(self, strategy_id: str, **kwargs: object) -> dict[str, object]:
        self.calls.append((strategy_id, kwargs))
        return {"strategy_id": strategy_id, "kwargs": kwargs}


def test_facade_evaluate_wrapper_preserves_legacy_module_api(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _StubRegistry()
    monkeypatch.setattr(facade, "_REGISTRY", registry)

    result = facade.evaluate(
        "strategy-1",
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T01:00:00Z",
        interval="1m",
        instrument_ids=["instrument-1"],
        variant_id="variant-1",
        config={"mode": "backtest"},
    )

    assert result["strategy_id"] == "strategy-1"
    assert registry.calls == [
        (
            "strategy-1",
            {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T01:00:00Z",
                "interval": "1m",
                "instrument_ids": ["instrument-1"],
                "variant_id": "variant-1",
                "config": {"mode": "backtest"},
            },
        )
    ]


def test_strategy_service_package_exports_evaluate(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _StubRegistry()
    monkeypatch.setattr(facade, "_REGISTRY", registry)

    strategy_service.evaluate(
        "strategy-2",
        start="2026-01-01T00:00:00Z",
        end="2026-01-01T01:00:00Z",
        interval="5m",
        instrument_ids=["instrument-2"],
        config={"mode": "paper"},
    )

    assert registry.calls == [
        (
            "strategy-2",
            {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T01:00:00Z",
                "interval": "5m",
                "instrument_ids": ["instrument-2"],
                "variant_id": None,
                "config": {"mode": "paper"},
            },
        )
    ]
