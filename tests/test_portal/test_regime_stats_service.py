from __future__ import annotations

from datetime import datetime, timezone

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("sqlalchemy")

from data_providers.config.runtime import runtime_config_from_env
from portal.backend.service.market import regime_stats_service as svc


class _DummyClassification:
    def __init__(self, payload):
        self._payload = dict(payload)

    def as_dict(self):
        return dict(self._payload)


def test_build_regimes_skips_block_persistence_when_engine_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = svc.RegimeStatsService(config=runtime_config_from_env().persistence, engine=None)
    candle_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(
        service._engine_impl,
        "classify",
        lambda candle_payload, stats: _DummyClassification(
            {
                "structure": {"state": "trend"},
                "volatility": {"state": "normal"},
                "liquidity": {"state": "normal"},
                "expansion": {"state": "stable"},
                "confidence": 0.75,
            }
        ),
    )
    monkeypatch.setattr(service._stabilizer, "stabilize", lambda raw_regime, **kwargs: dict(raw_regime))
    monkeypatch.setattr(
        svc,
        "build_regime_blocks",
        lambda points, **kwargs: (
            [
                {
                    "block_id": "block-1",
                    "start_ts": candle_time,
                    "end_ts": candle_time,
                }
            ],
            {0: "block-1"},
        ),
    )
    monkeypatch.setattr(
        service,
        "_upsert_blocks",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("runtime-local regime build must not persist blocks")),
    )

    candles_df = pd.DataFrame(
        [
            {
                "candle_time": pd.to_datetime(candle_time, utc=True),
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 42.0,
                "trade_count": None,
            }
        ]
    )
    stats_df = pd.DataFrame(
        [
            {
                "candle_time": pd.to_datetime(candle_time, utc=True),
                "stats": {"atr": 1.0},
            }
        ]
    )

    rows = service._build_regimes(
        candles_df,
        stats_df,
        instrument_id="inst-1",
        timeframe_seconds=3600,
        regime_version="regime-v1",
    )

    assert len(rows) == 1
    assert rows[0]["regime"]["regime_block_id"] == "block-1"
