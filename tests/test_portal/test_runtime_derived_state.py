from __future__ import annotations

from datetime import datetime, timezone

import pytest

pd = pytest.importorskip("pandas")

from engines.bot_runtime.core.domain import Candle
from portal.backend.service.bots.runtime_derived_state import _candles_to_dataframe


def test_candles_to_dataframe_tolerates_candles_without_trade_count() -> None:
    frame = _candles_to_dataframe(
        [
            Candle(
                time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                volume=42.0,
            )
        ]
    )

    assert "trade_count" in frame.columns
    assert "volume" in frame.columns
    assert frame.iloc[0]["volume"] == 42.0
    assert pd.isna(frame.iloc[0]["trade_count"])
