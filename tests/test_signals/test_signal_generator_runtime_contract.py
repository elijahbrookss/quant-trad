from __future__ import annotations

from datetime import datetime, timezone

import pytest

from signals.base import BaseSignal
from signals.engine.signal_generator import (
    build_signal_overlays,
    describe_indicator_rules,
    run_indicator_rules,
)


def test_run_indicator_rules_legacy_path_is_disabled() -> None:
    with pytest.raises(RuntimeError, match="legacy_signal_batch_path_disabled"):
        run_indicator_rules("market_profile", market_df=[])


def test_describe_indicator_rules_legacy_catalog_is_disabled() -> None:
    with pytest.raises(RuntimeError, match="legacy_signal_rule_catalog_disabled"):
        describe_indicator_rules("market_profile")


def test_build_signal_overlays_requires_registered_overlay_contract() -> None:
    signal = BaseSignal(
        type="breakout",
        symbol="ES",
        time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
        metadata={},
    )

    with pytest.raises(ValueError, match="overlay spec missing"):
        build_signal_overlays("unknown_indicator", [signal], plot_df=[])
