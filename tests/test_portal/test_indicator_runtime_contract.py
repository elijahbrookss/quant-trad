from __future__ import annotations

import pytest

from portal.backend.service.indicators.indicator_service.runtime_contract import (
    SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
    assert_engine_signal_runtime_path,
)


def test_assert_engine_signal_runtime_path_accepts_engine_snapshot() -> None:
    payload = {"runtime_path": SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT}
    result = assert_engine_signal_runtime_path(
        payload,
        context="test_context",
        indicator_id="ind-1",
    )
    assert result == SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT


def test_assert_engine_signal_runtime_path_rejects_non_engine_path() -> None:
    payload = {"runtime_path": "legacy_path_v1"}
    with pytest.raises(RuntimeError, match="runtime_path_mismatch"):
        assert_engine_signal_runtime_path(
            payload,
            context="test_context",
            indicator_id="ind-1",
        )
