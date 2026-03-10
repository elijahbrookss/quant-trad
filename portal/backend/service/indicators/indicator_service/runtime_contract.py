from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT = "engine_snapshot_v1"


def assert_engine_signal_runtime_path(
    payload: Mapping[str, Any],
    *,
    context: str,
    indicator_id: str | None = None,
) -> str:
    runtime_path = str(payload.get("runtime_path") or "").strip()
    if runtime_path != SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT:
        indicator_context = f" indicator_id={indicator_id}" if indicator_id else ""
        raise RuntimeError(
            f"{context}: runtime_path_mismatch{indicator_context} "
            f"runtime_path={runtime_path or 'missing'} expected={SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT}"
        )
    return runtime_path


__all__ = [
    "SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT",
    "assert_engine_signal_runtime_path",
]
