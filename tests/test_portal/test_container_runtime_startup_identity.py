from __future__ import annotations

import pytest

from portal.backend.service.bots.container_runtime import _resolve_backend_run_id


def test_container_runtime_requires_backend_owned_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QT_BOT_RUNTIME_RUN_ID", raising=False)

    with pytest.raises(
        RuntimeError,
        match="QT_BOT_RUNTIME_RUN_ID is required",
    ):
        _resolve_backend_run_id("bot-1")


def test_container_runtime_uses_exact_backend_owned_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QT_BOT_RUNTIME_RUN_ID", "run-backend-1")

    assert _resolve_backend_run_id("bot-1") == "run-backend-1"
