from __future__ import annotations

import pytest

from core.settings import get_settings


def test_settings_applies_single_underscore_env_overrides(monkeypatch):
    monkeypatch.setenv("QT_CONFIG_PROFILE", "dev")
    monkeypatch.setenv("QT_BOT_RUNTIME_IMAGE", "quanttrad-backend:test")
    monkeypatch.setenv("QT_WORKERS_QUANTLAB_INDEX", "2")
    monkeypatch.setenv("QT_WORKERS_QUANTLAB_TOTAL", "7")
    monkeypatch.setenv("PG_DSN", "postgresql://example/test")

    settings = get_settings(force_reload=True)

    assert settings.bot_runtime.image == "quanttrad-backend:test"
    assert settings.workers.quantlab.index == 2
    assert settings.workers.quantlab.total == 7
    assert settings.database.dsn == "postgresql://example/test"


def test_materialize_bot_config_flattens_bot_env_and_snapshot_interval():
    pytest.importorskip("sqlalchemy")

    from portal.backend.service.bots.container_runtime import _materialize_bot_config

    payload = {
        "id": "bot-1",
        "snapshot_interval_ms": 750,
        "bot_env": {
            "BOT_RUNTIME_STEP_TRACE_QUEUE_MAX": "4000",
            "CUSTOM_FLAG": True,
        },
    }

    materialized = _materialize_bot_config(payload)

    assert materialized["SNAPSHOT_INTERVAL_MS"] == 750
    assert materialized["BOT_RUNTIME_STEP_TRACE_QUEUE_MAX"] == "4000"
    assert materialized["CUSTOM_FLAG"] is True
