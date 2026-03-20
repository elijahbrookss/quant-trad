from __future__ import annotations

import pytest
import yaml

import core.settings as settings_module
from core.settings import get_settings


def test_settings_applies_single_underscore_env_overrides(monkeypatch):
    monkeypatch.setenv("QT_CONFIG_PROFILE", "dev")
    monkeypatch.setenv("QT_BOT_RUNTIME_IMAGE", "quanttrad-backend:test")
    monkeypatch.setenv("QT_WORKERS_INDICATORS_INDEX", "2")
    monkeypatch.setenv("QT_WORKERS_INDICATORS_TOTAL", "7")
    monkeypatch.setenv("QT_REPORTS_ARTIFACTS_OUTPUT_FORMAT", "csv")
    monkeypatch.setenv("PG_DSN", "postgresql://example/test")

    settings = get_settings(force_reload=True)

    assert settings.bot_runtime.image == "quanttrad-backend:test"
    assert settings.workers.indicators.index == 2
    assert settings.workers.indicators.total == 7
    assert settings.reports.artifacts.output_format == "csv"
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


def test_yaml_defaults_cover_all_canonical_env_bindings():
    sentinel = object()
    defaults = yaml.safe_load(settings_module._DEFAULTS_FILE.read_text()) or {}
    dev = yaml.safe_load((settings_module._CONFIG_DIR / "dev.yaml").read_text()) or {}
    prod = yaml.safe_load((settings_module._CONFIG_DIR / "prod.yaml").read_text()) or {}
    merged_dev = settings_module._deep_merge(defaults, dev)
    merged_prod = settings_module._deep_merge(defaults, prod)

    missing = []
    for env_name, path in settings_module._ENV_BINDINGS:
        in_defaults = settings_module._path_get(defaults, path, sentinel) is not sentinel
        in_dev = settings_module._path_get(merged_dev, path, sentinel) is not sentinel
        in_prod = settings_module._path_get(merged_prod, path, sentinel) is not sentinel
        if not (in_defaults or in_dev or in_prod):
            missing.append((env_name, ".".join(path)))

    assert settings_module._path_get(defaults, ("profile",), sentinel) == "dev"
    assert settings_module._path_get(merged_prod, ("profile",), sentinel) == "prod"
    assert not missing, missing
