from __future__ import annotations

import pytest
import yaml

import core.settings as settings_module
from core.settings import get_settings


def test_settings_applies_single_underscore_env_overrides(monkeypatch, request):
    request.addfinalizer(settings_module.clear_settings_cache)
    monkeypatch.setenv("QT_CONFIG_PROFILE", "dev")
    monkeypatch.setenv("QT_BOT_RUNTIME_IMAGE", "quanttrad-backend:test")
    monkeypatch.setenv("QT_BOT_RUNTIME_BOTLENS_PERSIST_OBSERVER_CONTINUITY", "true")
    monkeypatch.setenv("QT_WORKERS_INDICATORS_INDEX", "2")
    monkeypatch.setenv("QT_WORKERS_INDICATORS_TOTAL", "7")
    monkeypatch.setenv("QT_WORKERS_INDICATORS_IDLE_SLEEP_MAX_SECONDS", "3.5")
    monkeypatch.setenv("QT_WORKERS_RESEARCH_PROCESSES", "3")
    monkeypatch.setenv("QT_WORKERS_RESEARCH_INDEX", "1")
    monkeypatch.setenv("QT_WORKERS_RESEARCH_TOTAL", "3")
    monkeypatch.setenv("QT_WORKERS_COLLECTORS_IDLE_SLEEP_MAX_SECONDS", "7.5")
    monkeypatch.setenv(
        "QT_WORKERS_COLLECTORS_SHUTDOWN_DRAIN_TIMEOUT_SECONDS", "240"
    )
    monkeypatch.setenv("QT_ASYNC_JOBS_RECLAIM_INTERVAL_SECONDS", "45")
    monkeypatch.setenv("QT_BOT_RUNTIME_WATCHDOG_CLOCK_GAP_THRESHOLD_SECONDS", "42")
    monkeypatch.setenv("QT_REPORTS_ARTIFACTS_OUTPUT_FORMAT", "csv")
    monkeypatch.setenv("QT_REPORTS_MATERIALIZATION_TERMINAL_AUTO_ENQUEUE_ENABLED", "true")
    monkeypatch.setenv("PG_DSN", "postgresql://example/test")

    settings = get_settings(force_reload=True)

    assert settings.bot_runtime.image == "quanttrad-backend:test"
    assert settings.bot_runtime.botlens.persist_observer_continuity is True
    assert settings.workers.indicators.index == 2
    assert settings.workers.indicators.total == 7
    assert settings.workers.indicators.idle_sleep_max_seconds == 3.5
    assert settings.workers.research.processes == 3
    assert settings.workers.research.index == 1
    assert settings.workers.research.total == 3
    assert settings.workers.collectors.processes == 1
    assert settings.workers.collectors.idle_sleep_max_seconds == 7.5
    assert settings.workers.collectors.shutdown_drain_timeout_seconds == 240.0
    assert settings.async_jobs.reclaim_interval_seconds == 45.0
    assert settings.bot_runtime.watchdog.clock_gap_threshold_seconds == 42.0
    assert settings.reports.artifacts.output_format == "csv"
    assert settings.reports.materialization.terminal_auto_enqueue_enabled is True
    assert settings.database.dsn == "postgresql://example/test"


def test_config_file_cannot_define_a_second_database_dsn_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    custom_config = tmp_path / "custom.yaml"
    custom_config.write_text(
        "database:\n  dsn: postgresql://configuration-file/not-authoritative\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("QT_CONFIG_FILE", str(custom_config))
    monkeypatch.delenv("PG_DSN", raising=False)
    settings_module.clear_settings_cache()

    with pytest.raises(
        RuntimeError,
        match=r"database\.dsn must be supplied through PG_DSN",
    ):
        get_settings(force_reload=True)


def test_canonical_retention_environment_bindings(monkeypatch, request):
    request.addfinalizer(settings_module.clear_settings_cache)
    monkeypatch.setenv("QT_MARKET_DATA_LIFECYCLE_CANONICAL_HOT_DAYS", "14")
    monkeypatch.setenv("QT_MARKET_DATA_LIFECYCLE_CANONICAL_HOT_DAYS_BY_FACT_TYPE", '{"market.trade": 7}')
    monkeypatch.setenv("QT_MARKET_DATA_LIFECYCLE_CANONICAL_HOT_PAYLOAD_BUDGET_BYTES", "123456")
    monkeypatch.setenv("QT_MARKET_DATA_LIFECYCLE_CANONICAL_ARCHIVE_FILESYSTEM_BUDGET_BYTES", "654321")
    policy = get_settings(force_reload=True).market_data_lifecycle.canonical_retention
    assert policy.hot_days == 14 and policy.hot_days_by_fact_type == {"market.trade": 7}
    assert policy.hot_payload_budget_bytes == 123456
    assert policy.archive_filesystem_budget_bytes == 654321


def test_canonical_executor_environment_bindings(monkeypatch, request):
    request.addfinalizer(settings_module.clear_settings_cache)
    values = {"EXECUTION_ENABLED": True, "MAX_STEPS_PER_RUN": 3, "MAX_RUN_SECONDS": 120,
              "EXECUTION_STATEMENT_TIMEOUT_MS": 4321, "MAX_PAGE_ROWS": 17,
              "MAX_PAGE_LOGICAL_BYTES": 100000, "MAX_VERIFICATION_BYTES": 900000,
              "MAX_VERIFICATION_OBJECTS": 80, "MAX_VERIFICATION_PAGES": 99}
    for name, value in values.items():
        monkeypatch.setenv("QT_MARKET_DATA_LIFECYCLE_CANONICAL_" + name, str(value).lower())
    policy = get_settings(force_reload=True).market_data_lifecycle.canonical_retention
    for name, value in values.items():
        assert getattr(policy, name.lower()) == value


def test_explicit_dotenv_disable_prevents_repository_file_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_dotenv_load(*_args, **_kwargs):
        raise AssertionError("dotenv discovery must remain disabled")

    monkeypatch.setattr(settings_module, "_ENV_LOADED", False)
    monkeypatch.setattr(settings_module, "load_dotenv", unexpected_dotenv_load)
    monkeypatch.setenv("QT_DISABLE_DOTENV", "1")

    settings_module.ensure_env_loaded()

    assert settings_module._ENV_LOADED is True


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
