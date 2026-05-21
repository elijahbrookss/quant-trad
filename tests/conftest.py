import asyncio
import inspect
import os
from pathlib import Path
import warnings

import pytest
from dotenv import load_dotenv

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"websockets\.legacy.*",
)


_SESSION_EVENT_LOOP: asyncio.AbstractEventLoop | None = None


def _install_session_event_loop() -> None:
    """Install a default loop early so eventkit/ib_insync imports stay quiet."""

    global _SESSION_EVENT_LOOP
    if _SESSION_EVENT_LOOP is not None:
        return
    try:
        asyncio.get_running_loop()
        return
    except RuntimeError:
        pass
    _SESSION_EVENT_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_SESSION_EVENT_LOOP)


_install_session_event_loop()


_CI_PROFILES = {
    "pr",
    "core",
    "provider",
    "runtime",
    "botlens",
    "web",
    "cli",
    "reports",
    "docs",
}


def _env_flag(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _declares_db_marker(path: Path) -> bool:
    if path.suffix != ".py" or not path.name.startswith("test_"):
        return False
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return "pytest.mark.db" in source or "@pytest.mark.db" in source


def pytest_ignore_collect(collection_path, config):  # noqa: ANN001 - pytest hook type varies by version.
    if not _env_flag("QT_OMIT_DB_TESTS"):
        return False
    return _declares_db_marker(Path(str(collection_path)))


def pytest_pyfunc_call(pyfuncitem):
    if "asyncio" not in pyfuncitem.keywords or not inspect.iscoroutinefunction(pyfuncitem.obj):
        return None
    try:
        import pytest_asyncio  # noqa: F401

        return None
    except ImportError:
        pass

    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in pyfuncitem._fixtureinfo.argnames
        if name in pyfuncitem.funcargs
    }
    asyncio.run(pyfuncitem.obj(**kwargs))
    _install_session_event_loop()
    return True


@pytest.fixture(scope="session", autouse=True)
def _ensure_event_loop():
    """Guarantee a running asyncio event loop for libraries that expect it."""

    try:
        yield
    finally:
        global _SESSION_EVENT_LOOP
        if _SESSION_EVENT_LOOP is not None and not _SESSION_EVENT_LOOP.is_closed():
            _SESSION_EVENT_LOOP.close()
            _SESSION_EVENT_LOOP = None

@pytest.fixture(scope="session", autouse=True)
def load_env_once():
    load_dotenv(".env")
    load_dotenv("secrets.env")
    print("Environment variables loaded from .env and secrets.env")


def _normalise_test_path(path: str) -> tuple[str, str]:
    raw_path = Path(path)
    try:
        normalized = raw_path.relative_to(Path.cwd()).as_posix()
    except ValueError:
        normalized = raw_path.as_posix()
    return normalized, Path(normalized).name


def _ci_profile_markers_for_path(path: str) -> set[str]:
    normalized, name = _normalise_test_path(path)
    profiles: set[str] = set()

    if normalized.startswith("tests/smoke/"):
        profiles.update({"core", "web"})

    if normalized.startswith("tests/contract/providers/"):
        profiles.add("provider")
    elif normalized.startswith("tests/contract/"):
        profiles.add("docs" if "architecture_docs" in name else "core")

    if normalized.startswith("tests/test_cli/"):
        profiles.add("cli")

    if normalized.startswith("tests/test_data_providers/"):
        profiles.add("provider")

    if normalized.startswith("tests/integration/runtime/"):
        profiles.add("runtime")

    if normalized.startswith("tests/test_indicators/") or normalized.startswith("tests/test_strategies/"):
        profiles.add("core")

    if normalized in {
        "tests/test_indicator_engine_overlays.py",
        "tests/test_perf_log.py",
    }:
        profiles.add("core")

    if normalized.startswith("tests/test_reports/"):
        profiles.add("reports")

    if not normalized.startswith("tests/test_portal/"):
        return profiles

    if name.startswith("test_botlens_"):
        profiles.add("botlens")

    if (
        name.startswith("test_bot_runtime_")
        or name.startswith("test_container_runtime")
        or name.startswith("test_lifecycle_")
        or name.startswith("test_observe_only_runtime")
        or name.startswith("test_paper_market_stream")
        or name.startswith("test_runner_observability")
        or name.startswith("test_runtime_")
        or name.startswith("test_wallet_")
        or name in {
            "test_bot_projection_and_runner_contract.py",
            "test_bot_startup_orchestrator.py",
            "test_bot_watchdog.py",
            "test_bots_repo_status_contract.py",
            "test_fee_notional_cleanup.py",
            "test_margin_validation.py",
            "test_spot_execution.py",
        }
    ):
        profiles.add("runtime")

    if name.startswith("test_provider_"):
        profiles.add("provider")

    if name.startswith("test_report_") or name.startswith("test_run_research_dataset"):
        profiles.add("reports")

    if (
        name.startswith("test_strategy_")
        or name.startswith("test_indicator_")
        or name.startswith("test_bot_config_")
        or name.startswith("test_bot_run_context_")
        or name.startswith("test_bot_service_")
        or name.startswith("test_bot_strategy_")
        or name in {
            "test_async_jobs_partition_hash.py",
            "test_centralized_settings.py",
            "test_instrument_service.py",
            "test_run_storage_json_safety.py",
            "test_series_builder_incremental.py",
        }
    ):
        profiles.add("web")

    return profiles


def _is_pr_profile_test(path: str, name: str) -> bool:
    if path.startswith("tests/smoke/") or path.startswith("tests/contract/"):
        return True
    if path.startswith("tests/test_cli/"):
        return True
    if path.startswith("tests/test_data_providers/"):
        return True
    if path.startswith("tests/integration/runtime/"):
        return True
    if path.startswith("tests/test_indicators/") or path.startswith("tests/test_strategies/"):
        return True
    if path in {
        "tests/test_indicator_engine_overlays.py",
        "tests/test_perf_log.py",
    }:
        return True
    if path.startswith("tests/test_reports/"):
        return name != "test_reports_endpoints.py"
    if not path.startswith("tests/test_portal/"):
        return False

    return (
        name.startswith("test_bot_runtime_")
        or name.startswith("test_botlens_bootstrap_contracts")
        or name.startswith("test_botlens_canonical_facts")
        or name.startswith("test_botlens_domain_events")
        or name.startswith("test_botlens_event_replay_ordering")
        or name.startswith("test_botlens_execution_mode_contract")
        or name.startswith("test_botlens_runtime_state")
        or name.startswith("test_botlens_typed_deltas")
        or name.startswith("test_bots_repo_status_contract")
        or name.startswith("test_fee_")
        or name.startswith("test_indicator_runtime_contract")
        or name.startswith("test_indicator_signal_endpoint_filtering")
        or name.startswith("test_indicator_type_details")
        or name.startswith("test_lifecycle_")
        or name.startswith("test_margin_")
        or name.startswith("test_observe_only_runtime")
        or name.startswith("test_paper_market_stream")
        or name.startswith("test_provider_")
        or name.startswith("test_report_data")
        or name.startswith("test_report_execution_mode_contract")
        or name.startswith("test_run_artifact")
        or name.startswith("test_run_research_dataset")
        or name.startswith("test_run_storage_json_safety")
        or name.startswith("test_runner_observability")
        or name.startswith("test_runtime_events_repo")
        or name.startswith("test_spot_execution")
        or name.startswith("test_strategy_compile_contract")
        or name.startswith("test_strategy_preview_signal_contract")
        or name.startswith("test_strategy_read_contract")
        or name.startswith("test_strategy_rule_creation_contract")
        or name.startswith("test_strategy_service_public_api")
        or name.startswith("test_strategy_variant_resolution")
        or name.startswith("test_wallet_")
        or name in {
            "test_bot_projection_and_runner_contract.py",
            "test_bot_startup_orchestrator.py",
            "test_bot_watchdog.py",
        }
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        path, name = _normalise_test_path(str(item.path))
        profiles = _ci_profile_markers_for_path(str(item.path))
        if _is_pr_profile_test(path, name):
            profiles.add("pr")
        for profile in profiles:
            item.add_marker(getattr(pytest.mark, profile))

    ci_profile = os.getenv("QT_CI_PROFILE", "").strip()
    if ci_profile and ci_profile not in _CI_PROFILES:
        raise pytest.UsageError(
            f"unknown QT_CI_PROFILE={ci_profile!r}; expected one of {', '.join(sorted(_CI_PROFILES))}"
        )
    if ci_profile:
        selected = [item for item in items if ci_profile in item.keywords]
        deselected = [item for item in items if ci_profile not in item.keywords]
        if deselected:
            config.hook.pytest_deselected(items=deselected)
            items[:] = selected

    run_db_tests = _env_flag("RUN_DB_TESTS")
    if run_db_tests:
        return

    skip_db = pytest.mark.skip(
        reason="live DB tests are opt-in; set RUN_DB_TESTS=1 for manual PostgreSQL-backed runs"
    )
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip_db)
