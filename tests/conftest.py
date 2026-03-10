import asyncio
import os
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


def pytest_collection_modifyitems(config, items):
    run_db_tests = str(os.getenv("RUN_DB_TESTS", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if run_db_tests:
        return

    skip_db = pytest.mark.skip(
        reason="live DB tests are opt-in; set RUN_DB_TESTS=1 for manual PostgreSQL-backed runs"
    )
    for item in items:
        if "db" in item.keywords:
            item.add_marker(skip_db)
