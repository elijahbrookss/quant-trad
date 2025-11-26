import asyncio
import warnings

import pytest
from dotenv import load_dotenv

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"websockets\.legacy.*",
)


@pytest.fixture(scope="session", autouse=True)
def _ensure_event_loop():
    """Guarantee a running asyncio event loop for libraries that expect it."""

    created = False
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        created = True
    try:
        yield
    finally:
        if created:
            loop = asyncio.get_event_loop()
            loop.close()

@pytest.fixture(scope="session", autouse=True)
def load_env_once():
    load_dotenv(".env")
    load_dotenv("secrets.env")
    print("Environment variables loaded from .env and secrets.env")
