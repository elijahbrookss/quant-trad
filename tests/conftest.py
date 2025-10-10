import warnings

import pytest
from dotenv import load_dotenv

warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module=r"websockets\.legacy",
)

@pytest.fixture(scope="session", autouse=True)
def load_env_once():
    load_dotenv(".env")
    load_dotenv("secrets.env")
    print("Environment variables loaded from .env and secrets.env")
