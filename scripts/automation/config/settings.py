# scripts/automation/config/settings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# ---- Project root + dotenv loading ----


# 1) Load the same secrets.env your test file uses
#  (absolute path so imports don't break it)
# load_dotenv(dotenv_path="/home/jorge/projects/quant-trad/secrets.env")
# parents[0] = notion
# parents[1] = automation
# parents[2] = scripts
# parents[3] = quant-trad (repo root)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Load both .env and secrets.env if they exist
for env_name in (".env", "secrets.env"):
    env_path = PROJECT_ROOT / env_name
    if env_path.exists():
        load_dotenv(env_path)


# ---- Helpers ----

class MissingEnvError(RuntimeError):
    pass


def require_env(key: str) -> str:
    """Get a required env var or raise a nice error."""
    value = os.getenv(key)
    if value is None or value == "":
        raise MissingEnvError(f"Missing required env var: {key}")
    return value


def optional_env(key: str, default: str | None = None) -> str | None:
    """Get an optional env var with a default."""
    return os.getenv(key, default)


# ---- Domain-specific settings ----

@dataclass(frozen=True)
class NotionSettings:
    token: str = require_env("NOTION_TOKEN")
    release_db_id: str = require_env("NOTION_RELEASE_DB_ID")


@dataclass(frozen=True)
class MastodonSettings:
    base_url: str = require_env("MASTODON_BASE_URL")
    access_token: str = require_env("MASTODON_ACCESS_TOKEN")


def debug_settings():
    print("---- SETTINGS DEBUG ----")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")

    print("\nLoaded Notion Settings:")
    try:
        n = NotionSettings()
        print(f"  NOTION_TOKEN: {n.token[:6]}... (hidden)")
        print(f"  RELEASE_DB_ID: {n.release_db_id}")
    except Exception as e:
        print("  ERROR loading Notion settings:", e)

    print("\nLoaded Mastodon Settings:")
    try:
        m = MastodonSettings()
        print(f"  BASE_URL: {m.base_url}")
        print(f"  ACCESS_TOKEN: {m.access_token[:6]}... (hidden)")
    except Exception as e:
        print("  ERROR loading Mastodon settings:", e)

    print("-------------------------")
