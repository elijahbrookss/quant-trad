"""Encrypted provider credential storage backed by Postgres."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from cryptography.fernet import Fernet, InvalidToken
from core.logger import logger

_TABLE_NAME = "portal_provider_credentials"
_ENGINE: Optional[Engine] = None
_FERNET: Optional[Fernet] = None


def _dsn() -> str:
    value = os.getenv("PG_DSN")
    if value:
        return value
    raise RuntimeError("PG_DSN is required for credential storage.")


def _engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(_dsn(), future=True)
    return _ENGINE


def _cipher() -> Fernet:
    global _FERNET
    if _FERNET is None:
        key = _ensure_provider_key()
        try:
            _FERNET = Fernet(key)
        except Exception as exc:  # pragma: no cover - configuration error
            raise RuntimeError("PROVIDER_CREDENTIAL_KEY must be a valid 32-byte urlsafe base64 key.") from exc
    return _FERNET


def _normalize(value: Optional[str]) -> str:
    return (value or "").strip().upper()


def _env_path() -> Path:
    # repo root: .../src/data_providers/services -> repo root is parents[3]
    return Path(__file__).resolve().parents[3] / ".env"


def _ensure_provider_key() -> str:
    """Fetch PROVIDER_CREDENTIAL_KEY; generate and persist to .env if missing."""
    existing = os.getenv("PROVIDER_CREDENTIAL_KEY")
    if existing:
        return existing

    generated = Fernet.generate_key().decode("utf-8")
    env_path = _env_path()
    try:
        env_path.parent.mkdir(parents=True, exist_ok=True)
        if not env_path.exists():
            env_path.touch(mode=0o600, exist_ok=True)

        # Avoid duplicating if another process just wrote it
        should_append = True
        try:
            content = env_path.read_text(encoding="utf-8")
            if "PROVIDER_CREDENTIAL_KEY" in content:
                should_append = False
        except OSError:
            should_append = True

        if should_append:
            with env_path.open("a", encoding="utf-8") as handle:
                handle.write(f"\nPROVIDER_CREDENTIAL_KEY={generated}\n")

        os.environ["PROVIDER_CREDENTIAL_KEY"] = generated
        logger.warning("provider_credential_key_created | path=%s", env_path)
    except Exception as exc:  # pragma: no cover - best-effort persistence
        os.environ["PROVIDER_CREDENTIAL_KEY"] = generated
        logger.warning("provider_credential_key_create_failed | fallback_env_only | error=%s", exc)

    return generated


def ensure_schema() -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
        provider_id TEXT NOT NULL,
        venue_id TEXT NOT NULL,
        secrets_encrypted TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (provider_id, venue_id)
    );
    """
    with _engine().begin() as conn:
        conn.execute(text(ddl))


def save_credentials(provider_id: Optional[str], venue_id: Optional[str], secrets: Dict[str, str]) -> None:
    if not secrets:
        raise ValueError("No secrets provided.")
    ensure_schema()
    payload = json.dumps(secrets)
    encrypted = _cipher().encrypt(payload.encode("utf-8"))
    normalized_provider = _normalize(provider_id)
    normalized_venue = _normalize(venue_id)
    sql = text(
        f"""
        INSERT INTO {_TABLE_NAME} (provider_id, venue_id, secrets_encrypted, created_at, updated_at)
        VALUES (:provider_id, :venue_id, :secrets, now(), now())
        ON CONFLICT (provider_id, venue_id)
        DO UPDATE SET
            secrets_encrypted = EXCLUDED.secrets_encrypted,
            updated_at = now()
        """
    )
    try:
        with _engine().begin() as conn:
            conn.execute(sql, {
                "provider_id": normalized_provider,
                "venue_id": normalized_venue,
                "secrets": encrypted.decode("utf-8"),
            })
    except SQLAlchemyError as exc:
        logger.error(
            "provider_credentials_upsert_failed | provider=%s venue=%s | error=%s",
            normalized_provider,
            normalized_venue,
            exc.__class__.__name__,
        )
        raise RuntimeError("Unable to persist provider credentials. Check database connectivity.") from exc


def load_credentials(provider_id: Optional[str], venue_id: Optional[str]) -> Optional[Dict[str, str]]:
    ensure_schema()
    normalized_provider = _normalize(provider_id)
    normalized_venue = _normalize(venue_id)
    sql = text(
        f"""
        SELECT secrets_encrypted
        FROM {_TABLE_NAME}
        WHERE provider_id = :provider_id AND venue_id = :venue_id
        """
    )
    with _engine().begin() as conn:
        row = conn.execute(sql, {
            "provider_id": normalized_provider,
            "venue_id": normalized_venue,
        }).one_or_none()
        if not row and normalized_venue:
            row = conn.execute(sql, {
                "provider_id": normalized_provider,
                "venue_id": "",
            }).one_or_none()
    if not row:
        if venue_id is not None:
            logger.warning(
                "provider_credentials_missing | provider=%s venue=%s",
                normalized_provider,
                normalized_venue,
            )
        return None
    encrypted: str = row.secrets_encrypted
    try:
        decrypted = _cipher().decrypt(encrypted.encode("utf-8"))
    except (InvalidToken, ValueError) as exc:
        logger.error(
            "provider_credentials_decrypt_failed | provider=%s venue=%s | error=%s",
            normalized_provider,
            normalized_venue,
            exc.__class__.__name__,
        )
        raise RuntimeError("Failed to decrypt provider credentials. Check PROVIDER_CREDENTIAL_KEY and re-save secrets.") from exc
    try:
        payload = json.loads(decrypted.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Stored credentials payload is invalid JSON.") from exc
    return {str(k): str(v) for k, v in (payload or {}).items()}


def has_credentials(provider_id: Optional[str], venue_id: Optional[str], required_keys: Optional[list[str]] = None) -> bool:
    secrets = load_credentials(provider_id, venue_id)
    if not secrets:
        return False
    if not required_keys:
        return True
    return all(secrets.get(key) for key in required_keys)


__all__ = ["save_credentials", "load_credentials", "has_credentials", "ensure_schema"]
