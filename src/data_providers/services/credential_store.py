"""Encrypted provider credential references backed by Postgres."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Dict, Iterable, Optional

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from core.logger import logger
from core.settings import get_settings
from data_providers.registry import normalize_provider_id, normalize_venue_id

_TABLE_NAME = "portal_provider_credential_refs"
_ENGINE: Optional[Engine] = None
_FERNET: Optional[Fernet] = None
_SCHEMA_READY = False
_CREDENTIAL_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


@dataclass(frozen=True)
class CredentialMetadata:
    credential_ref: str
    provider_id: str
    venue_id: str
    environment: str
    display_name: Optional[str]
    status: str
    required_secret_keys: list[str]
    validation: dict[str, Any]
    created_at: Optional[str]
    updated_at: Optional[str]
    last_validated_at: Optional[str]
    last_used_at: Optional[str]
    revoked_at: Optional[str]

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "credential_ref": self.credential_ref,
            "provider_id": self.provider_id,
            "venue_id": self.venue_id,
            "environment": self.environment,
            "display_name": self.display_name,
            "status": self.status,
            "required_secret_keys": list(self.required_secret_keys),
            "validation": dict(self.validation),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_validated_at": self.last_validated_at,
            "last_used_at": self.last_used_at,
            "revoked_at": self.revoked_at,
        }


def _settings():
    return get_settings()


def _dsn() -> str:
    value = _settings().database.dsn
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
            raise RuntimeError(
                "QT_SECURITY_PROVIDER_CREDENTIAL_KEY must be a valid 32-byte urlsafe base64 key."
            ) from exc
    return _FERNET


def _ensure_provider_key() -> str:
    """Fetch the provider credential encryption key and fail loud if missing."""

    existing = str(_settings().security.provider_credential_key or "").strip()
    if existing:
        return existing
    raise RuntimeError(
        "QT_SECURITY_PROVIDER_CREDENTIAL_KEY is required for provider credential encryption/decryption."
    )


def normalize_environment(value: Optional[str]) -> str:
    text = str(value or "paper").strip().lower()
    return text or "paper"


def normalize_credential_ref(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("credential_ref is required.")
    if not _CREDENTIAL_REF_RE.match(text):
        raise ValueError(
            "credential_ref may contain only letters, numbers, '_', '-', '.', ':' and must be 1-128 characters."
        )
    return text


def default_credential_ref(
    provider_id: Optional[str],
    venue_id: Optional[str],
    environment: Optional[str] = None,
) -> str:
    provider = (normalize_provider_id(provider_id) or "provider").lower().replace("_", "-")
    venue = (normalize_venue_id(venue_id) or "default").lower().replace("_", "-")
    env = normalize_environment(environment).replace("_", "-")
    return normalize_credential_ref(f"{provider}-{venue}-{env}")


def _normalize_provider(value: Optional[str]) -> str:
    normalized = normalize_provider_id(value)
    if not normalized:
        raise ValueError("provider_id is required.")
    return normalized


def _normalize_venue(value: Optional[str]) -> str:
    return normalize_venue_id(value) or ""


def _secret_keys(values: Iterable[str] | None, secrets: Dict[str, str]) -> list[str]:
    keys = [str(key).strip() for key in values or [] if str(key).strip()]
    if keys:
        return keys
    return sorted(str(key) for key in secrets.keys())


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _row_metadata(row: Any) -> CredentialMetadata:
    mapping = row._mapping if hasattr(row, "_mapping") else row
    required = mapping.get("required_secret_keys") or []
    if isinstance(required, str):
        try:
            required = json.loads(required)
        except json.JSONDecodeError:
            required = []
    validation = mapping.get("validation") or {}
    if isinstance(validation, str):
        try:
            validation = json.loads(validation)
        except json.JSONDecodeError:
            validation = {}
    return CredentialMetadata(
        credential_ref=str(mapping.get("credential_ref") or ""),
        provider_id=str(mapping.get("provider_id") or ""),
        venue_id=str(mapping.get("venue_id") or ""),
        environment=str(mapping.get("environment") or ""),
        display_name=mapping.get("display_name"),
        status=str(mapping.get("status") or "unknown"),
        required_secret_keys=[str(item) for item in required],
        validation=dict(validation),
        created_at=_iso(mapping.get("created_at")),
        updated_at=_iso(mapping.get("updated_at")),
        last_validated_at=_iso(mapping.get("last_validated_at")),
        last_used_at=_iso(mapping.get("last_used_at")),
        revoked_at=_iso(mapping.get("revoked_at")),
    )


def ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
        credential_ref TEXT PRIMARY KEY,
        provider_id TEXT NOT NULL,
        venue_id TEXT NOT NULL DEFAULT '',
        environment TEXT NOT NULL DEFAULT 'paper',
        display_name TEXT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        secrets_encrypted TEXT NOT NULL,
        secret_version INTEGER NOT NULL DEFAULT 1,
        required_secret_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
        validation JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_validated_at TIMESTAMPTZ NULL,
        last_used_at TIMESTAMPTZ NULL,
        revoked_at TIMESTAMPTZ NULL
    );
    """
    with _engine().begin() as conn:
        exists = conn.execute(text("SELECT to_regclass(:table_name)"), {"table_name": _TABLE_NAME}).scalar()
        if not exists:
            logger.warning("provider_credential_refs_table_missing_provisioning | table=%s", _TABLE_NAME)
        conn.execute(text(ddl))
    _SCHEMA_READY = True


def _select_by_ref(credential_ref: str) -> Any | None:
    sql = text(
        f"""
        SELECT *
        FROM {_TABLE_NAME}
        WHERE credential_ref = :credential_ref
        """
    )
    with _engine().begin() as conn:
        return conn.execute(sql, {"credential_ref": credential_ref}).one_or_none()


def _select_default(provider_id: str, venue_id: str, environment: str) -> Any | None:
    sql = text(
        f"""
        SELECT *
        FROM {_TABLE_NAME}
        WHERE provider_id = :provider_id
          AND venue_id = :venue_id
          AND environment = :environment
          AND revoked_at IS NULL
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """
    )
    with _engine().begin() as conn:
        row = conn.execute(
            sql,
            {
                "provider_id": provider_id,
                "venue_id": venue_id,
                "environment": environment,
            },
        ).one_or_none()
        if row is None and venue_id:
            row = conn.execute(
                sql,
                {
                    "provider_id": provider_id,
                    "venue_id": "",
                    "environment": environment,
                },
            ).one_or_none()
    return row


def save_credentials(
    provider_id: Optional[str],
    venue_id: Optional[str],
    secrets: Dict[str, str],
    *,
    credential_ref: Optional[str] = None,
    environment: Optional[str] = None,
    display_name: Optional[str] = None,
    required_secret_keys: Optional[list[str]] = None,
) -> CredentialMetadata:
    if not secrets:
        raise ValueError("No secrets provided.")

    ensure_schema()
    normalized_provider = _normalize_provider(provider_id)
    normalized_venue = _normalize_venue(venue_id)
    normalized_environment = normalize_environment(environment)
    normalized_ref = normalize_credential_ref(
        credential_ref or default_credential_ref(normalized_provider, normalized_venue, normalized_environment)
    )

    existing = _select_by_ref(normalized_ref)
    if existing is not None:
        existing_meta = _row_metadata(existing)
        if (
            existing_meta.provider_id != normalized_provider
            or existing_meta.venue_id != normalized_venue
            or existing_meta.environment != normalized_environment
        ):
            raise ValueError(
                "credential_ref already belongs to "
                f"{existing_meta.provider_id}/{existing_meta.venue_id or '<provider>'}/{existing_meta.environment}."
            )

    cleaned = {str(key): str(value) for key, value in secrets.items() if str(value).strip()}
    if not cleaned:
        raise ValueError("No non-empty secrets provided.")

    encrypted = _cipher().encrypt(json.dumps(cleaned, sort_keys=True).encode("utf-8"))
    required = _secret_keys(required_secret_keys, cleaned)
    sql = text(
        f"""
        INSERT INTO {_TABLE_NAME} (
            credential_ref,
            provider_id,
            venue_id,
            environment,
            display_name,
            status,
            secrets_encrypted,
            secret_version,
            required_secret_keys,
            validation,
            created_at,
            updated_at,
            revoked_at
        )
        VALUES (
            :credential_ref,
            :provider_id,
            :venue_id,
            :environment,
            :display_name,
            'active',
            :secrets,
            1,
            CAST(:required_secret_keys AS jsonb),
            CAST(:validation AS jsonb),
            now(),
            now(),
            NULL
        )
        ON CONFLICT (credential_ref)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            status = 'active',
            secrets_encrypted = EXCLUDED.secrets_encrypted,
            secret_version = {_TABLE_NAME}.secret_version + 1,
            required_secret_keys = EXCLUDED.required_secret_keys,
            validation = EXCLUDED.validation,
            updated_at = now(),
            revoked_at = NULL
        RETURNING *
        """
    )
    validation = {
        "status": "not_checked",
        "message": "Credential payload was stored but not provider-validated.",
    }
    try:
        with _engine().begin() as conn:
            row = conn.execute(
                sql,
                {
                    "credential_ref": normalized_ref,
                    "provider_id": normalized_provider,
                    "venue_id": normalized_venue,
                    "environment": normalized_environment,
                    "display_name": str(display_name).strip() if display_name else None,
                    "secrets": encrypted.decode("utf-8"),
                    "required_secret_keys": json.dumps(required),
                    "validation": json.dumps(validation),
                },
            ).one()
    except SQLAlchemyError as exc:
        logger.error(
            "provider_credentials_upsert_failed | credential_ref=%s provider=%s venue=%s environment=%s error=%s",
            normalized_ref,
            normalized_provider,
            normalized_venue,
            normalized_environment,
            exc.__class__.__name__,
        )
        raise RuntimeError("Unable to persist provider credentials. Check database connectivity.") from exc

    logger.info(
        "provider_credentials_saved | credential_ref=%s provider=%s venue=%s environment=%s required=%s",
        normalized_ref,
        normalized_provider,
        normalized_venue,
        normalized_environment,
        required,
    )
    return _row_metadata(row)


def _decrypt_row(row: Any, *, mark_used: bool) -> Dict[str, str]:
    mapping = row._mapping if hasattr(row, "_mapping") else row
    meta = _row_metadata(row)
    encrypted: str = str(mapping.get("secrets_encrypted") or "")
    try:
        decrypted = _cipher().decrypt(encrypted.encode("utf-8"))
    except (InvalidToken, ValueError) as exc:
        logger.error(
            "provider_credentials_decrypt_failed | credential_ref=%s provider=%s venue=%s environment=%s error=%s",
            meta.credential_ref,
            meta.provider_id,
            meta.venue_id,
            meta.environment,
            exc.__class__.__name__,
        )
        raise RuntimeError(
            "Failed to decrypt provider credentials. "
            "Check QT_SECURITY_PROVIDER_CREDENTIAL_KEY and re-save secrets."
        ) from exc
    try:
        payload = json.loads(decrypted.decode("utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        raise RuntimeError("Stored credentials payload is invalid JSON.") from exc

    if mark_used:
        sql = text(
            f"""
            UPDATE {_TABLE_NAME}
            SET last_used_at = now()
            WHERE credential_ref = :credential_ref
            """
        )
        with _engine().begin() as conn:
            conn.execute(sql, {"credential_ref": meta.credential_ref})

    return {str(k): str(v) for k, v in (payload or {}).items()}


def load_credentials(
    provider_id: Optional[str],
    venue_id: Optional[str],
    *,
    credential_ref: Optional[str] = None,
    environment: Optional[str] = None,
    mark_used: bool = True,
    warn_missing: bool = True,
) -> Optional[Dict[str, str]]:
    ensure_schema()
    normalized_provider = _normalize_provider(provider_id)
    normalized_venue = _normalize_venue(venue_id)
    normalized_environment = normalize_environment(environment)

    row = None
    if credential_ref:
        normalized_ref = normalize_credential_ref(credential_ref)
        row = _select_by_ref(normalized_ref)
        if row is None:
            if warn_missing:
                logger.warning("provider_credentials_missing | credential_ref=%s", normalized_ref)
            return None
        meta = _row_metadata(row)
        if meta.provider_id != normalized_provider or meta.venue_id != normalized_venue:
            raise RuntimeError(
                "Credential reference does not match requested provider/venue "
                f"| credential_ref={normalized_ref} provider={normalized_provider} venue={normalized_venue}"
            )
        if meta.revoked_at:
            if warn_missing:
                logger.warning("provider_credentials_revoked | credential_ref=%s", normalized_ref)
            return None
    else:
        row = _select_default(normalized_provider, normalized_venue, normalized_environment)

    if row is None:
        if warn_missing:
            logger.warning(
                "provider_credentials_missing | provider=%s venue=%s environment=%s",
                normalized_provider,
                normalized_venue,
                normalized_environment,
            )
        return None

    return _decrypt_row(row, mark_used=mark_used)


def has_credentials(
    provider_id: Optional[str],
    venue_id: Optional[str],
    required_keys: Optional[list[str]] = None,
    *,
    credential_ref: Optional[str] = None,
    environment: Optional[str] = None,
) -> bool:
    secrets = load_credentials(
        provider_id,
        venue_id,
        credential_ref=credential_ref,
        environment=environment,
        mark_used=False,
        warn_missing=False,
    )
    if not secrets:
        return False
    if not required_keys:
        return True
    return all(secrets.get(key) for key in required_keys)


def list_credentials(
    *,
    provider_id: Optional[str] = None,
    venue_id: Optional[str] = None,
    include_revoked: bool = False,
) -> list[CredentialMetadata]:
    ensure_schema()
    normalized_provider = normalize_provider_id(provider_id)
    normalized_venue = normalize_venue_id(venue_id)
    filters = []
    params: dict[str, Any] = {}
    if normalized_provider:
        filters.append("provider_id = :provider_id")
        params["provider_id"] = normalized_provider
    if normalized_venue:
        filters.append("venue_id = :venue_id")
        params["venue_id"] = normalized_venue
    if not include_revoked:
        filters.append("revoked_at IS NULL")
    where = "WHERE " + " AND ".join(filters) if filters else ""
    sql = text(
        f"""
        SELECT
            credential_ref,
            provider_id,
            venue_id,
            environment,
            display_name,
            status,
            required_secret_keys,
            validation,
            created_at,
            updated_at,
            last_validated_at,
            last_used_at,
            revoked_at
        FROM {_TABLE_NAME}
        {where}
        ORDER BY provider_id, venue_id, environment, updated_at DESC
        """
    )
    with _engine().begin() as conn:
        rows = conn.execute(sql, params).all()
    return [_row_metadata(row) for row in rows]


def validate_credentials(
    credential_ref: str,
    *,
    required_keys: Optional[list[str]] = None,
) -> CredentialMetadata:
    ensure_schema()
    normalized_ref = normalize_credential_ref(credential_ref)
    row = _select_by_ref(normalized_ref)
    if row is None:
        raise ValueError(f"Credential reference not found: {normalized_ref}")
    meta = _row_metadata(row)
    if meta.revoked_at:
        raise ValueError(f"Credential reference is revoked: {normalized_ref}")

    secrets = _decrypt_row(row, mark_used=False)
    required = required_keys if required_keys is not None else meta.required_secret_keys
    missing = [key for key in required if not secrets.get(key)]
    if missing:
        status = "invalid"
        validation = {"status": "failed", "missing": missing}
    else:
        status = "active"
        validation = {
            "status": "passed",
            "checked": sorted(required),
            "message": "Credential payload decrypted and required keys are present.",
        }

    sql = text(
        f"""
        UPDATE {_TABLE_NAME}
        SET
            status = :status,
            validation = CAST(:validation AS jsonb),
            last_validated_at = now(),
            updated_at = now()
        WHERE credential_ref = :credential_ref
        RETURNING *
        """
    )
    with _engine().begin() as conn:
        updated = conn.execute(
            sql,
            {
                "credential_ref": normalized_ref,
                "status": status,
                "validation": json.dumps(validation),
            },
        ).one()
    return _row_metadata(updated)


def revoke_credentials(credential_ref: str) -> CredentialMetadata:
    ensure_schema()
    normalized_ref = normalize_credential_ref(credential_ref)
    sql = text(
        f"""
        UPDATE {_TABLE_NAME}
        SET status = 'revoked', revoked_at = now(), updated_at = now()
        WHERE credential_ref = :credential_ref
        RETURNING *
        """
    )
    with _engine().begin() as conn:
        row = conn.execute(sql, {"credential_ref": normalized_ref}).one_or_none()
    if row is None:
        raise ValueError(f"Credential reference not found: {normalized_ref}")
    logger.info("provider_credentials_revoked | credential_ref=%s", normalized_ref)
    return _row_metadata(row)


__all__ = [
    "CredentialMetadata",
    "default_credential_ref",
    "ensure_schema",
    "has_credentials",
    "list_credentials",
    "load_credentials",
    "normalize_credential_ref",
    "normalize_environment",
    "revoke_credentials",
    "save_credentials",
    "validate_credentials",
]
