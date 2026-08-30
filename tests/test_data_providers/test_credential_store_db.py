from __future__ import annotations

import json
import os
import uuid
from types import SimpleNamespace

from cryptography.fernet import Fernet
import pytest
from sqlalchemy import create_engine, text

from data_providers.services import credential_store
from portal.backend.db.session import Database


pytestmark = pytest.mark.db


def test_credentials_stay_encrypted_private_and_key_bound() -> None:
    """Exercise the credential boundary against the isolated PostgreSQL route."""

    dsn = str(os.environ.get("PG_DSN") or "").strip()
    assert dsn, "the isolated database test route must supply PG_DSN"

    bootstrap = Database(dsn)
    assert bootstrap.ensure_schema(), str(bootstrap.last_error)

    engine = create_engine(dsn, future=True)
    test_id = uuid.uuid4().hex[:16]
    credential_ref = f"qt-test-credential-{test_id}"
    provider_id = f"QT_TEST_PROVIDER_{test_id}"
    venue_id = f"QT_TEST_VENUE_{test_id}"
    first_secrets = {
        "api_key": "QT_TEST_API_KEY_PLAINTEXT_DO_NOT_USE",
        "api_secret": "QT_TEST_API_SECRET_PLAINTEXT_DO_NOT_USE",
    }
    rotated_secrets = {
        "api_key": "QT_TEST_ROTATED_KEY_PLAINTEXT_DO_NOT_USE",
        "api_secret": "QT_TEST_ROTATED_SECRET_PLAINTEXT_DO_NOT_USE",
    }
    correct_key = Fernet.generate_key().decode("ascii")
    settings = SimpleNamespace(
        database=SimpleNamespace(dsn=dsn),
        security=SimpleNamespace(provider_credential_key=correct_key),
    )

    original_engine = credential_store._ENGINE
    original_fernet = credential_store._FERNET
    original_schema_ready = credential_store._SCHEMA_READY
    original_settings = credential_store._settings
    try:
        credential_store._ENGINE = engine
        credential_store._FERNET = None
        credential_store._SCHEMA_READY = False
        credential_store._settings = lambda: settings

        saved = credential_store.save_credentials(
            provider_id,
            venue_id,
            first_secrets,
            credential_ref=credential_ref,
            environment="paper",
            display_name="Synthetic credential confinement test",
        )
        assert credential_store.load_credentials(
            provider_id,
            venue_id,
            credential_ref=credential_ref,
            environment="paper",
            mark_used=False,
        ) == first_secrets

        with engine.begin() as conn:
            first_row = conn.execute(
                text(
                    """
                    SELECT secrets_encrypted, secret_version
                    FROM portal_provider_credential_refs
                    WHERE credential_ref = :credential_ref
                    """
                ),
                {"credential_ref": credential_ref},
            ).mappings().one()

        first_ciphertext = str(first_row["secrets_encrypted"])
        assert int(first_row["secret_version"]) == 1
        assert first_ciphertext
        assert all(value not in first_ciphertext for value in first_secrets.values())

        public_metadata = saved.to_public_dict()
        assert "secrets" not in public_metadata
        assert "secrets_encrypted" not in public_metadata
        assert all(
            secret not in json.dumps(public_metadata, sort_keys=True)
            for secret in first_secrets.values()
        )
        listed = credential_store.list_credentials(
            provider_id=provider_id,
            venue_id=venue_id,
        )
        assert [item.credential_ref for item in listed] == [credential_ref]
        assert all(
            secret not in json.dumps(listed[0].to_public_dict(), sort_keys=True)
            for secret in first_secrets.values()
        )

        settings.security.provider_credential_key = Fernet.generate_key().decode("ascii")
        credential_store._FERNET = None
        with pytest.raises(RuntimeError, match="Failed to decrypt provider credentials"):
            credential_store.load_credentials(
                provider_id,
                venue_id,
                credential_ref=credential_ref,
                environment="paper",
                mark_used=False,
            )

        settings.security.provider_credential_key = correct_key
        credential_store._FERNET = None
        credential_store.save_credentials(
            provider_id,
            venue_id,
            rotated_secrets,
            credential_ref=credential_ref,
            environment="paper",
            display_name="Synthetic credential confinement test",
        )

        with engine.begin() as conn:
            rotated_row = conn.execute(
                text(
                    """
                    SELECT secrets_encrypted, secret_version
                    FROM portal_provider_credential_refs
                    WHERE credential_ref = :credential_ref
                    """
                ),
                {"credential_ref": credential_ref},
            ).mappings().one()

        rotated_ciphertext = str(rotated_row["secrets_encrypted"])
        assert int(rotated_row["secret_version"]) == 2
        assert rotated_ciphertext != first_ciphertext
        assert all(
            value not in rotated_ciphertext
            for value in (*first_secrets.values(), *rotated_secrets.values())
        )
        assert credential_store.load_credentials(
            provider_id,
            venue_id,
            credential_ref=credential_ref,
            environment="paper",
            mark_used=False,
        ) == rotated_secrets
    finally:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        DELETE FROM portal_provider_credential_refs
                        WHERE credential_ref = :credential_ref
                        """
                    ),
                    {"credential_ref": credential_ref},
                )
        finally:
            engine.dispose()
            bootstrap.reset_connection_state()
            credential_store._ENGINE = original_engine
            credential_store._FERNET = original_fernet
            credential_store._SCHEMA_READY = original_schema_ready
            credential_store._settings = original_settings
