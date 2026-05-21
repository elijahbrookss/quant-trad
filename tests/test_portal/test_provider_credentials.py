from __future__ import annotations

from portal.backend.service.providers import provider_service


def test_credential_schema_uses_registry_metadata_without_secret_values():
    schema = provider_service.credential_schema("coinbase", "coinbase_direct")

    assert schema == {
        "provider_id": "COINBASE",
        "venue_id": "COINBASE_DIRECT",
        "environment": "paper",
        "default_credential_ref": "coinbase-coinbase-direct-paper",
        "required": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
        "optional": [],
        "accepted": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
        "secrets_are_returned": False,
    }


def test_credential_schema_exposes_optional_ccxt_credentials_without_requiring_them():
    schema = provider_service.credential_schema("ccxt", "kraken_pro")

    assert schema["provider_id"] == "CCXT"
    assert schema["venue_id"] == "KRAKEN_PRO"
    assert schema["required"] == []
    assert schema["optional"] == ["CCXT_API_KEY", "CCXT_SECRET", "CCXT_PASSWORD"]
    assert schema["accepted"] == ["CCXT_API_KEY", "CCXT_SECRET", "CCXT_PASSWORD"]
