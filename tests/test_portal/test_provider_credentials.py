from __future__ import annotations

from portal.backend.service.providers import provider_service
from portal.backend.service.providers import secret_status


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


def test_coinbase_feature_contract_exposes_only_implemented_capabilities(monkeypatch):
    monkeypatch.setattr(secret_status, "_credential_refs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(secret_status, "has_credentials", lambda *_args, **_kwargs: False)

    providers = provider_service.provider_payloads()
    coinbase = next(item for item in providers if item["id"] == "COINBASE")
    venue = coinbase["venues"][0]
    features = venue["feature_contract"]["features"]

    assert features["instrument_metadata"]["state"] == "available"
    assert features["instrument_metadata"]["auth"] == "public"
    assert features["open_interest_current"]["state"] == "available"
    assert features["open_interest_current"]["auth"] == "public"
    assert features["account_fees"]["state"] == "missing_secrets"
    assert features["account_fees"]["auth"] == "credentials"
    assert "funding_current" not in features
    assert "orders" not in features


def test_coinbase_public_instrument_validation_does_not_require_credentials(monkeypatch):
    monkeypatch.setattr(secret_status, "_credential_refs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(secret_status, "has_credentials", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(provider_service, "resolve_status", secret_status.resolve_status)

    valid, errors, normalized = provider_service.validate_provider_venue(
        "COINBASE", "COINBASE_DIRECT"
    )

    assert valid is True
    assert errors == {}
    assert normalized == {"provider_id": "COINBASE", "venue_id": "COINBASE_DIRECT"}
