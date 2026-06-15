from __future__ import annotations

import pytest

from data_providers.services import credential_store
from data_providers.services.credential_store import (
    default_credential_ref,
    normalize_credential_ref,
    normalize_environment,
)


def test_default_credential_ref_is_stable_and_slugged():
    assert default_credential_ref("coinbase", "coinbase_direct", "paper") == "coinbase-coinbase-direct-paper"
    assert default_credential_ref("CCXT", "KRAKEN_PRO", "sandbox") == "ccxt-kraken-pro-sandbox"


def test_credential_ref_validation_rejects_shell_sensitive_characters():
    assert normalize_credential_ref("coinbase-main.1") == "coinbase-main.1"
    with pytest.raises(ValueError):
        normalize_credential_ref("../coinbase")
    with pytest.raises(ValueError):
        normalize_credential_ref("coinbase/main")


def test_environment_defaults_to_paper():
    assert normalize_environment(None) == "paper"
    assert normalize_environment(" LIVE ") == "live"


class _CredentialInspector:
    def __init__(
        self,
        *,
        tables: set[str] | None = None,
        columns: set[str] | None = None,
        indexes: set[str] | None = None,
    ) -> None:
        self.tables = tables or set()
        self.columns = columns or set()
        self.indexes = indexes or set()

    def get_table_names(self) -> list[str]:
        return sorted(self.tables)

    def get_columns(self, name: str) -> list[dict[str, str]]:
        return [{"name": column} for column in sorted(self.columns)]

    def get_indexes(self, name: str) -> list[dict[str, str]]:
        return [{"name": index_name} for index_name in sorted(self.indexes)]


def test_credential_store_requires_portal_schema_bootstrap(monkeypatch):
    monkeypatch.setattr(credential_store, "_SCHEMA_READY", False)
    monkeypatch.setattr(credential_store, "_engine", lambda: object())
    monkeypatch.setattr(credential_store, "inspect", lambda _engine: _CredentialInspector())

    with pytest.raises(RuntimeError, match="portal_provider_credential_refs.*missing"):
        credential_store.ensure_schema()


def test_credential_store_validates_required_index(monkeypatch):
    monkeypatch.setattr(credential_store, "_SCHEMA_READY", False)
    monkeypatch.setattr(credential_store, "_engine", lambda: object())
    inspector = _CredentialInspector(
        tables={"portal_provider_credential_refs"},
        columns=set(credential_store._REQUIRED_COLUMNS),
        indexes=set(),
    )
    monkeypatch.setattr(credential_store, "inspect", lambda _engine: inspector)

    with pytest.raises(RuntimeError, match="ix_provider_credential_refs_provider_venue"):
        credential_store.ensure_schema()
