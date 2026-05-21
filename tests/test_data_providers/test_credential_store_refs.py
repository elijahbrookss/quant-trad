from __future__ import annotations

import pytest

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
