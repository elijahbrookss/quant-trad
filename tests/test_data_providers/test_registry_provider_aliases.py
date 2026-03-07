from __future__ import annotations

import pytest

pytest.importorskip("pandas")

from data_providers.registry import normalize_provider_id


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("IBKR", "INTERACTIVE_BROKERS"),
        ("ibkr", "INTERACTIVE_BROKERS"),
        ("interactive_brokers", "INTERACTIVE_BROKERS"),
        ("interactive-brokers", "INTERACTIVE_BROKERS"),
    ],
)
def test_normalize_provider_id_accepts_interactive_brokers_aliases(value: str, expected: str) -> None:
    assert normalize_provider_id(value) == expected
