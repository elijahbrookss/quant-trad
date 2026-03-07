"""Smoke tests guarding import boundaries between portal and provider stacks."""

from __future__ import annotations

import importlib

import pytest

pytest.importorskip("pandas")

@pytest.mark.unit
@pytest.mark.core
@pytest.mark.web
@pytest.mark.smoke
@pytest.mark.parametrize(
    "module_name",
    [
        "portal.backend.service.indicators.indicator_service.signals",
        "portal.backend.service.providers.persistence_bootstrap",
        "data_providers.providers.factory",
    ],
)
def test_import_boundary_modules_load_without_sdk_side_effect_failures(module_name: str) -> None:
    """These imports must be resilient at collection time across CI environments."""

    module = importlib.import_module(module_name)
    assert module is not None
