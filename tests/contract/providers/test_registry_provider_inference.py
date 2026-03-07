from __future__ import annotations

import pytest

pytest.importorskip("pandas")

from data_providers.registry import _Registry


class _InlineProvider:
    pass


def test_provider_decorator_infers_implementation_from_class():
    registry = _Registry()

    @registry.provider(
        id="INLINE",
        label="Inline",
        supported_venues=["INLINE"],
    )
    class InlineProvider(_InlineProvider):
        pass

    cfg = registry.get_provider("INLINE")
    assert cfg is not None
    assert cfg.implementation_class == "InlineProvider"
    assert cfg.implementation_module == __name__


def test_provider_decorator_infers_implementation_from_registration_function():
    registry = _Registry()

    class _FuncProvider:
        pass

    @registry.provider(
        id="FUNC",
        label="Func",
        supported_venues=["FUNC"],
    )
    def _register_func_provider():
        return _FuncProvider

    cfg = registry.get_provider("FUNC")
    assert cfg is not None
    assert cfg.implementation_class == "_FuncProvider"
    assert cfg.implementation_module == __name__
