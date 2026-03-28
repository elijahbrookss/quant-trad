from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pandas", reason="pandas required for indicator output prefs roundtrip tests")

from portal.backend.service.indicators.indicator_factory import IndicatorFactory
from portal.backend.service.indicators.indicator_service.api import (
    create_instance,
    update_instance,
)
from portal.backend.service.indicators.indicator_service.context import IndicatorServiceContext
from portal.backend.service.providers.data_provider_resolver import default_resolver


class _Repo:
    def __init__(self) -> None:
        self._records: dict[str, dict] = {}

    def upsert(self, meta: dict) -> None:
        self._records[str(meta["id"])] = dict(meta)

    def get(self, indicator_id: str) -> dict | None:
        record = self._records.get(str(indicator_id))
        return dict(record) if record is not None else None

    def load(self) -> list[dict]:
        return [dict(record) for record in self._records.values()]

    def strategies_for_indicator(self, indicator_id: str) -> list[dict]:
        _ = indicator_id
        return []


def _ctx() -> IndicatorServiceContext:
    resolver = default_resolver()
    repository = _Repo()
    factory = IndicatorFactory(resolver=resolver)
    context = IndicatorServiceContext(
        repository=repository,
        resolver=resolver,
        factory=factory,
        overlay_cache=SimpleNamespace(purge_indicator=lambda _indicator_id: None),
        cache_owner="test",
        cache_scope_id="indicator_output_prefs_roundtrip",
    )
    factory._ctx = context
    return context


def test_indicator_update_round_trips_disabled_signal_outputs() -> None:
    ctx = _ctx()

    created = create_instance(
        "market_profile",
        "MP",
        {},
        output_prefs={"balance_breakout": {"enabled": False}},
        ctx=ctx,
    )

    created_outputs = {entry["name"]: entry for entry in created["typed_outputs"]}
    assert created["output_prefs"] == {"balance_breakout": {"enabled": False}}
    assert created_outputs["balance_breakout"]["enabled"] is False
    assert created_outputs["balance_retest"]["enabled"] is True

    updated = update_instance(
        created["id"],
        "market_profile",
        {},
        "MP",
        output_prefs={"balance_retest": {"enabled": False}},
        ctx=ctx,
    )

    updated_outputs = {entry["name"]: entry for entry in updated["typed_outputs"]}
    assert updated["output_prefs"] == {"balance_retest": {"enabled": False}}
    assert updated_outputs["balance_breakout"]["enabled"] is True
    assert updated_outputs["balance_retest"]["enabled"] is False


def test_factory_build_meta_from_record_preserves_output_prefs() -> None:
    factory = IndicatorFactory()

    meta = factory.build_meta_from_record(
        {
            "id": "mp-1",
            "type": "market_profile",
            "name": "Market Profile",
            "params": {},
            "dependencies": [],
            "output_prefs": {"balance_breakout": {"enabled": False}},
            "enabled": True,
        }
    )

    typed_outputs = {entry["name"]: entry for entry in meta["typed_outputs"]}

    assert meta["output_prefs"] == {"balance_breakout": {"enabled": False}}
    assert typed_outputs["balance_breakout"]["enabled"] is False
    assert typed_outputs["balance_retest"]["enabled"] is True
