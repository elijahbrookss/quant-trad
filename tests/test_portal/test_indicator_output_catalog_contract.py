from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pandas", reason="pandas required for indicator output catalog tests")

from portal.backend.service.indicators.indicator_factory import IndicatorFactory
from portal.backend.service.indicators.indicator_service.api import create_instance, update_instance
from portal.backend.service.indicators.indicator_service.context import IndicatorServiceContext
from portal.backend.service.providers.data_provider_resolver import default_resolver


class _Repo:
    def __init__(self) -> None:
        self._records: dict[str, dict] = {}
        self._strategies: list[dict] = []

    def upsert(self, meta: dict) -> None:
        self._records[str(meta["id"])] = dict(meta)

    def get(self, indicator_id: str) -> dict | None:
        record = self._records.get(str(indicator_id))
        return dict(record) if record is not None else None

    def load(self) -> list[dict]:
        return [dict(record) for record in self._records.values()]

    def strategies_for_indicator(self, indicator_id: str) -> list[dict]:
        _ = indicator_id
        return list(self._strategies)


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
        cache_scope_id="indicator_output_catalog_contract",
    )
    factory._ctx = context
    return context


def test_indicator_create_update_exposes_all_signal_outputs_without_prefs() -> None:
    ctx = _ctx()

    created = create_instance("market_profile", "MP", {}, ctx=ctx)

    created_outputs = {entry["name"]: entry for entry in created["typed_outputs"]}
    assert "output_prefs" not in created
    assert "enabled" not in created_outputs["balance_breakout"]
    assert "enabled" not in created_outputs["balance_reclaim"]
    assert "enabled" not in created_outputs["balance_retest"]

    updated = update_instance(created["id"], "market_profile", {}, "MP", ctx=ctx)

    updated_outputs = {entry["name"]: entry for entry in updated["typed_outputs"]}
    assert "output_prefs" not in updated
    assert {"balance_breakout", "balance_reclaim", "balance_retest"}.issubset(updated_outputs)
    assert all("enabled" not in updated_outputs[name] for name in ("balance_breakout", "balance_reclaim", "balance_retest"))


def test_factory_build_meta_from_record_keeps_outputs_catalog_only() -> None:
    factory = IndicatorFactory()

    meta = factory.build_meta_from_record(
        {
            "id": "mp-1",
            "type": "market_profile",
            "name": "Market Profile",
            "params": {},
            "dependencies": [],
            "enabled": True,
        }
    )

    typed_outputs = {entry["name"]: entry for entry in meta["typed_outputs"]}

    assert "output_prefs" not in meta
    assert "enabled" not in typed_outputs["balance_breakout"]
    assert "enabled" not in typed_outputs["balance_reclaim"]
    assert "enabled" not in typed_outputs["balance_retest"]


def test_indicator_update_rejects_material_edit_when_strategy_bound() -> None:
    ctx = _ctx()

    created = create_instance("candle_stats", "Stats", {"warmup_bars": 200}, ctx=ctx)
    ctx.repository._strategies = [{"strategy_id": "strategy-1", "indicator_id": created["id"]}]

    with pytest.raises(ValueError, match="strategy-bound indicator"):
        update_instance(created["id"], "candle_stats", {"warmup_bars": 80}, "Stats", ctx=ctx)

    renamed = update_instance(created["id"], "candle_stats", {"warmup_bars": 200}, "Stats renamed", ctx=ctx)
    assert renamed["name"] == "Stats renamed"
