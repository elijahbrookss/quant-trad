"""Normalization admission preserves evidence; it does not rerun the formula."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
import json

import pytest

from market_data.canonical_adapters import canonicalize_market_trade, canonicalize_normalized_feature, canonicalize_response_feature
from market_data.canonical_storage import legacy_material_alias
from market_data.normalization import NormalizationInput, NormalizationSpec, evaluate_normalization
from market_data.structure import MarketSide
from portal.backend.service.storage.repos import fact_normalization_admission as admission
from portal.backend.service.storage.repos import fact_book_admission, fact_derived_admission, market_data
from tests.test_market_data.test_fact_response_admission import _row
from tests.test_market_data.test_market_state_phase3 import BASE, SOURCE, _trade


def _fixture():
    spec = NormalizationSpec(feature_name="retention_zscore", semantic_version="1",
        input_fact_type="market.trade", output_fact_type="market.normalized.retention_zscore",
        formula="causal_zscore", units="zscore", window_seconds=10,
        minimum_observations=1, warmup_observations=1, parameters={"require_full_window": False})
    trades = [_trade(str(index), offset=str(index), side=MarketSide.BUY, price=str(100 + index),
                     receive_ordinal=index) for index in range(1, 6)]
    rows = [_row(canonicalize_market_trade(trade, source=SOURCE), 20, index)
            for index, trade in enumerate(trades, 1)]
    inputs = [NormalizationInput(source_series_id=20, effective_at=row["observation_time"],
        known_at=row["known_at"], market_commit_seq=row["market_commit_seq"], material_hash=row["material_hash"],
        value=trade.quote_notional) for row, trade in zip(rows, trades)]
    fact = evaluate_normalization(spec, inputs, output_series_id=60)[-1]
    assert fact.input_count == 5 and len(fact.source_material_hashes) <= 3
    return spec, fact, rows


def _lookups(monkeypatch, specs, sources, *, damage=None):
    def execute(statement, params):
        if "normalization_specs" in str(statement):
            found = [{**spec.material(), "id": spec.spec_id, "spec_hash": spec.spec_hash}
                     for spec in specs if spec.spec_id in params["ids"]]
            if damage == "missing_spec":
                found = []
            elif damage == "corrupt_spec":
                found[0]["units"] = "wrong"
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: found))
        assert "source.instrument_id=root.instrument_id" in str(statement)
        assert "root.contract_version=requested.contract_version" in str(statement)
        assert "source.timeframe_seconds=requested.required_timeframe" in str(statement)
        return SimpleNamespace(all=lambda: [] if damage == "scope" else
            [(row["root_id"], "instrument") for row in json.loads(params["requests"])])
    def material(session, **kwargs):
        selected = {}
        for request in kwargs["requests"]:
            selected[(request["root_id"], request["role"])] = [row["id"] for row in sources
                if row["series_id"] == request["series_id"] and request["material_hash"] in
                {row["material_hash"], (legacy_material_alias(row) or {}).get("material_hash")}]
            assert selected[(request["root_id"], request["role"])]
        return {row["id"]: row for row in sources}, selected
    def windows(session, **kwargs):
        selected, found = {}, {}
        for request in kwargs["requests"]:
            assert request["include_end"] is True and request["source_id"] is None
            matches = [row for row in sources if row["series_id"] == request["series_id"]
                and row["market_commit_seq"] <= request["root_commit"]
                and row["known_at"] <= request["known_at"]
                and request["range_start"] <= row["observation_time"] <= request["range_end"]]
            if damage == "incomplete":
                matches = matches[:3]
            selected[request["root_id"]] = [row["id"] for row in matches]
            found.update((row["id"], row) for row in matches)
        return found, selected
    monkeypatch.setattr(admission, "resolve_material_source_revisions", material)
    monkeypatch.setattr(admission, "resolve_causal_window_revisions", windows)
    monkeypatch.setattr(fact_derived_admission, "resolve_derived_source_revisions", lambda *args, **kwargs: [])
    monkeypatch.setattr(fact_book_admission, "resolve_book_source_revisions", lambda *args, **kwargs: [])
    return SimpleNamespace(execute=execute)


@pytest.mark.parametrize("damage", [None, "missing_spec", "corrupt_spec", "scope", "clock", "incomplete", "budget", "bytes", "witness"])
def test_normalization_requires_full_window_spec_and_exact_witnesses(monkeypatch, damage):
    spec, fact, sources = _fixture()
    if damage == "clock":
        fact = replace(fact, input_watermark=10)
    elif damage == "witness":
        # A large enough window still cannot omit its explicitly named first input.
        fact = replace(fact, input_start=BASE + timedelta(seconds=2), input_count=4)
    root = _row(canonicalize_normalized_feature(fact, spec=spec), 60, 10)
    session = _lookups(monkeypatch, [spec], sources, damage=damage)
    kwargs = dict(rows=[root], object_store=None, max_rows=4 if damage == "budget" else 20,
                  max_logical_bytes=1 if damage == "bytes" else 1024**2)
    if damage is not None:
        with pytest.raises(RuntimeError, match="normalization_"):
            admission.resolve_normalized_source_revisions(session, **kwargs)
    else:
        assert admission.resolve_normalized_source_revisions(session, **kwargs) == sorted(sources, key=lambda row: row["id"])


def test_nested_normalization_preserves_inner_window_and_legacy_material_alias(monkeypatch):
    spec, fact, sources = _fixture()
    inner = _row(canonicalize_normalized_feature(fact, spec=spec), 60, 10)
    assert legacy_material_alias(inner)["material_hash"] == fact.material_hash != inner["material_hash"]
    outer_spec = NormalizationSpec(feature_name="retention_nested", semantic_version="1",
        input_fact_type=spec.output_fact_type, output_fact_type="market.normalized.retention_nested",
        formula="basis_points", units="bps", window_seconds=None, minimum_observations=0, warmup_observations=0)
    outer_fact = evaluate_normalization(outer_spec, [NormalizationInput(source_series_id=60,
        effective_at=fact.effective_at, known_at=fact.known_at, market_commit_seq=10,
        material_hash=fact.material_hash, value=fact.value)], output_series_id=70)[0]
    root = _row(canonicalize_normalized_feature(outer_fact, spec=outer_spec), 70, 20)
    session = _lookups(monkeypatch, [spec, outer_spec], [*sources, inner])
    result = admission.resolve_normalized_source_revisions(session, rows=[root], object_store=None,
        max_rows=20, max_logical_bytes=1024**2)
    assert {row["id"] for row in result} == {row["id"] for row in [*sources, inner]}
    with pytest.raises(RuntimeError, match="edge_budget"):
        admission.resolve_normalized_source_revisions(session, rows=[root], object_store=None,
            max_rows=5, max_logical_bytes=1024**2)
    def size(row):
        return len(json.dumps(row, default=str, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    # Each individual level fits, but retaining both levels exceeds the cap.
    per_level_bytes = max(size(inner), sum(size(row) for row in sources))
    with pytest.raises(RuntimeError, match="source_budget"):
        admission.resolve_normalized_source_revisions(session, rows=[root], object_store=None,
            max_rows=20, max_logical_bytes=per_level_bytes)


def test_normalization_first_freeze_preserves_history_before_dynamic_schema_loading(monkeypatch):
    def not_loaded(_):
        raise ValueError("dynamic schema has not been loaded yet")
    monkeypatch.setattr(market_data, "get_fact_payload_schema", not_loaded)
    assert market_data._preserves_canonical_revision_history("market.normalized_feature.v1/nsp_" + "a" * 31)
    assert not market_data._preserves_canonical_revision_history("market.normalized_feature.v1/nsp_wrong")


def test_normalization_budget_deadline_interrupts_before_reading(monkeypatch):
    spec, fact, sources = _fixture()
    root = _row(canonicalize_normalized_feature(fact, spec=spec), 60, 10)
    def expired():
        raise RuntimeError("deadline_reached")
    with pytest.raises(RuntimeError, match="deadline_reached"):
        admission.resolve_normalized_source_revisions(None, rows=[root], object_store=None,
            max_rows=20, max_logical_bytes=1024**2, check_budget=expired)


def test_normalized_response_keeps_post_book_canonical_time(monkeypatch):
    from tests.test_market_data.test_fact_response_admission import _fixture as response_fixture
    response, _, _, _ = response_fixture()
    source = _row(canonicalize_response_feature(response), 60, 10)
    spec = NormalizationSpec(feature_name="retention_response", semantic_version="1",
        input_fact_type="market.market_response", output_fact_type="market.normalized.retention_response",
        formula="basis_points", units="bps", window_seconds=None, minimum_observations=0, warmup_observations=0)
    fact = evaluate_normalization(spec, [NormalizationInput(source_series_id=60,
        effective_at=response.bucket_start, known_at=response.known_at, market_commit_seq=10,
        material_hash=response.material_hash, value=response.response_bps)], output_series_id=70)[0]
    assert fact.input_end < source["observation_time"] <= fact.known_at
    root = _row(canonicalize_normalized_feature(fact, spec=spec), 70, 20)
    session = _lookups(monkeypatch, [spec], [source])
    found = admission.resolve_normalized_source_revisions(session, rows=[root], object_store=None,
        max_rows=20, max_logical_bytes=1024**2)
    assert found == [source]


def test_hot_self_contained_normalization_does_not_open_cold_store(monkeypatch):
    source = {"id": "funding", "fact_type": "derivatives.funding_rate"}
    monkeypatch.setattr(admission, "resolve_normalized_source_revisions",
                        lambda *args, **kwargs: [source])
    monkeypatch.setattr(
        "portal.backend.service.storage.repos.fact_dependencies.collect_source_history_archive_refs",
        lambda session, *, rows, object_store: {} if rows == [source] else pytest.fail("wrong rows"),
    )
    def cold_store():
        raise AssertionError("hot-only closure must not initialize cold storage")
    assert admission.collect_normalized_history_archive_refs_deferred(
        None, rows=[], object_store_factory=cold_store
    ) == {}
