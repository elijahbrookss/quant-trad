from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace

import pytest

from market_data.canonical import CanonicalFactRecord
from market_data.canonical_adapters import canonicalize_market_trade, canonicalize_response_feature, canonicalize_trade_flow_feature
from market_data.canonical_storage import record_to_storage_row
from market_data.market_state import derive_response_features, derive_trade_flow_feature
from market_data.structure import MarketSide
from portal.backend.service.storage.repos import fact_response_admission as admission, fact_dependencies
from tests.test_market_data.test_market_state_phase3 import BASE, SOURCE, _aggregate, _contract, _state, _trade


def _row(fact, series_id, commit=10):
    return record_to_storage_row(CanonicalFactRecord(fact=fact, series_id=series_id, source_id=1,
        revision=1, market_commit_seq=commit), series_dimensions={})


def _fixture():
    trade = _trade("response-trade", offset="0.2", side=MarketSide.BUY, price="100.02", receive_ordinal=10)
    flow = derive_trade_flow_feature(series_id=30, source_trade_flow_series_id=31, aggregate=_aggregate((trade,)),
        trades=(trade,), computed_at=BASE + timedelta(seconds=1))
    states = [_state(1, "0.1", asks=(("100.02", "100"),)),
              _state(2, "0.6", asks=(("100.02", "40"),)),
              _state(3, "1.3", asks=(("100.02", "70"),))]
    response = derive_response_features(states, [trade], [flow], contract=_contract(), series_id=60,
        computed_at=BASE + timedelta(seconds=2))[0]
    return response, flow, trade, states


@pytest.mark.parametrize("damage", [None, "scope", "flow", "book_scope", "trade_endpoint", "budget"])
def test_response_keeps_named_book_states_full_windows_and_transitive_flow(monkeypatch, damage):
    response, flow, trade, states = _fixture()
    if damage == "book_scope":
        response = replace(response, post_book_source_position=replace(response.post_book_source_position, session_id="wrong"))
    elif damage == "trade_endpoint":
        response = replace(response, last_trade_id="missing")
    row = _row(canonicalize_response_feature(response), 60, 20)
    if damage == "flow":
        flow = replace(flow, source_trade_flow_series_id=99)
    flow_row = _row(canonicalize_trade_flow_feature(flow), 30)
    trade_row = _row(canonicalize_market_trade(trade, source=SOURCE), 20, 1)
    books = {f"book-{index}": {"id": f"book-{index}", "fact_type": "market.l2_book", "observation_time": state.effective_at}
             for index, state in enumerate(states)}
    # One non-endpoint book revision must be retained as well.
    intermediate = {"id": "intermediate", "fact_type": "market.l2_book", "observation_time": BASE + timedelta(seconds=0.4)}
    def material(session, **kwargs):
        assert kwargs["requests"][0]["material_hash"] == response.source_flow_material_hash
        return {flow_row["id"]: flow_row}, {(row["id"], "flow"): [flow_row["id"]]}
    monkeypatch.setattr(admission, "resolve_material_source_revisions", material)
    def positions(session, **kwargs):
        assert len(kwargs["requests"]) == 3
        return books, {f"{row['id']}:{role}": [f"book-{index}"] for index, role in enumerate(("pre", "trough", "post"))}
    monkeypatch.setattr(admission, "resolve_book_position_revisions", positions)
    def windows(session, **kwargs):
        requested = kwargs["requests"]
        assert requested[0]["series_id"] is None and requested[0]["source_id"] is None
        assert requested[0]["range_start"] == response.bucket_start and requested[0]["range_end"] == response.bucket_end
        assert requested[0]["include_end"] is False
        assert requested[1]["series_id"] == response.source_l2_series_id and requested[1]["include_end"] is True
        assert requested[1]["range_start"] == states[0].effective_at and requested[1]["range_end"] == states[-1].effective_at
        return {**books, "intermediate": intermediate, trade_row["id"]: trade_row}, {
            f"{row['id']}:trades": [trade_row["id"]], f"{row['id']}:books": [*books, "intermediate"]}
    monkeypatch.setattr(admission, "resolve_causal_window_revisions", windows)
    descendant = {"id": "aggregate", "fact_type": "market.trade_flow"}
    monkeypatch.setattr(admission, "resolve_flow_feature_source_revisions", lambda *args, **kwargs: [descendant, trade_row])
    original_decode = admission.record_from_storage_row
    monkeypatch.setattr(admission, "record_from_storage_row", lambda item:
        None if item["fact_type"] == "market.l2_book" else original_decode(item))
    def execute(statement, params):
        assert "flow.instrument_id=root.instrument_id" in str(statement) and "book.instrument_id=root.instrument_id" in str(statement)
        assert json.loads(params["requests"])[0]["book_series_id"] == 10
        return SimpleNamespace(all=lambda: [] if damage == "scope" else [(row["id"], "instrument")])
    kwargs = dict(rows=[row], object_store=None, max_rows=6 if damage == "budget" else 20, max_logical_bytes=1024**2)
    if damage is None:
        result = admission.resolve_response_source_revisions(SimpleNamespace(execute=execute), **kwargs)
        assert {item["id"] for item in result} == {flow_row["id"], trade_row["id"], *books, "intermediate", "aggregate"}
    else:
        with pytest.raises(RuntimeError, match="canonical_response_"):
            admission.resolve_response_source_revisions(SimpleNamespace(execute=execute), **kwargs)


@pytest.mark.parametrize("damage", [None, "budget", "missing", "clock", "commit", "series", "source", "family", "end"])
def test_canonical_window_retains_all_revisions_and_rechecks_bounds(monkeypatch, damage):
    _, _, trade, _ = _fixture()
    row = _row(canonicalize_market_trade(trade, source=SOURCE), 20, 1)
    request = dict(root_id="root", instrument_id="instrument", fact_type="market.trade", series_id=20, source_id=1,
        root_commit=2, known_at=BASE + timedelta(seconds=2), range_start=BASE, range_end=BASE + timedelta(seconds=1), include_end=False)
    changed = dict(row, id="second", revision=2)
    if damage == "clock":
        changed["known_at"] = request["known_at"] + timedelta(seconds=1)
    elif damage == "commit":
        changed["market_commit_seq"] = 3
    elif damage in {"series", "source"}:
        changed[damage + "_id"] = 99
    elif damage == "family":
        changed["fact_type"] = "market.l2_book"
    elif damage == "end":
        changed["observation_time"] = request["range_end"]
    sources = {item["id"]: item for item in (row, changed)}
    monkeypatch.setattr(fact_dependencies, "read_canonical_dependency_rows", lambda *args, **kwargs: {} if damage == "missing" else sources)
    def execute(statement, params):
        assert "source.market_commit_seq<=requested.root_commit" in str(statement)
        assert "source.known_at<=requested.known_at" in str(statement) and "source.state=" not in str(statement)
        return SimpleNamespace(all=lambda: [("root", identity) for identity in sources])
    kwargs = dict(requests=[request], reader=None, max_rows=1 if damage == "budget" else 10, max_logical_bytes=1024**2)
    if damage is None:
        found, selected = fact_dependencies.resolve_causal_window_revisions(SimpleNamespace(execute=execute), **kwargs)
        assert found == sources and selected == {"root": list(sources)}
    else:
        with pytest.raises(RuntimeError, match="canonical_window_"):
            fact_dependencies.resolve_causal_window_revisions(SimpleNamespace(execute=execute), **kwargs)


def test_window_batches_and_edge_limit_include_repeated_sources(monkeypatch):
    _, _, trade, _ = _fixture()
    row = _row(canonicalize_market_trade(trade, source=SOURCE), 20, 1)
    # Closed book-style ranges may include their exact end; half-open trade
    # ranges must not. The shared SQL and post-hydration check agree.
    requests = [dict(root_id=str(index), instrument_id="instrument", fact_type="market.trade", series_id=None, source_id=None,
        root_commit=2, known_at=BASE + timedelta(seconds=2), range_start=BASE, range_end=row["observation_time"], include_end=True)
        for index in range(129)]
    batches = []
    def execute(statement, params):
        batch = json.loads(params["requests"])
        batches.append(len(batch))
        return SimpleNamespace(all=lambda: [(item["root_id"], row["id"]) for item in batch])
    monkeypatch.setattr(fact_dependencies, "read_canonical_dependency_rows", lambda *args, **kwargs: {row["id"]: row})
    found, selections = fact_dependencies.resolve_causal_window_revisions(SimpleNamespace(execute=execute), requests=requests,
        reader=None, max_rows=129, max_logical_bytes=1024**2)
    assert batches == [128, 1] and found == {row["id"]: row} and len(selections) == 129
    with pytest.raises(RuntimeError, match="request_budget"):
        fact_dependencies.resolve_causal_window_revisions(None, requests=requests, reader=None, max_rows=128, max_logical_bytes=1024**2)


@pytest.mark.parametrize("conflict", [False, True])
def test_response_history_binds_trade_and_book_windows_through_their_read_owners(monkeypatch, conflict):
    from portal.backend.service.storage.repos import fact_flow_admission, fact_book_prefix
    sources = [{"id": family, "fact_type": family} for family in
        ("market.l2_book", "market.trade", "market.trade_flow", "market.trade_flow_feature")]
    monkeypatch.setattr(admission, "resolve_response_source_revisions", lambda *args, **kwargs: sources)
    def trades(session, **kwargs):
        assert {row["fact_type"] for row in kwargs["rows"]} == {"market.trade", "market.trade_flow"}
        return {"trade": {"object_key": "trade.parquet"}}
    def books(session, **kwargs):
        assert [row["fact_type"] for row in kwargs["rows"]] == ["market.l2_book"]
        return {"trade" if conflict else "book": {"object_key": "book.parquet"}}
    monkeypatch.setattr(fact_flow_admission, "collect_trade_history_archive_refs", trades)
    monkeypatch.setattr(fact_book_prefix, "resolve_book_prefixes_for_read", books)
    if conflict:
        with pytest.raises(RuntimeError, match="dependency_conflict"):
            admission.collect_response_history_archive_refs(None, rows=[], object_store=None)
    else:
        assert set(admission.collect_response_history_archive_refs(None, rows=[], object_store=None)) == {"trade", "book"}
