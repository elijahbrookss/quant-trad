from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from scripts.db import canonical_cold_research_replay as replay
from tests.test_market_data.test_fact_storage_tiers_db import BASE, _placement, storage  # noqa: F401


@pytest.mark.db
def test_frozen_native_check_preserves_ordered_results_and_causal_inputs_after_cooling(storage, tmp_path, monkeypatch):
    from market_data.archive import FilesystemRawArchiveObjectStore
    from market_data.cold_history import ColdHistoryReader
    from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact
    from market_data.fact_archive import publish_canonical_fact_archive
    from portal.backend import db as db_module
    from portal.backend.db import session as db_session
    from portal.backend.db.fact_identity_schema import fact_header_partition_name
    from portal.backend.db.fact_storage_schema import fact_partition_name
    from portal.backend.service import provenance
    from portal.backend.service.market import runtime_market_data
    from portal.backend.service.research import execution, service
    from portal.backend.service.research.raw_candle_evaluator import OUTCOME_SCHEMA
    from portal.backend.service.storage.repos import candles, instruments
    from portal.backend.service.storage.repos.cold_history import PostgresColdHistory
    from portal.backend.service.storage.repos.fact_storage import CANONICAL_ROW_COLUMNS, CANONICAL_ROW_FROM
    from research_science.check import CheckResult

    monkeypatch.setattr(db_module, "db", storage.database)
    monkeypatch.setattr(db_session, "db", storage.database)
    monkeypatch.setattr(candles, "db", storage.database)
    monkeypatch.setattr(instruments, "db", storage.database)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(tmp_path))
    archive = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    series = storage.repo.register_series(instrument_id="storage-fixture", fact_type=CANDLE_FACT_TYPE,
                                          timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION)
    start = BASE.replace(second=0, microsecond=0)
    old_day = storage.today - timedelta(days=1)
    bars = []
    for index in range(18):
        if index == 12:
            continue
        opened, closed = (start + timedelta(minutes=minute) for minute in (index, index + 1))
        price, opening = 100 + index, 100 + index
        if index in (0, 7):
            opening += 1
        elif index in (1, 8):
            opening -= 1
            price += 1
        volume = 20 if index == 1 else 0 if index in (7, 8) else 10
        bars.append(CandleFact(open_time=opened, close_time=closed, open=opening, close=price,
            high=max(opening, price)+1, low=min(opening, price)-1, volume=volume,
            accepted_at=start+timedelta(days=1), known_at=closed, known_at_method="interval_close_inferred"))
    _placement(monkeypatch, old_day)
    storage.repo.ingest_candles(series_id=series, source_id=storage.source_id, facts=bars[:8])
    _placement(monkeypatch, storage.today)
    storage.repo.ingest_candles(series_id=series, source_id=storage.source_id, facts=bars[8:])
    storage.repo.record_gap_evidence(series_id=series, source_id=storage.source_id,
        start=start+timedelta(minutes=12), end=start+timedelta(minutes=13), classification="provider_missing_data",
        expected_count=1, observed_count=0, evidence={"schema_version": "market_gap_evidence.v1", "reason_code": "fixture_gap"})

    class NoProvider:
        def __init__(self, **_kwargs):
            pass

        def __getattr__(self, name):
            raise AssertionError(f"provider accessed in frozen Check: {name}")

    monkeypatch.setattr(runtime_market_data, "MarketDataCollectorService", NoProvider)
    request = {"check_family": "raw_forward_outcome", "scope": {"instrument_id": "storage-fixture", "timeframe": "1m",
        "start": (start + timedelta(minutes=1)).isoformat(), "end": (start + timedelta(minutes=18)).isoformat()},
        "detector": {"all": [{"field": "previous_close", "operator": "lt", "value_field": "previous_open"},
            {"field": "close", "operator": "gt", "value_field": "open"},
            {"field": "open", "operator": "lte", "value_field": "previous_close"},
            {"field": "close", "operator": "gte", "value_field": "previous_open"},
            {"field": "volume", "operator": "gt", "value_field": "previous_volume"}]},
        "outcomes": {"schema_version": OUTCOME_SCHEMA, "forward_bars": [5], "entry_lag_bars": 1, "min_sample_count": 1},
        "gap_policy": "continue_degraded", "preparation": {"freeze": True, "name": "review-frozen-ordered-output"}}
    prepared = service.prepare_research_check_evidence(request)
    assert prepared["status"] == "frozen", prepared
    check_request = prepared["next_request"]
    dataset = storage.repo.get_dataset(check_request["dataset_id"])
    # An after-freeze, after-close correction must not affect the prior Check.
    storage.repo.ingest_candles(series_id=series, source_id=storage.source_id, facts=[replace(bars[2], close=102.5,
        accepted_at=start+timedelta(days=2), known_at=start+timedelta(days=2), known_at_method="platform_acceptance")])
    lower, upper = dataset.series[0]["range_start"], dataset.series[0]["range_end"]
    history = storage.repo.read_fact_revisions(series_id=series, start=lower, end=upper, as_of_commit_seq=dataset.max_commit_seq)
    commit = storage.repo.current_commit_seq()
    plan = {"read_boundary": {"restore_pair_hash": "a" * 64, "commit_seq": commit, "retire_before": storage.today.isoformat(),
        "repeats": 5, "timeout_seconds": 15, "max_rows": 100, "max_archive_bytes": 2**20,
        "cases": [{"name": "frozen-candle-revisions", "mode": "all_revisions", "series_id": series,
                   "start": lower.isoformat(), "end": upper.isoformat(), "known_at_lte": (start+timedelta(days=1)).isoformat(),
                   "source_identity_keys": [], "as_of_commit_seq": dataset.max_commit_seq,
                   "storage_witnesses": {history[1].fact_version_id: old_day.isoformat(),
                                         history[-1].fact_version_id: storage.today.isoformat()}}]},
        "checks": [{"name": "ordered-raw-forward", "dataset_hash": dataset.dataset_hash,
                    "witness": "frozen-candle-revisions", "request": check_request}]}
    revision = "b" * 40
    monkeypatch.setattr(execution, "evidence_source_revision", lambda: revision)
    monkeypatch.setattr(provenance, "evidence_source_revision", lambda: revision)
    engine = storage.database._engine

    def fixture_evidence(read_engine):
        with read_engine.connect() as conn:
            assert conn.scalar(text("SHOW transaction_read_only")) == "on"
            layout, cutoff = conn.execute(text("SELECT layout_version,evidence->>'cold_before_day' FROM market.fact_storage_state")).one()
        return {"system_identifier": "disposable", "oid": 7, "layout": layout, "cold_before_day": cutoff}

    monkeypatch.setattr(replay.reads, "_db_url", lambda _dsn: engine.url)
    monkeypatch.setattr(replay.reads, "_database_evidence", fixture_evidence)

    def trial(side):
        # No change to the regular bootstrap owner. The operator entry binds
        # this deliberately blank app wrapper to its enforced read-only pool.
        guard = db_session.Database()
        monkeypatch.setattr(db_session, "db", guard)
        from portal.backend.service.storage.repos import market_data
        monkeypatch.setattr(market_data, "db", guard)
        monkeypatch.setattr(candles, "db", guard)
        monkeypatch.setattr(instruments, "db", guard)
        return replay.run(SimpleNamespace(side=side, revision=revision, runtime_digest="sha256:" + "c" * 64,
            system_identifier="disposable", database_oid=7, archive_root=tmp_path), plan)

    left = trial("source")
    with engine.begin() as conn:
        rows = [dict(row) for row in conn.execute(text(f"SELECT {CANONICAL_ROW_COLUMNS} {CANONICAL_ROW_FROM} "
            "WHERE versions.storage_day=:day ORDER BY versions.market_commit_seq,versions.id"), {"day": old_day}).mappings()]
        for row in rows:
            row.pop("storage_day")
        page = publish_canonical_fact_archive(rows, object_store=archive, temporary_directory=tmp_path)
        PostgresColdHistory(ColdHistoryReader(archive)).admit(conn, page)
        conn.exec_driver_sql(f"DROP TABLE market.{fact_header_partition_name(old_day)}")
        conn.exec_driver_sql(f"DROP TABLE market.{fact_partition_name(old_day)}")
        conn.execute(text("""INSERT INTO market.cold_fact_retired_days
            (storage_day,manifest_set_hash,row_count,reclaimed_header_bytes,protected_dataset_ranges,handoff_kind)
            VALUES (:day,:digest,:count,0,0,'initial')"""), {"day": old_day, "digest": page.manifest_hash, "count": len(rows)})
        conn.execute(text("""UPDATE market.fact_storage_state SET layout_version='market.canonical_cold_history.v1',
            evidence=jsonb_build_object('cold_before_day',CAST(:cutoff AS text))"""), {"cutoff": storage.today})
    revision = "d" * 40
    right = trial("candidate")
    result = replay.compare(left, right)["checks"]["ordered-raw-forward"]
    assert result["source_native_sha256"] != result["candidate_native_sha256"]
    output = right["checks"]["ordered-raw-forward"]["native"]["result"]["result"]
    assert output["complete_sample_count"] == 1 and len(output["events"]) == 1
    assert right["checks"]["ordered-raw-forward"]["execution_metrics"]["consumed"]["archive_source_bytes"] > 0
    changed = deepcopy(right)
    case = changed["checks"]["ordered-raw-forward"]
    native = case["native"]
    native["result"]["result"]["events"].clear()
    with pytest.raises(RuntimeError, match="receipt hashes differ"):
        replay.compare(left, changed)
    native["result"] = CheckResult.from_dict(native["result"] | {"result_hash": ""}).to_dict()
    case["native_sha256"] = replay.reads._digest(native)
    case["semantic_sha256"] = replay.reads._digest(replay._semantics(native,
        revision=changed["application_revision"], case=plan["checks"][0], boundary=plan["read_boundary"]))
    with pytest.raises(RuntimeError, match="paired ordered Check output"):
        replay.compare(left, changed)
