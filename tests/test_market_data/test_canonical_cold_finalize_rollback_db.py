from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from market_data.archive_namespace import archive_namespace
from market_data.archive_verification import ArchiveVerificationLimits
from market_data.cold_history import ColdHistoryReader
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, CandleFact, DatasetSeriesRequest, SourceIdentity
from market_data.fact_archive import archive_evidence_hash
from portal.backend.db.cold_history_models import COLD_HISTORY_LAYOUT_VERSION
from portal.backend.db.fact_storage_schema import FACT_STORAGE_LAYOUT_VERSION, assert_fact_storage_contract
from portal.backend.db.session import Database
from portal.backend.service.storage import history_policy
from portal.backend.service.storage.repos import cold_history, market_data, market_structure
from portal.backend.service.storage.repos.cold_retirement import prune_oldest_retired_identities
from portal.backend.service.storage.repos.fact_archival import PostgresCanonicalFactArchiveRepository
from portal.backend.service.storage.repos.fact_reclamation import PostgresCanonicalFactReclamationRepository
from portal.backend.service.storage.repos.fact_storage import PostgresCanonicalFactStorageRepository
from scripts.db import canonical_cold_finalize_rollback as rollback_finalizer
from scripts.db.canonical_cold_custody_placement import assert_original_custody_members, inspect_custody_members
from scripts.db.canonical_cold_database_fence import close_database_fence, reopen_database_fence
from scripts.db.canonical_cold_finalize_rollback import finalize_rollback_layout
from scripts.db.canonical_cold_raw_index import RECENT_TABLESPACE_OID, inspect_raw_mapping_index_member
from scripts.db.canonical_cold_restore_initial_dated import restore_initial_dated_day
from scripts.db.canonical_cold_restore_legacy import restore_legacy_headers
from scripts.db.canonical_cold_restore_legacy_payload import capture_original_payload_day, restore_legacy_payload_day
from scripts.db.canonical_cold_restore_reclaimed_day import restore_reclaimed_header_day
from scripts.db.canonical_cold_restore_references import capture_original_reference_bindings
from scripts.db.canonical_cold_reverse_allocator import capture_allocator
from scripts.db.canonical_cold_reverse_projection import restore_candidate_only_pages
from scripts.db.canonical_cold_rollback_stream import capture_rollback_stream, verify_rollback_stream
from scripts.db.canonical_cold_witness import measure_witnesses
from tests.test_market_data.exact_deployed_reader import check_deployed_source_reader
from tests.test_market_data.test_canonical_cold_activate_db import _fence_connections
from tests.test_market_data.test_canonical_cold_restore_initial_dated_db import initially_dated_rollback_clone  # noqa: F401
from tests.test_market_data.test_canonical_cold_restore_legacy_db import legacy_rollback_clone, storage  # noqa: F401
from tests.test_market_data.test_canonical_recovery_spool_db import DEFINITION, _admitted_rows, _define_trade_stream, _native_recover, _seal_trade
from tests.test_market_data.test_fact_derived_retention_db import _archive_day
from tests.test_market_data.test_fact_storage_tiers_db import _ingest, _placement


pytestmark = pytest.mark.db


def _later_intake(scope, root, monkeypatch):
    store = scope.candidate_store
    # Both native readers use the mounted fixture's same immutable objects.
    reader = PostgresCanonicalFactStorageRepository(object_store_factory=lambda: store, read_cache_factory=lambda: None)
    monkeypatch.setattr(market_data, "canonical_fact_storage_repository", reader)
    monkeypatch.setattr(market_structure, "canonical_fact_storage_repository", reader)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(root))
    monkeypatch.setattr(cold_history, "require_cold_acquisition_headroom", lambda session: None)
    monkeypatch.setattr(history_policy, "_read", lambda session: (SimpleNamespace(movement_enabled=True), None))
    monkeypatch.setattr(history_policy, "_archive_policy", lambda session, saved, defaults, directory: defaults)
    with scope.storage.database._engine.begin() as conn:
        conn.execute(text("""INSERT INTO public.portal_storage_policy(id,revision,policy,updated_at)
            VALUES (1,1,jsonb_build_object('recent_days',14),clock_timestamp())"""))
    repo = scope.storage.repo
    product = repo.register_source(SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT",
                                                   source_kind="historical_api", adapter_version="rollback.integration.v1"))
    series = repo.register_series(instrument_id="storage-fixture", fact_type=CANDLE_FACT_TYPE,
                                 timeframe_seconds=60, contract_version=CANDLE_FACT_VERSION)
    start = datetime.combine(scope.storage.today - timedelta(days=90), time(), tzinfo=UTC)
    accepted = datetime.combine(scope.storage.today, time(hour=1), tzinfo=UTC)
    candle = CandleFact(open_time=start, close_time=start + timedelta(minutes=1), open=100, high=105, low=95,
                        close=102, volume=1, received_at=accepted, accepted_at=accepted, known_at=accepted,
                        known_at_method="platform_acceptance")
    result = repo.ingest_candles(series_id=series, source_id=product, facts=[candle], ingestion_run_id="rollback-later-candle")
    assert result.inserted_count == 1
    selection = {"series_id": series, "start": start, "end": start + timedelta(minutes=2)}
    frozen = repo.freeze_dataset([DatasetSeriesRequest(**selection)])
    candles = repo.read_facts(**selection)
    frozen_candles = repo.read_dataset_series(dataset_id=frozen.dataset_id, series_id=series)
    assert len(candles) == len(frozen_candles) == 1
    structures, claim, trade_series = _define_trade_stream(scope.storage, monkeypatch)
    segment, event = _seal_trade(root, claim, trade_id="rollback-after-full-cutover")
    _native_recover(structures, root)
    assert segment.ack_path.exists() and not segment.sealed_path.exists()
    trades = _admitted_rows(scope.storage.database, trade_series)
    assert len(trades[0]) == len(trades[1]) == 1 and trades[1][0].raw_record_id == event.raw_record_id
    window = {"series_id": trade_series, "start": event.received_at - timedelta(seconds=1),
              "end": event.received_at + timedelta(seconds=30)}
    trade_frozen = repo.freeze_dataset([DatasetSeriesRequest(**window)])
    trade = repo.read_dataset_fact_revisions(dataset_id=trade_frozen.dataset_id, series_id=trade_series)
    assert len(trade) == 1
    return SimpleNamespace(frozen=frozen, candles=candles, frozen_candles=frozen_candles,
                           selection=selection, candle=candle, product=product,
                           trade_frozen=trade_frozen, window=window, trade=trade, trades=trades)


def _later_retirement(scope, root, monkeypatch):
    day = scope.end_day
    _placement(monkeypatch, day)
    # A distinct observation key, received after cutover and retired under the
    # normal candidate owner before the latest recovery boundary.
    fact = replace(scope.storage.fact, observation_key="later-retired-funding",
                   observation_time=scope.storage.fact.observation_time - timedelta(days=1))
    _ingest(scope.storage, fact)
    archive = PostgresCanonicalFactArchiveRepository(database=scope.storage.database,
        object_store=scope.candidate_store, temporary_directory=root / "recurring-stage")
    _archive_day(archive, day)
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=archive, enabled=True)
    options = {"eligible_before": scope.storage.today, "execute": True}
    assert reclaimer.reclaim_partition(day, **options)["status"] == "cold_page_admitted"
    retired = reclaimer.reclaim_partition(day, **options)
    assert retired["status"] == "partition_reclaimed"
    assert prune_oldest_retired_identities(scope.storage.database, deadline=lambda: None)["day_complete"]
    return day, retired["manifest_set_hash"], archive.limits, fact


@pytest.mark.parametrize("rollback_clone", ["legacy_rollback_clone", "initially_dated_rollback_clone"])
def test_reverse_layout_keeps_post_cutover_cold_candle_and_native_trade(rollback_clone, request, monkeypatch):
    _exercise_reverse_layout(request.getfixturevalue(rollback_clone), monkeypatch)


@pytest.mark.parametrize("legacy_rollback_clone", ["verified-native-raw-index"], indirect=True)
def test_reverse_layout_inverts_native_raw_index_after_new_writes(legacy_rollback_clone, monkeypatch):
    _exercise_reverse_layout(legacy_rollback_clone, monkeypatch, raw_index_move=legacy_rollback_clone.certificate["raw_mapping_index"])


def _exercise_reverse_layout(scope, monkeypatch, *, raw_index_move=None):
    legacy_days = getattr(scope, "legacy_days", ())
    store = scope.candidate_store
    root = store.root.parent
    later = _later_intake(scope, root, monkeypatch)
    with monkeypatch.context() as patch:
        recurring_day, recurring_hash, page_limits, recurring_fact = _later_retirement(scope, root, patch)
    raw_key = later.trades[1][0].object_key
    raw = store.local_path(raw_key).read_bytes()
    target_identity = scope.target_identity
    latest = scope.storage.repo.current_commit_seq()
    with scope.storage.database._engine.begin() as conn:
        assert conn.scalar(text("SELECT count(*) FROM market.fact_versions WHERE series_id=:series"),
                           {"series": later.selection["series_id"]}) == 0
        gap = conn.scalar(text("SELECT nextval('market.fact_commit_seq')"))
        assert gap == latest + 1
    legacy_range = {key: value.isoformat() if isinstance(value, datetime) else value for key, value in scope.selection.items()}
    cases = [
        {"name": "exact", "mode": "exact", "max_rows": 4,
         "ids": [scope.expected_frozen[0].fact_version_id, scope.expected_latest[0].fact_version_id,
                 later.candles[0].fact_version_id, later.trade[0].fact_version_id]},
        {"name": "frozen", "mode": "frozen_revisions", **legacy_range, "max_rows": 4, "known_at_lte": None,
         "dataset_id": scope.frozen.dataset_id, "dataset_hash": scope.frozen.dataset_hash},
        {"name": "as of", "mode": "as_of", **legacy_range, "max_rows": 4, "known_at_lte": None,
         "as_of_commit_seq": scope.frozen.max_commit_seq},
    ]
    with _fence_connections(scope.storage) as (conn, control), scope.donor.snapshot() as donor:
        fence = close_database_fence(conn, control, expected_database=target_identity, timeout_seconds=90, record=lambda *_: None)
        original = capture_original_reference_bindings(donor)
        donor_payloads = [capture_original_payload_day(
            donor, donor_identity=scope.donor_identity, day=day, expected_header_oid=scope.donor_scope["physical"]["oid"],
            expected_source_commit_seq=scope.source_commit, max_rows=20, max_bytes=2 * 1024**2,
        ) for day in legacy_days]
        with conn.begin():
            allocator = capture_allocator(conn, fence=fence, layout=COLD_HISTORY_LAYOUT_VERSION)
            assert allocator.visible_commit == latest and allocator.next_value == gap + 1
            witness = measure_witnesses(conn, cases, store=store, timeout_seconds=15, max_archive_bytes=2 * 1024**2)
            stream_args = {"fence": fence, "store": store, "max_rows": 128,
                           "object_limits": ArchiveVerificationLimits(max_objects=8, max_bytes=2 * 1024**2)}
            with archive_namespace(store.root, exclusive=True):
                stream = capture_rollback_stream(conn, definition_id=DEFINITION, **stream_args)
        with conn.begin():
            direct = restore_candidate_only_pages(
                conn, fence=fence, expected_database=target_identity, expected_execution_hash=scope.certificate["execution_hash"],
                expected_source_commit=latest, reader=ColdHistoryReader(store), max_pages=8, max_bytes=2 * 1024**2,
            )
            assert direct["page_count"] == direct["row_count"] == 1
            archive_args = dict(fence=fence, audit=scope.audit, forward_receipts=scope.forward_receipts, store=store,
                                donor_store=scope.donor_store, max_copy_bytes=2 * 1024**2, max_seconds=30)
            legacy = restore_legacy_headers(
                conn, donor, donor_scope=scope.donor_scope, expected_donor_hash=scope.donor_scope["scope_sha256"], **archive_args,
            ) if legacy_days else None
            dated = [] if legacy_days else [restore_initial_dated_day(
                conn, donor, original=scope.donor_scope, expected_original_hash=scope.donor_scope["receipt_sha256"], **archive_args,
            )]
            payloads = [restore_legacy_payload_day(
                conn, donor, fence=fence, original=payload, expected_original_hash=payload["receipt_sha256"],
                legacy_header_receipt=legacy, expected_legacy_header_receipt_hash=legacy["receipt_sha256"],
                max_rows=20, max_bytes=2 * 1024**2, store=store, object_limits=ArchiveVerificationLimits(max_pages=8),
            ) for payload in donor_payloads]
            recurring = restore_reclaimed_header_day(
                conn, fence=fence, day=recurring_day, expected_manifest_set_hash=recurring_hash, store=store,
                limits=page_limits, object_limits=ArchiveVerificationLimits(max_pages=8),
            )
            assert ((payloads or dated)[0]["payload_oid"] is None) == (scope.state == "reclaimed")
            args = {"fence": fence, "allocator": allocator, "original": original, "original_hash": archive_evidence_hash(original),
                    "legacy": legacy, "payloads": payloads, "dated": dated, "recurring": [recurring],
                    "direct_cold": direct, "witness": witness,
                    "witness_hash": archive_evidence_hash(witness), "store": store, "timeout_seconds": 15}
            missing_scope = {"payloads": [], "dated": []}
            with pytest.raises(RuntimeError, match="(payload|dated)_scope_changed"):
                finalize_rollback_layout(conn, donor, **{**args, **missing_scope})
            with pytest.raises(RuntimeError, match="recurring_scope_changed"):
                finalize_rollback_layout(conn, donor, **{**args, "recurring": []})
            omitted = {key: value for key, value in direct.items() if key not in {"receipt_sha256", "inserted"}}
            omitted.update(page_count=0, row_count=0, archive_bytes=0)
            omitted["receipt_sha256"] = archive_evidence_hash(omitted)
            with pytest.raises(RuntimeError, match="candidate page budget exceeded"):
                finalize_rollback_layout(conn, donor, **{**args, "direct_cold": omitted})
            assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == COLD_HISTORY_LAYOUT_VERSION
            if raw_index_move is not None:
                custody_forward = scope.certificate["custody_move"]
                custody_original = inspect_custody_members(donor)
                assert len({member["tablespace_oid"] for member in custody_original}) == 2
                custody_before = inspect_custody_members(conn)
                assert {member["tablespace_oid"] for member in custody_before} == {custody_forward["target"]["tablespace_oid"]}
                with pytest.raises(ValueError, match="custody_copy_budget_required"):
                    finalize_rollback_layout(conn, donor, **args)
                with pytest.raises(RuntimeError, match="custody_restore_copy_budget_exceeded"):
                    finalize_rollback_layout(conn, donor, **args, custody_max_copy_bytes=0)
                with pytest.raises(ValueError, match="custody_restore_forward_receipt_invalid"), conn.begin_nested():
                    conn.exec_driver_sql("""UPDATE market.fact_storage_state SET evidence=jsonb_set(evidence,
                        '{custody_move,receipt_hash}', to_jsonb(repeat('0', 64)))""")
                    finalize_rollback_layout(conn, donor, **args, custody_max_copy_bytes=2 * 1024**2)
                with pytest.raises(RuntimeError, match="custody_original_placement_changed"), conn.begin_nested():
                    conn.exec_driver_sql("UPDATE market.fact_storage_state SET evidence=evidence - 'custody_move'")
                    finalize_rollback_layout(conn, donor, **args)
                args["custody_max_copy_bytes"] = 2 * 1024**2
                raw_before = inspect_raw_mapping_index_member(conn)
                assert raw_before["tablespace_oid"] == raw_index_move["after"]["tablespace_oid"]
                with pytest.raises(ValueError, match="raw_index_copy_budget_required"):
                    finalize_rollback_layout(conn, donor, **args)
                with pytest.raises(RuntimeError, match="raw_index_copy_budget_exceeded"):
                    finalize_rollback_layout(conn, donor, **args, raw_index_max_copy_bytes=0)
                with pytest.raises(RuntimeError, match="original_raw_index_changed"), conn.begin_nested():
                    conn.exec_driver_sql("UPDATE market.fact_storage_state SET evidence=evidence - 'raw_mapping_index'")
                    finalize_rollback_layout(conn, donor, **args)
                args["raw_index_max_copy_bytes"] = 2 * 1024**2
                with pytest.raises(ValueError, match="finalize_rollback_invalid_boundary"):
                    finalize_rollback_layout(conn, donor, **(args | {"timeout_seconds": MAX_MIGRATION_SECONDS + 1}))
                args["timeout_seconds"] = 121
                with monkeypatch.context() as patch:
                    def fail_reader(*args, **kwargs):
                        assert 1 <= kwargs["timeout_seconds"] <= 90
                        assert inspect_raw_mapping_index_member(conn)["tablespace_oid"] == RECENT_TABLESPACE_OID
                        assert_original_custody_members(conn, custody_original)
                        raise RuntimeError("injected restored reader failure")

                    patch.setattr(rollback_finalizer, "check_witnesses", fail_reader)
                    with pytest.raises(RuntimeError, match="injected restored reader failure"):
                        finalize_rollback_layout(conn, donor, **args)
                assert inspect_raw_mapping_index_member(conn) == raw_before
                assert inspect_custody_members(conn) == custody_before
                assert conn.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == COLD_HISTORY_LAYOUT_VERSION
            result = finalize_rollback_layout(conn, donor, **args)
            if raw_index_move is not None:
                inverse = result["restored_raw_mapping_index"]
                assert inverse["forward_receipt_hash"] == raw_index_move["receipt_hash"]
                assert inverse["before"] == raw_before
                assert inverse["after"] == inspect_raw_mapping_index_member(conn)
                assert inverse["after"]["tablespace_oid"] == RECENT_TABLESPACE_OID
                assert inverse["after"]["index_oid"] == raw_index_move["before"]["index_oid"]
                custody_inverse = result["restored_custody_placement"]
                assert custody_inverse["forward_receipt_hash"] == custody_forward["receipt_hash"]
                assert custody_inverse["before"] == custody_before
                assert custody_inverse["after"] == assert_original_custody_members(conn, custody_original)
                assert custody_inverse["copy_bytes"] > 0
            if legacy_days:
                assert result["legacy_relation_mapping"]["original_oid"] != result["legacy_relation_mapping"]["restored_oid"]
            else:
                restored_day = result["restored_dated"][scope.dated_days[0].isoformat()]
                assert restored_day["original_header_oid"] != restored_day["header_oid"]
            with archive_namespace(store.root, exclusive=True):
                assert verify_rollback_stream(conn, evidence=stream, expected_hash=stream["receipt_sha256"], **stream_args) == stream

        def original_outcome(check):
            assert_fact_storage_contract(check)
            assert check.scalar(text("SELECT layout_version FROM market.fact_storage_state")) == FACT_STORAGE_LAYOUT_VERSION
            persisted = check.scalar(text("SELECT evidence FROM market.fact_storage_state"))
            assert archive_evidence_hash(persisted) == result["receipt_sha256"]
            assert check.exec_driver_sql("SELECT last_value,is_called FROM market.fact_commit_seq").one() == (gap, True)
            if raw_index_move is not None:
                assert inspect_raw_mapping_index_member(check) == persisted["restored_raw_mapping_index"]["after"]
                assert inspect_custody_members(check) == persisted["restored_custody_placement"]["after"]
            return "original"

        reopen_database_fence(fence, control, expected_outcome="original", read_outcome=original_outcome, record=lambda *_: None)

    restored = Database(scope.storage.dsn)
    try:
        assert restored.ensure_schema(), str(restored.last_error)
        monkeypatch.setattr(market_data, "db", restored)
        assert scope.storage.repo.current_commit_seq() == latest
        assert scope.storage.repo.read_facts(**scope.selection) == scope.expected_latest
        assert scope.storage.repo.read_facts(**later.selection) == later.candles
        assert scope.storage.repo.read_facts(**later.window) == later.trade
        recurring_read = scope.storage.repo.read_facts(series_id=scope.storage.series_id,
            start=recurring_fact.observation_time - timedelta(seconds=1), end=recurring_fact.observation_time + timedelta(seconds=1))
        assert len(recurring_read) == 1 and recurring_read[0].fact == recurring_fact
        assert _admitted_rows(restored, later.window["series_id"]) == later.trades
        assert store.local_path(raw_key).read_bytes() == raw
        check_deployed_source_reader(restored._engine.url, store, scope.frozen.dataset_id,
                                     scope.selection["series_id"], scope.expected_frozen)
        check_deployed_source_reader(restored._engine.url, store, later.frozen.dataset_id,
                                     later.selection["series_id"], later.frozen_candles, latest_only=True)
        check_deployed_source_reader(restored._engine.url, store, later.trade_frozen.dataset_id,
                                     later.window["series_id"], later.trade)
        original_runtime_write = replace(later.candle, open_time=later.candle.close_time,
                                         close_time=later.candle.close_time + timedelta(minutes=1))
        write = scope.storage.repo.ingest_candles(series_id=later.selection["series_id"], source_id=later.product,
                                                  facts=[original_runtime_write], ingestion_run_id="rollback-next-candle")
        assert write.inserted_count == 1 and write.max_commit_seq == result["next_commit"]
    finally:
        restored._reset_engine()
