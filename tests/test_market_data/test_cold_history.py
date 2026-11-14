from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, timedelta
from io import BytesIO
import os

import pytest

from core.execution_control import ExecutionBudgetExceededError, ExecutionControl, controlled_execution
from market_data import fact_archive
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical_storage import record_from_storage_row, record_to_storage_row
from market_data.cold_history import ColdFactLocation, ColdHistoryReader, publish_cold_page, select_fact_rows
from market_data.fact_archive import ArchiveChecksumMismatch, FactArchiveLimits
from tests.test_market_data.test_canonical_fact_archive import BASE, _row


@pytest.fixture
def store(tmp_path):
    return FilesystemRawArchiveObjectStore(tmp_path / "objects")


def test_cold_page_exact_readback_and_lookup(store, tmp_path):
    rows = [_row(), _row(revision=2, commit=4), _row(series_id=2, commit=5)]
    working = tmp_path / "working"
    manifest = publish_cold_page(
        [dict(row, storage_day=date(2026, 10, 9)) for row in reversed(rows)],
        object_store=store, temporary_directory=working,
    )
    contracts = []
    reader = ColdHistoryReader(store, ensure_contracts=contracts.extend)
    assert reader.read_page(manifest) == tuple(rows)
    assert not list(working.iterdir())
    assert reader.read_by_ids({
        rows[0]["id"]: ColdFactLocation(manifest, 0),
        rows[2]["id"]: ColdFactLocation(manifest, 2),
    }) == {row["id"]: row for row in (rows[0], rows[2])}
    assert contracts == [(rows[0]["payload_schema_id"], rows[0]["payload_contract_hash"])] * 2
    with pytest.raises(RuntimeError, match="canonical_cold_locator_mismatch"):
        reader.read_by_ids({rows[1]["id"]: ColdFactLocation(manifest, 0)})
    with pytest.raises(ValueError, match="canonical_cold_ordinal_invalid"):
        ColdFactLocation(manifest, 3)


def test_admitted_exact_read_only_decodes_selected_groups_and_preserves_full_verification(store, tmp_path):
    limits = FactArchiveLimits(max_rows=32, row_group_size=7)
    rows = [_row(revision=ordinal + 1, commit=ordinal + 1) for ordinal in range(22)]
    manifest = publish_cold_page(rows, object_store=store, temporary_directory=tmp_path, limits=limits)
    reader = ColdHistoryReader(store, limits=limits)
    locators = {rows[index]["id"]: ColdFactLocation(manifest, index) for index in (0, 2, 21)}
    expected = {rows[index]["id"]: rows[index] for index in (0, 2, 21)}
    selective, full = ExecutionControl(), ExecutionControl()
    with controlled_execution(selective):
        assert reader.read_admitted_by_ids(locators) == expected
    sparse_counts = selective.snapshot()["consumed"]
    assert sparse_counts["archive_decoded_rows"] == 8
    assert sparse_counts["archive_decoded_row_groups"] == 2
    assert sparse_counts["archive_source_bytes"] >= manifest.byte_count
    assert sparse_counts["input_rows"] == len(locators)
    with controlled_execution(full):
        assert reader.read_by_ids(locators) == expected
    assert full.snapshot()["consumed"]["archive_decoded_rows"] == len(rows)

    fallback = ExecutionControl()
    with controlled_execution(fallback):
        # The existing whole-page reader handles archive pages authored with
        # wider groups without expanding the fast path's decode bound.
        assert ColdHistoryReader(store, limits=replace(limits, row_group_size=4)).read_admitted_by_ids(locators) == expected
    assert fallback.snapshot()["consumed"]["archive_decoded_rows"] == len(rows)

    budget = ExecutionControl()
    budget.limit(seconds=10, archive_decoded_rows=6)
    with pytest.raises(ExecutionBudgetExceededError, match="archive_decoded_rows"), controlled_execution(budget):
        reader.read_admitted_by_ids(locators)


def test_admitted_exact_read_rejects_unselected_damage_and_path_replacement(store, tmp_path, monkeypatch):
    limits = FactArchiveLimits(max_rows=32, row_group_size=7)
    rows = [_row(revision=index + 1, commit=index + 1) for index in range(22)]
    manifest = publish_cold_page(rows, object_store=store, temporary_directory=tmp_path, limits=limits)
    path = store.local_path(manifest.object_key)
    initial = path.read_bytes()
    locators = {rows[-1]["id"]: ColdFactLocation(manifest, len(rows) - 1)}
    reader = ColdHistoryReader(store, limits=limits)
    damaged = bytearray(initial)
    damaged[12] ^= 1
    path.write_bytes(damaged)
    with pytest.raises(ArchiveChecksumMismatch, match="checksum_mismatch"):
        reader.read_admitted_by_ids(locators)
    path.write_bytes(initial)
    original_hash = fact_archive._hash_handle

    def replace_after_hash(handle, *, max_bytes):
        verified = original_hash(handle, max_bytes=max_bytes)
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(initial)
        os.replace(replacement, path)
        return verified

    monkeypatch.setattr(fact_archive, "_hash_handle", replace_after_hash)
    with pytest.raises(ArchiveChecksumMismatch, match="changed_during_read"):
        reader.read_admitted_by_ids(locators)


def test_archive_byte_budget_includes_decoder_threads():
    control = ExecutionControl()
    control.limit(seconds=10, archive_read_bytes=3)
    with pytest.raises(ExecutionBudgetExceededError, match="used=4 limit=3"), controlled_execution(control):
        with fact_archive._MeteredArchiveFile(BytesIO(b"abcd"), tier="source") as handle:
            with ThreadPoolExecutor(max_workers=1) as executor:
                assert executor.submit(handle.read, 2).result() == b"ab"
                assert control.snapshot()["consumed"]["archive_source_bytes"] == 2
                executor.submit(handle.read, 2).result()


def test_range_prunes_only_impossible_pages_and_fails_when_selected_bytes_are_missing(store, tmp_path):
    relevant = _row()
    unrelated = _row(series_id=2, commit=2)
    first = publish_cold_page([relevant], object_store=store, temporary_directory=tmp_path)
    other = publish_cold_page([unrelated], object_store=store, temporary_directory=tmp_path)
    store.local_path(other.object_key).unlink()
    reader = ColdHistoryReader(store)
    assert list(reader.iter_range(
        [first, other], start=BASE, end=BASE + timedelta(microseconds=1), series_id=1,
    )) == [relevant]
    assert list(reader.iter_range(
        [first, other], start=BASE - timedelta(microseconds=1), end=BASE, series_id=1,
    )) == []
    assert list(reader.iter_range(
        [first], start=BASE, end=BASE + timedelta(seconds=1), series_id=1, as_of_commit_seq=0,
    )) == []
    with pytest.raises(FileNotFoundError):
        list(reader.iter_range([other], start=BASE, end=BASE + timedelta(seconds=1), series_id=2))
    store.local_path(first.object_key).write_bytes(b"broken")
    with pytest.raises(ArchiveChecksumMismatch):
        reader.read_by_ids({relevant["id"]: ColdFactLocation(first, 0)})


def test_publish_reads_only_one_bounded_input_page(store, tmp_path):
    pulled = []

    def input_rows():
        for sequence in range(1, 10):
            pulled.append(sequence)
            yield _row(commit=sequence)

    with pytest.raises(ValueError, match="canonical_archive_limit_exceeded"):
        publish_cold_page(input_rows(), object_store=store, temporary_directory=tmp_path,
                          limits=FactArchiveLimits(max_rows=2, row_group_size=1))
    assert pulled == [1, 2, 3]
    assert not list(store.root.rglob("*.parquet"))


def test_mixed_selection_filters_before_ranking_and_applies_tombstone_afterward(store, tmp_path):
    active = _row()
    corrected = _row(2)
    invalidated = _row(3, state="invalidated")
    record = record_from_storage_row(invalidated)
    amended_source = replace(record.fact.source, adapter_version="other.adapter.v1")
    invalidated = record_to_storage_row(replace(
        record, source_id=9, fact=replace(record.fact, source=amended_source),
        row_hash=None, fact_version_id=None,
    ), series_dimensions={"currency": "USD"})
    manifest = publish_cold_page([active, corrected], object_store=store, temporary_directory=tmp_path)
    reader = ColdHistoryReader(store)
    scope = {"series_id": 1, "start": BASE, "end": BASE + timedelta(seconds=1)}

    def read(**options):
        filters = {key: value for key, value in options.items()
                   if key in ("known_at_lte", "as_of_commit_seq", "source_identity_keys")}
        return select_fact_rows(
            [dict(invalidated, storage_day=date(2026, 10, 9))],
            reader.iter_range([manifest], **scope, **filters), **scope, **options,
        )

    assert read() == []
    assert read(include_invalidated=True) == [invalidated]
    assert read(as_of_commit_seq=1) == [active]
    assert read(known_at_lte=corrected["known_at"]) == [corrected]
    assert read(source_identity_keys=(active["source_identity_key"],)) == [corrected]
    assert read(latest_only=False, include_invalidated=True) == [active, corrected, invalidated]


def test_close_causality_and_overlap_equality_apply_across_tiers():
    base = record_from_storage_row(_row())
    close = BASE + timedelta(seconds=10)
    payload = {"close_time": close, "open": 1.0000000000000002, "high": 2.0,
               "low": 0.1, "close": 1.2, "volume": None, "trade_count": None}
    fact = replace(base.fact, fact_type="candle.ohlcv", payload_schema_id="candle.ohlcv.v1",
                   payload=payload, known_at=close)
    earlier = record_to_storage_row(replace(base, fact=fact, row_hash=None, fact_version_id=None),
                                    series_dimensions={})
    later = record_to_storage_row(replace(
        base, revision=2, market_commit_seq=2, row_hash=None, fact_version_id=None,
        fact=replace(fact, known_at=close + timedelta(microseconds=1)),
    ), series_dimensions={})
    scope = {"series_id": 1, "start": BASE, "end": close}
    assert select_fact_rows([later], [earlier], **scope) == [later]
    assert select_fact_rows([later], [earlier], **scope, causal_at_interval_close=True) == [earlier]
    assert select_fact_rows([later], [earlier], **scope, causal_at_interval_close=True,
                            latest_only=False) == [earlier]
    assert select_fact_rows([dict(earlier, storage_day=date(2026, 10, 9))], [earlier], **scope) == [earlier]
    with pytest.raises(RuntimeError, match="canonical_cold_overlap_mismatch"):
        select_fact_rows([dict(earlier, quality={})], [earlier], **scope)
    with pytest.raises(RuntimeError, match="canonical_cold_overlap_mismatch"):
        float_quality = {**earlier["quality"], "ordered": [2.0, 1]}
        select_fact_rows([dict(earlier, quality=float_quality)], [earlier], **scope)
    with pytest.raises(RuntimeError, match="canonical_cold_duplicate_fact"):
        select_fact_rows([earlier], [earlier, earlier], **scope)
    with pytest.raises(RuntimeError, match="canonical_cold_revision_conflict"):
        select_fact_rows([dict(earlier, id="different-id")], [earlier], **scope)
