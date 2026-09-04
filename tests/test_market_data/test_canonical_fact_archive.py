from __future__ import annotations

import copy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import hashlib
import json
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.canonical_storage import record_from_storage_row, record_to_storage_row
from market_data.contracts import SourceIdentity
from market_data.fact_archive import (
    FACT_ARCHIVE_RECORD_SELECTION, FactArchiveLimits, FactArchiveManifest,
    encode_canonical_fact_archive, publish_canonical_fact_archive, read_canonical_fact_archive,
    verify_canonical_fact_archive_rows,
)


BASE = datetime(2026, 8, 9, 12, 0, 0, 123456, tzinfo=UTC)


def _row(revision=1, *, series_id=1, commit=None, state="active"):
    accepted = BASE + timedelta(seconds=revision, microseconds=revision)
    fact = CanonicalFact(
        fact_type="derivatives.funding_rate", payload_schema_id="derivatives.funding_rate.v2",
        observation_key="funding-observation", observation_time=BASE,
        observation_time_method="collector_schedule", accepted_at=accepted, known_at=accepted,
        known_at_method="platform_acceptance", received_at=accepted - timedelta(microseconds=1),
        source_published_at=BASE - timedelta(microseconds=1),
        source=SourceIdentity(provider="COINBASE", venue="COINBASE_DIRECT", source_kind="poll_api",
                              adapter_version="funding.fixture.v1"),
        transformation_id="fixture.v1", state=state,
        payload={"rate": f"0.{revision}2345678901234567890", "raw_rate": f"+0.{revision}2345678901234567890",
                 "funding_time": BASE - timedelta(hours=1), "interval_seconds": 3600, "unit": "fraction"},
        provenance={"response_hash": "a" * 64,
                    "source_positions": [{"definition_id": "definition", "session_id": "session",
                                          "connection_epoch": 2, "receive_ordinal": 17}],
                    "raw_record_id": "raw-fixture", "nullable": None},
        quality={"limitation": "test fixture", "ordered": [2, 1], "valid": state == "active"},
        external_event_key="event", external_event_group_key="group",
        external_event_component_key=None,
    )
    record = CanonicalFactRecord(series_id=series_id, source_id=7, revision=revision,
                                 market_commit_seq=commit or revision, ingestion_run_id="ingestion-fixture",
                                 fact=fact)
    return record_to_storage_row(record, series_dimensions={"currency": "USD"})


@pytest.fixture
def history():
    return [_row(), _row(2), _row(3, state="invalidated"), _row(series_id=2, commit=4)]


def test_every_revision_round_trips_with_exact_clocks_payloads_and_lineage(tmp_path, history):
    encoded = encode_canonical_fact_archive(history, temporary_directory=tmp_path)
    replayed = read_canonical_fact_archive(encoded.path, expected=encoded.manifest)
    assert replayed == tuple(history)
    assert [record_from_storage_row(row) for row in replayed] == [record_from_storage_row(row) for row in history]
    assert replayed[0]["payload"]["rate"] == "0.1234567890123456789"
    assert replayed[0]["received_at"].microsecond == 123456
    assert replayed[0]["known_at"].microsecond == 123457
    assert replayed[2]["state"] == "invalidated"
    assert encoded.manifest.row_count == 4
    assert encoded.manifest.first_cursor == (1, history[0]["id"])
    assert encoded.manifest.last_cursor == (4, history[-1]["id"])
    bounds = encoded.manifest.series
    assert [(item.series_id, item.row_count) for item in bounds] == [(1, 3), (2, 1)]
    assert bounds[0].last_known_at == history[2]["known_at"]
    assert bounds[0].source_ids == (7,)
    assert bounds[0].payload_contracts == ((history[0]["payload_schema_id"], history[0]["payload_contract_hash"]),)
    assert encoded.manifest.to_dict()["record_selection"] == FACT_ARCHIVE_RECORD_SELECTION
    assert len(encoded.manifest.manifest_hash) == 64
    assert pq.ParquetFile(encoded.path).metadata.row_group(0).column(0).compression == "ZSTD"


def test_source_page_verification_requires_every_exact_revision(tmp_path, history):
    encoded = encode_canonical_fact_archive(history, temporary_directory=tmp_path)
    verify_canonical_fact_archive_rows(iter(history), expected=encoded.manifest)
    for incomplete in (history[:-1], history[1:], history[::2]):
        with pytest.raises(RuntimeError, match="canonical_archive_source_mismatch"):
            verify_canonical_fact_archive_rows(incomplete, expected=encoded.manifest)


def test_read_only_store_never_creates_publishes_or_deletes(tmp_path, history):
    missing = tmp_path / "not-created"
    with pytest.raises(FileNotFoundError, match="market_archive_root_missing"):
        FilesystemRawArchiveObjectStore(missing, writable=False)
    assert not missing.exists()
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    manifest = publish_canonical_fact_archive(history, object_store=store, temporary_directory=tmp_path / "staging")
    reader = FilesystemRawArchiveObjectStore(store.root, writable=False)
    path = reader.local_path(manifest.object_key)
    assert read_canonical_fact_archive(path, expected=manifest) == tuple(history)
    with pytest.raises(PermissionError, match="publication is disabled"):
        reader.put_verified(object_key=manifest.object_key, source_path=path, expected_sha256=manifest.object_sha256)
    with pytest.raises(PermissionError, match="deletion is disabled"):
        reader.delete_verified(object_key=manifest.object_key, expected_sha256=manifest.object_sha256)
    assert path.exists()


def test_codec_does_not_change_causal_revision_selection(tmp_path, history):
    encoded = encode_canonical_fact_archive(history, temporary_directory=tmp_path)
    replayed = read_canonical_fact_archive(encoded.path, expected=encoded.manifest)
    def visible(rows, watermark, known_at):
        admitted = [row for row in rows if row["series_id"] == 1 and row["market_commit_seq"] <= watermark
                    and row["known_at"] <= known_at]
        latest = max(admitted, key=lambda row: row["revision"]) if admitted else None
        return latest if latest and latest["state"] == "active" else None
    for watermark in (1, 2, 3, 4):
        for delta in (0, 1, 2, 3, 4):
            at = BASE + timedelta(seconds=delta, microseconds=10)
            assert visible(replayed, watermark, at) == visible(history, watermark, at)
    assert visible(replayed, 3, BASE + timedelta(seconds=5)) is None
    assert visible(replayed, 2, BASE + timedelta(seconds=5))["revision"] == 2


def test_bigint_identity_and_watermark_do_not_pass_through_binary_float(tmp_path):
    row = _row(series_id=2**53 + 1, commit=2**53 + 3)
    encoded = encode_canonical_fact_archive([row], temporary_directory=tmp_path)
    assert read_canonical_fact_archive(encoded.path, expected=encoded.manifest) == (row,)
    assert encoded.manifest.first_cursor[0] == 2**53 + 3


@pytest.mark.parametrize("field,value", [("market_commit_seq", 1.0), ("revision", True),
                                        ("id", None), ("observation_time", None)])
def test_column_types_cannot_be_silently_coerced(tmp_path, field, value):
    row = _row()
    row[field] = value
    with pytest.raises(ValueError, match="canonical_archive_"):
        encode_canonical_fact_archive([row], temporary_directory=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_encoding_is_deterministic_across_input_iterators_and_row_groups(tmp_path, history):
    limits = FactArchiveLimits(row_group_size=2)
    first = encode_canonical_fact_archive(history, temporary_directory=tmp_path, limits=limits)
    second = encode_canonical_fact_archive(iter(history), temporary_directory=tmp_path, limits=limits)
    assert first.path != second.path
    assert first.path.read_bytes() == second.path.read_bytes()
    assert first.manifest == second.manifest
    assert pq.ParquetFile(first.path).metadata.num_row_groups == 2


@pytest.mark.parametrize("kind", ["candle", "l2"])
def test_legacy_float_and_atomic_book_payloads_preserve_their_own_hash_contract(tmp_path, kind):
    record = record_from_storage_row(_row())
    if kind == "candle":
        fact_type, schema = "candle.ohlcv", "candle.ohlcv.v1"
        payload = {"close_time": BASE + timedelta(minutes=1), "open": 1.0000000000000002,
                   "high": 2.0, "low": 0.1, "close": 1.2, "volume": None, "trade_count": None}
    else:
        fact_type, schema = "market.l2_book", "market.l2_book.v1"
        payload = {"event_type": "snapshot", "product_definition_version_id": "product.v1",
                   "validity_interval_id": "validity", "reconstruction_version": "l2-absolute.v1",
                   "before_state_hash": None, "after_state_hash": "a" * 64,
                   "event_material_hash": "b" * 64, "entry_count": 2, "unknown_zero_delete_count": 0,
                   "entries": [{"ordinal": i, "side": side, "price": price, "quantity": "1.234567890123456789",
                                "provider_size_unit": "base", "provider_event_time": BASE}
                               for i, (side, price) in enumerate((("bid", "118000"), ("ask", "118001")))]}
    fact = replace(record.fact, fact_type=fact_type, payload_schema_id=schema, payload=payload)
    record = replace(record, fact=fact, row_hash=None, fact_version_id=None)
    row = record_to_storage_row(record, series_dimensions={})
    encoded = encode_canonical_fact_archive([row], temporary_directory=tmp_path)
    assert read_canonical_fact_archive(encoded.path, expected=encoded.manifest) == (row,)
    assert record_from_storage_row(row) == record


def test_persisted_manifest_round_trips_with_a_bound_descriptor_hash(tmp_path, history):
    encoded = encode_canonical_fact_archive(history, temporary_directory=tmp_path)
    payload = json.loads(json.dumps(encoded.manifest.to_dict()))
    restored = FactArchiveManifest.from_dict(payload, expected_hash=encoded.manifest.manifest_hash)
    assert restored == encoded.manifest
    assert read_canonical_fact_archive(encoded.path, expected=restored) == tuple(history)
    payload["series"][0]["last_known_at"] = BASE.isoformat()
    with pytest.raises(ValueError, match="canonical_archive_manifest_hash_mismatch"):
        FactArchiveManifest.from_dict(payload, expected_hash=encoded.manifest.manifest_hash)


@pytest.mark.parametrize("field,value", [("record_selection", "latest_only"), ("object_key", "../wrong"),
                                        ("extra", "unreviewed"), ("row_count", True)])
def test_manifest_rejects_other_contracts_and_noncanonical_fields(tmp_path, field, value):
    encoded = encode_canonical_fact_archive([_row()], temporary_directory=tmp_path)
    payload = encoded.manifest.to_dict()
    payload[field] = value
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    with pytest.raises(ValueError, match="canonical_archive_manifest_invalid"):
        FactArchiveManifest.from_dict(payload, expected_hash=digest)


@pytest.mark.parametrize("mode", ["missing", "truncated", "changed"])
def test_absent_or_corrupt_bytes_are_never_returned(tmp_path, history, mode):
    encoded = encode_canonical_fact_archive(history, temporary_directory=tmp_path)
    if mode == "missing":
        encoded.path.unlink()
        with pytest.raises(FileNotFoundError):
            read_canonical_fact_archive(encoded.path, expected=encoded.manifest)
        return
    content = encoded.path.read_bytes()
    encoded.path.write_bytes(content[:100] if mode == "truncated" else content[:100] + bytes([content[100] ^ 1]) + content[101:])
    with pytest.raises(RuntimeError, match="canonical_archive_checksum_mismatch"):
        read_canonical_fact_archive(encoded.path, expected=encoded.manifest)


@pytest.mark.parametrize("field", ["row_count", "content_fingerprint", "first_cursor", "series"])
def test_manifest_count_fingerprint_cursor_and_bounds_are_verified(tmp_path, history, field):
    encoded = encode_canonical_fact_archive(history, temporary_directory=tmp_path)
    changes = {
        "row_count": 3, "content_fingerprint": "0" * 64, "first_cursor": (2, history[1]["id"]),
        "series": (replace(encoded.manifest.series[0], last_known_at=BASE), encoded.manifest.series[1]),
    }
    with pytest.raises(RuntimeError, match="canonical_archive_manifest_mismatch"):
        read_canonical_fact_archive(encoded.path, expected=replace(encoded.manifest, **{field: changes[field]}))


@pytest.mark.parametrize("field", ["payload_hash", "row_hash", "source_identity_key", "payload_contract_hash"])
def test_bad_envelope_is_rejected_before_a_manifest_exists(tmp_path, field):
    row = _row()
    row[field] = "0" * 64
    with pytest.raises(RuntimeError, match="market_data_corrupt"):
        encode_canonical_fact_archive([row], temporary_directory=tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mutation", ["extra", "missing", "naive", "duplicate", "reordered", "empty"])
def test_shape_and_page_order_fail_loud_without_partial_artifacts(tmp_path, history, mutation):
    rows = copy.deepcopy(history)
    if mutation == "extra":
        rows[0]["unreviewed_column"] = "must not disappear"
    elif mutation == "missing":
        rows[0].pop("quality")
    elif mutation == "naive":
        rows[0]["known_at"] = rows[0]["known_at"].replace(tzinfo=None)
    elif mutation == "duplicate":
        rows[1] = rows[0]
    elif mutation == "reordered":
        rows.reverse()
    else:
        rows = []
    with pytest.raises(ValueError, match="canonical_archive_"):
        encode_canonical_fact_archive(rows, temporary_directory=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_page_limit_never_silently_truncates_or_consumes_unbounded_input(tmp_path):
    consumed = []
    def rows():
        for index in range(1, 100):
            consumed.append(index)
            yield _row(index)
    with pytest.raises(ValueError, match="canonical_archive_limit_exceeded"):
        encode_canonical_fact_archive(rows(), temporary_directory=tmp_path,
                                      limits=FactArchiveLimits(max_rows=2, row_group_size=1))
    assert consumed == [1, 2, 3]
    assert list(tmp_path.iterdir()) == []


def test_logical_and_physical_limits_block_encode_and_read(tmp_path):
    with pytest.raises(ValueError, match="canonical_archive_limit_exceeded"):
        encode_canonical_fact_archive([_row()], temporary_directory=tmp_path,
                                      limits=FactArchiveLimits(max_logical_bytes=100))
    assert list(tmp_path.iterdir()) == []
    encoded = encode_canonical_fact_archive([_row()], temporary_directory=tmp_path)
    with pytest.raises(ValueError, match="canonical_archive_file_limit_exceeded"):
        read_canonical_fact_archive(encoded.path, expected=encoded.manifest,
                                    limits=FactArchiveLimits(max_file_bytes=100))


def test_repacked_corrupt_payload_is_still_checked_by_the_shared_canonical_decoder(tmp_path):
    encoded = encode_canonical_fact_archive([_row()], temporary_directory=tmp_path)
    table = pq.read_table(encoded.path)
    rows = table.to_pylist()
    payload = json.loads(rows[0]["payload"])
    payload["rate"] = "0.9"
    rows[0]["payload"] = json.dumps(payload)
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), encoded.path, compression="zstd")
    changed = replace(encoded.manifest, object_sha256=hashlib.sha256(encoded.path.read_bytes()).hexdigest(),
                      byte_count=encoded.path.stat().st_size)
    with pytest.raises(RuntimeError, match="canonical Fact hash mismatch"):
        read_canonical_fact_archive(encoded.path, expected=changed)


@pytest.mark.parametrize("change", ["selection", "compression"])
def test_repacked_file_cannot_change_archive_contract(tmp_path, change):
    encoded = encode_canonical_fact_archive([_row()], temporary_directory=tmp_path)
    table = pq.read_table(encoded.path)
    if change == "selection":
        table = table.replace_schema_metadata({**table.schema.metadata, b"record_selection": b"latest_only"})
    pq.write_table(table, encoded.path, compression="snappy" if change == "compression" else "zstd")
    changed = replace(encoded.manifest, object_sha256=hashlib.sha256(encoded.path.read_bytes()).hexdigest(),
                      byte_count=encoded.path.stat().st_size)
    with pytest.raises(RuntimeError, match="canonical_archive_(schema|compression)_mismatch"):
        read_canonical_fact_archive(encoded.path, expected=changed)


def test_interruption_after_upload_can_resume_without_replacing_the_object(tmp_path, history):
    class InterruptedStore(FilesystemRawArchiveObjectStore):
        def put_verified(self, **kwargs):
            super().put_verified(**kwargs)
            raise RuntimeError("injected interruption before manifest registration")
    root = tmp_path / "objects"
    staging = tmp_path / "staging"
    with pytest.raises(RuntimeError, match="injected interruption"):
        publish_canonical_fact_archive(history, object_store=InterruptedStore(root), temporary_directory=staging)
    assert len(list(root.rglob("*.parquet"))) == 1
    assert list(staging.iterdir()) == []
    store = FilesystemRawArchiveObjectStore(root)
    resumed = publish_canonical_fact_archive(history, object_store=store, temporary_directory=staging)
    assert len(list(root.rglob("*.parquet"))) == 1
    assert read_canonical_fact_archive(store.local_path(resumed.object_key), expected=resumed) == tuple(history)


def test_publish_is_immutable_reusable_and_cleans_only_its_staging(tmp_path, history):
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    temporary = tmp_path / "staging"
    first = publish_canonical_fact_archive(history, object_store=store, temporary_directory=temporary)
    second = publish_canonical_fact_archive(history, object_store=store, temporary_directory=temporary)
    assert first == second
    assert read_canonical_fact_archive(store.local_path(first.object_key), expected=first) == tuple(history)
    assert list(temporary.iterdir()) == []
    assert len(list(store.root.rglob("*.parquet"))) == 1


def test_publish_verifies_destination_not_just_upload_acknowledgement(tmp_path, history):
    class CorruptingStore(FilesystemRawArchiveObjectStore):
        def put_verified(self, **kwargs):
            result = super().put_verified(**kwargs)
            self.local_path(result.object_key).write_bytes(b"bad destination")
            return result
    store = CorruptingStore(tmp_path / "objects")
    with pytest.raises(RuntimeError, match="canonical_archive_checksum_mismatch"):
        publish_canonical_fact_archive(history, object_store=store, temporary_directory=tmp_path / "staging")
    assert list((tmp_path / "staging").iterdir()) == []


def test_full_default_page_stress_preserves_all_revisions(tmp_path):
    rows = [_row(revision) for revision in range(1, 10_001)]
    started = time.perf_counter()
    encoded = encode_canonical_fact_archive(rows, temporary_directory=tmp_path)
    encoded_at = time.perf_counter()
    replayed = read_canonical_fact_archive(encoded.path, expected=encoded.manifest)
    finished = time.perf_counter()
    assert replayed == tuple(rows)
    assert encoded.manifest.row_count == FactArchiveLimits().max_rows
    print("canonical_archive_bounded_page_proof " + json.dumps({
        "rows": len(rows), "logical_bytes": encoded.manifest.logical_byte_count,
        "parquet_bytes": encoded.manifest.byte_count,
        "encode_and_verify_seconds": round(encoded_at - started, 3),
        "read_and_verify_seconds": round(finished - encoded_at, 3),
    }, sort_keys=True))
