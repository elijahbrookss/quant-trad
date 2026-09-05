from dataclasses import replace
from decimal import Decimal
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data import book_archive
from market_data.book_archive import BookCheckpointReadLimits, encode_book_checkpoint_parquet, read_book_checkpoint_parquet
from market_data.order_book import (
    BOOK_CHECKPOINT_SCHEMA_VERSION, Level2BookReconstructor,
    book_checkpoint_content_fingerprint, checkpoint_canonical_rows,
)
from tests.test_market_data.test_order_book_phase2 import _contract, _snapshot


def _fixture(tmp_path):
    checkpoint = Level2BookReconstructor(series_id=1, contract=_contract()).process(_snapshot()).checkpoints[0]
    encoded = encode_book_checkpoint_parquet(checkpoint, temporary_directory=tmp_path)
    expected = {"id": checkpoint.checkpoint_id, "state_hash": checkpoint.state_hash,
        "content_fingerprint": checkpoint.content_fingerprint, "object_sha256": encoded.sha256,
        "byte_count": encoded.byte_count, "level_count": encoded.level_count,
        "bid_level_count": len(checkpoint.bids), "ask_level_count": len(checkpoint.asks),
        "provider_size_unit": checkpoint.provider_size_unit.value, "format": "parquet",
        "compression": "zstd", "schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION}
    return checkpoint, encoded.path, expected


def test_streamed_checkpoint_fingerprint_preserves_exact_v1_json():
    identity = 'checkpoint-"\\\u03b1'
    state_hash = "a" * 64
    levels = [("bid", Decimal("98.00"), Decimal("0.2500")), ("ask", Decimal("1E+2"), Decimal("1.5"))]
    legacy = {"schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION, "checkpoint_id": identity,
              "state_hash": state_hash, "levels": [("bid", "98", "0.25"), ("ask", "100", "1.5")]}
    expected = hashlib.sha256(json.dumps(legacy, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    assert book_checkpoint_content_fingerprint(checkpoint_id=identity, state_hash=state_hash, levels=iter(levels)) == expected


def test_checkpoint_reads_bounded_physical_batches_and_binds_manifest(tmp_path, monkeypatch):
    checkpoint, path, expected = _fixture(tmp_path)
    original = pq.ParquetFile
    batches = []
    class PhysicalFile:
        def __init__(self, *args, **kwargs):
            assert kwargs["page_checksum_verification"] is True
            self.file = original(*args, **kwargs)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return self.file.__exit__(*args)
        def __getattr__(self, name):
            return getattr(self.file, name)
        def read(self, *args, **kwargs):
            raise AssertionError("whole-file Arrow read is not bounded")
        def iter_batches(self, **kwargs):
            batches.append(kwargs)
            return self.file.iter_batches(**kwargs)
    monkeypatch.setattr(pq, "ParquetFile", PhysicalFile)
    assert read_book_checkpoint_parquet(path, expected=expected, limits=BookCheckpointReadLimits(batch_rows=1)) == checkpoint_canonical_rows(checkpoint)
    assert batches == [{"batch_size": 1, "use_threads": False}]


@pytest.mark.parametrize("field,value", [
    ("schema_version", "wrong"), ("checkpoint_id", "different"), ("side", "unknown"),
    ("provider_size_unit", "base"), ("provider_size_unit", "unknown"), ("level_ordinal", 1),
    ("price", "NaN"), ("price", "Infinity"), ("price", "1e999999999"), ("price", "0"),
    ("quantity", "-1"), ("quantity", "0"), ("quantity", "9" * 257), ("quantity", "2"),
])
def test_checkpoint_rejects_bad_row_identity_types_order_and_fingerprint(tmp_path, field, value):
    _, path, _ = _fixture(tmp_path)
    with pq.ParquetFile(path) as parquet:
        table = parquet.read()
    rows = table.to_pylist()
    rows[0][field] = value
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path, compression="zstd", row_group_size=1)
    with pytest.raises(RuntimeError, match="market_book_checkpoint_replay_invalid"):
        read_book_checkpoint_parquet(path)


@pytest.mark.parametrize("damage", ["fingerprint", "state_hash", "missing_identity", "extra_column", "compression", "missing_side", "empty", "out_of_order"])
def test_checkpoint_rejects_incomplete_or_inconsistent_files(tmp_path, damage):
    _, path, _ = _fixture(tmp_path)
    with pq.ParquetFile(path) as parquet:
        table = parquet.read()
    metadata = dict(table.schema.metadata)
    if damage in {"fingerprint", "state_hash", "missing_identity"}:
        if damage == "missing_identity":
            metadata.pop(b"checkpoint_id")
        else:
            metadata[b"content_fingerprint" if damage == "fingerprint" else b"state_hash"] = b"0" * 64
        table = table.replace_schema_metadata(metadata)
    elif damage == "extra_column":
        table = table.append_column("unexpected", pa.array([1] * len(table)))
    elif damage in {"missing_side", "empty"}:
        table = table.slice(0, 2 if damage == "missing_side" else 0)
    elif damage == "out_of_order":
        rows = table.to_pylist()
        rows[0], rows[1] = rows[1], rows[0]
        table = pa.Table.from_pylist(rows, schema=table.schema)
    pq.write_table(table, path, compression="snappy" if damage == "compression" else "zstd")
    with pytest.raises(RuntimeError, match="market_book_checkpoint_(?:replay_invalid|read_row_budget)"):
        read_book_checkpoint_parquet(path)


@pytest.mark.parametrize("field", ["max_rows", "max_file_bytes", "max_logical_bytes", "max_row_group_bytes"])
def test_checkpoint_budget_rejection_precedes_decoding(tmp_path, field, monkeypatch):
    _, path, _ = _fixture(tmp_path)
    def must_not_decode(*args, **kwargs):
        raise AssertionError("rejected before Arrow batch decode")
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", must_not_decode)
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        read_book_checkpoint_parquet(path, limits=replace(BookCheckpointReadLimits(), **{field: 1}))


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_checkpoint_limits_reject_nonpositive_or_noninteger_values(value):
    with pytest.raises(ValueError, match="read_limit_invalid"):
        BookCheckpointReadLimits(batch_rows=value)


@pytest.mark.parametrize("field", ["id", "state_hash", "content_fingerprint", "object_sha256", "byte_count", "level_count",
                                   "bid_level_count", "ask_level_count", "provider_size_unit", "format", "compression", "schema_version"])
def test_checkpoint_manifest_fields_are_mandatory_and_exact(tmp_path, field):
    _, path, expected = _fixture(tmp_path)
    missing = {name: value for name, value in expected.items() if name != field}
    with pytest.raises(RuntimeError, match=f"manifest_mismatch:.*field={field}"):
        read_book_checkpoint_parquet(path, expected=missing)
    changed = {**expected, field: expected[field] + 1 if type(expected[field]) is int else "0" * 64}
    with pytest.raises(RuntimeError, match=f"manifest_mismatch:.*field={field}"):
        read_book_checkpoint_parquet(path, expected=changed)


def test_checkpoint_verification_obeys_cancellation_and_detects_change_after_hash(tmp_path, monkeypatch):
    _, path, expected = _fixture(tmp_path)
    count = 0
    def cancel():
        nonlocal count
        count += 1
        if count == 6:
            raise RuntimeError("test cancellation")
    with pytest.raises(RuntimeError, match="test cancellation"):
        read_book_checkpoint_parquet(path, expected=expected, check_budget=cancel)
    fingerprint = book_archive.book_checkpoint_content_fingerprint
    def changed_during_read(**kwargs):
        result = fingerprint(**kwargs)
        with path.open("ab") as file:
            file.write(b"changed after checksum")
        return result
    monkeypatch.setattr(book_archive, "book_checkpoint_content_fingerprint", changed_during_read)
    with pytest.raises(RuntimeError, match="changed_during_verification"):
        read_book_checkpoint_parquet(path, expected=expected)
