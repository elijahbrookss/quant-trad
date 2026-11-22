from dataclasses import replace
import os

import pytest

from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdFactLocation, ColdHistoryReader, publish_cold_page
from market_data.fact_archive import ArchiveChecksumMismatch, FactArchiveLimits
from scripts.db import canonical_cold_sparse_probe as probe
from scripts.db.canonical_cold_scale import _row


@pytest.fixture
def page(tmp_path):
    limits = FactArchiveLimits(max_rows=32, row_group_size=7)
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    rows = [_row(index) for index in range(22)]
    manifest = publish_cold_page(rows, object_store=store, temporary_directory=tmp_path, limits=limits)
    assert ColdHistoryReader(store, limits=limits).read_page(manifest) == tuple(rows)
    path = store.local_path(manifest.object_key)

    def claim(ordinal):
        row = rows[ordinal]
        return probe.AdmittedLocator(row["id"], ColdFactLocation(manifest, ordinal), row["series_id"],
            probe.sha256(row["observation_key"].encode()).digest(), row["revision"], row["market_commit_seq"], row["row_hash"])

    return limits, rows, path, claim


def test_sparse_exact_claims_validate_groups_and_terminal_short_group(page):
    limits, rows, path, claim = page
    results, decoded = probe.read_admitted_rows(path, claims=[claim(0), claim(2), claim(21)], limits=limits)
    assert results == {rows[i]["id"]: rows[i] for i in (0, 2, 21)}
    assert decoded == 8  # A seven-row group and the terminal single row.
    for field, value in (("fact_id", rows[1]["id"]), ("series_id", 90), ("revision", 40),
                         ("key_digest", b"x" * 32), ("row_hash", "0" * 64), ("market_commit_seq", 99)):
        with pytest.raises(RuntimeError, match="locator mismatch"):
            probe.read_admitted_rows(path, claims=[replace(claim(2), **{field: value})], limits=limits)
    with pytest.raises(RuntimeError, match="row-group extent exceeds"):
        probe.read_admitted_rows(path, claims=[claim(21)], limits=replace(limits, row_group_size=5))


def test_sparse_probe_verifies_unselected_bytes_and_detects_path_replacement(page, monkeypatch):
    limits, _rows, path, claim = page
    initial = path.read_bytes()
    damaged = bytearray(initial)
    damaged[12] ^= 1  # Corrupt bytes outside the only selected terminal group.
    path.write_bytes(damaged)
    with pytest.raises(ArchiveChecksumMismatch, match="checksum"):
        probe.read_admitted_rows(path, claims=[claim(21)], limits=limits)
    path.write_bytes(initial)
    original_hash = probe._hash_handle

    def swap_after_hash(handle, *, max_bytes):
        value = original_hash(handle, max_bytes=max_bytes)
        replacement = path.with_suffix(".swap")
        replacement.write_bytes(initial)
        os.replace(replacement, path)
        return value

    monkeypatch.setattr(probe, "_hash_handle", swap_after_hash)
    with pytest.raises(ArchiveChecksumMismatch, match="changed during read"):
        probe.read_admitted_rows(path, claims=[claim(21)], limits=limits)
