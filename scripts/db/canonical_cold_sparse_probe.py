"""Offline exact-ID row-group probe for fully admitted immutable archive v1 pages."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
from stat import S_ISREG

import pyarrow.parquet as pq

from core.execution_control import execution_checkpoint
from core.storage_mounts import require_configured_archive_mount
from market_data.canonical_storage import record_from_storage_row
from market_data.cold_history import ColdFactLocation
from market_data.fact_archive import (
    ArchiveChecksumMismatch, FactArchiveLimits, _JSON_COLUMNS, _MeteredArchiveFile,
    _hash_handle, _json_bytes, _schema,
)


@dataclass(frozen=True)
class AdmittedLocator:
    """Full visible SQL locator bound to its authenticated manifest descriptor."""

    fact_id: str
    location: ColdFactLocation
    series_id: int
    key_digest: bytes
    revision: int
    market_commit_seq: int
    row_hash: str


def _stamp(info):
    return info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _assert_stable(path, source, original):
    if _stamp(os.fstat(source.fileno())) != original or _stamp(path.lstat()) != original:
        raise ArchiveChecksumMismatch(f"sparse probe immutable object changed during read: {path.name}")


def _groups(metadata, expected, requests, limits):
    if (metadata.num_rows != expected.row_count or metadata.num_rows > limits.max_rows
            or not 1 <= metadata.num_row_groups <= metadata.num_rows):
        raise RuntimeError("sparse probe footer row count differs from admitted manifest")
    remaining = iter(sorted(requests))
    wanted = next(remaining, None)
    groups = []
    offset = uncompressed = 0
    for index in range(metadata.num_row_groups):
        execution_checkpoint()
        group = metadata.row_group(index)
        if not 1 <= group.num_rows <= limits.row_group_size:
            raise RuntimeError("sparse probe row-group extent exceeds budget")
        for column in range(group.num_columns):
            info = group.column(column)
            if info.compression != "ZSTD" or info.file_path:
                raise RuntimeError("sparse probe requires inline ZSTD column chunks")
            uncompressed += info.total_uncompressed_size
        chosen = []
        end = offset + group.num_rows
        while wanted is not None and wanted < end:
            chosen.append((wanted - offset, requests[wanted]))
            wanted = next(remaining, None)
        if chosen:
            groups.append((index, group.num_rows, chosen))
        offset = end
    if offset != expected.row_count or wanted is not None or uncompressed > limits.max_file_bytes:
        raise RuntimeError("sparse probe footer coverage or uncompressed extent differs")
    return groups


def _verify_selected_row(raw, *, claim, bounds):
    for name in _JSON_COLUMNS:
        raw[name] = json.loads(raw[name])
        if not isinstance(raw[name], dict):
            raise RuntimeError("sparse probe requires exact object-valued canonical JSON")
    record = record_from_storage_row(raw)
    actual = (record.fact_version_id, record.series_id, sha256(record.fact.observation_key.encode()).digest(),
              record.revision, record.market_commit_seq, record.row_hash)
    if actual != (claim.fact_id, claim.series_id, claim.key_digest, claim.revision, claim.market_commit_seq, claim.row_hash):
        raise RuntimeError(f"sparse probe exact locator mismatch: id={claim.fact_id}")
    fact = record.fact
    manifest = claim.location.manifest
    cursor = record.market_commit_seq, claim.fact_id
    if (not manifest.first_cursor <= cursor <= manifest.last_cursor or bounds is None
            or raw["source_id"] not in bounds.source_ids
            or (fact.payload_schema_id, fact.payload_contract_hash) not in bounds.payload_contracts
            or not bounds.first_observation_at <= fact.observation_time <= bounds.last_observation_at
            or not bounds.first_known_at <= fact.known_at <= bounds.last_known_at
            or not bounds.first_accepted_at <= fact.accepted_at <= bounds.last_accepted_at):
        raise RuntimeError(f"sparse probe exact row outside admitted manifest bounds: id={claim.fact_id}")
    return raw


def read_admitted_rows(path: Path, *, claims, limits=FactArchiveLimits(), ensure_contracts=None):
    """Hash every compressed byte, selectively decode only claimed row groups.

    Requires the exact manifest of a successful prior full admission. It is
    unsuitable for first publication, recovery verification or search discovery.
    """
    if not claims or len(claims) > limits.max_rows:
        raise ValueError("sparse probe claims empty or exceed page limit")
    expected = claims[0].location.manifest
    by_position = {claim.location.ordinal: claim for claim in claims}
    if (len(by_position) != len(claims) or len({claim.fact_id for claim in claims}) != len(claims)
            or any(claim.location.manifest != expected for claim in claims)):
        raise ValueError("sparse probe conflicting or cross-page claims")
    if expected.byte_count > limits.max_file_bytes or expected.logical_byte_count > limits.max_logical_bytes:
        raise ValueError("sparse probe admitted manifest exceeds budget")
    bounds = {item.series_id: item for item in expected.series}
    if ensure_contracts is not None:
        ensure_contracts(sorted({contract for item in expected.series for contract in item.payload_contracts}))
    require_configured_archive_mount(path, require_writable=False)
    result, decoded = {}, 0
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), "rb") as source, _MeteredArchiveFile(source, tier="source") as handle:
        observed = os.fstat(source.fileno())
        if not S_ISREG(observed.st_mode) or observed.st_size != expected.byte_count:
            raise ArchiveChecksumMismatch("sparse probe immutable file type or size differs from manifest")
        original = _stamp(observed)
        _assert_stable(path, source, original)
        before = _hash_handle(handle, max_bytes=limits.max_file_bytes)
        if before != (expected.object_sha256, expected.byte_count):
            raise ArchiveChecksumMismatch("sparse probe immutable checksum differs from manifest")
        _assert_stable(path, source, original)
        handle.seek(0)
        parquet = pq.ParquetFile(handle, page_checksum_verification=True,
                                 thrift_string_size_limit=8 * 1024**2, thrift_container_size_limit=100_000)
        if not parquet.schema_arrow.equals(_schema(), check_metadata=True):
            raise RuntimeError("sparse probe archive schema differs from native v1")
        for index, count, positions in _groups(parquet.metadata, expected, by_position, limits):
            execution_checkpoint()
            group = parquet.read_row_group(index, use_threads=False)
            if group.num_rows != count:
                raise RuntimeError("sparse probe decoded row group differs from authenticated footer")
            decoded += count
            for offset, claim in positions:
                raw, = group.slice(offset, 1).to_pylist()
                result[claim.fact_id] = _verify_selected_row(raw, claim=claim, bounds=bounds.get(claim.series_id))
        _assert_stable(path, source, original)
    if len(result) != len(claims) or sum(len(_json_bytes(row)) for row in result.values()) > limits.max_logical_bytes:
        raise RuntimeError("sparse probe exact selected coverage or byte budget differs")
    return result, decoded


def read_sparse_claims(store, claims, *, limits=FactArchiveLimits(), ensure_contracts=None):
    groups = defaultdict(list)
    manifests = {}
    for claim in claims:
        manifest = claim.location.manifest
        if manifests.setdefault(manifest.manifest_id, manifest) != manifest:
            raise RuntimeError("sparse probe ambiguous immutable manifest")
        groups[manifest.manifest_id].append(claim)
    result, count = {}, 0
    for identity, members in groups.items():
        page = manifests[identity]
        rows, decoded = read_admitted_rows(store.local_path(page.object_key), claims=members,
                                           limits=limits, ensure_contracts=ensure_contracts)
        if result.keys() & rows.keys():
            raise RuntimeError("sparse probe duplicate cross-page row identity")
        result.update(rows)
        count += decoded
    return result, count
