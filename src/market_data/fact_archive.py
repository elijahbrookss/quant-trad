"""Bounded, lossless Parquet objects for all canonical Fact revisions.

This codec does not select latest state, reconstruct facts, authorize deletion,
or publish database manifests. The lifecycle repository owns those decisions.
"""

from __future__ import annotations

import hashlib
import json
import io
import logging
import os
import struct
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from stat import S_ISREG
from time import perf_counter
from typing import Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

from core.storage_mounts import require_configured_archive_mount
from core.execution_control import (
    consume_execution_resource, current_execution_control, execution_checkpoint, measure_execution_stage,
)
from .archive import RawArchiveObjectStore
from .archive_verification import _stamp
from .canonical_storage import record_from_storage_row


FACT_ARCHIVE_SCHEMA_VERSION = "market.canonical_fact_archive.v1"
FACT_ARCHIVE_CONTENT_VERSION = "market.canonical_fact_archive_content.v1"
FACT_ARCHIVE_RECORD_SELECTION = "all_canonical_revisions.v1"

_INTEGER_COLUMNS = ("series_id", "source_id", "revision", "market_commit_seq")
_TIME_COLUMNS = ("observation_time", "source_published_at", "received_at", "accepted_at", "known_at")
_JSON_COLUMNS = ("payload", "provenance", "quality", "series_dimensions")
_RANGE_COLUMNS = ("series_id", "observation_time", "market_commit_seq", "known_at", "source_identity_key")
_STRING_COLUMNS = (
    "id", "ingestion_run_id", "observation_key", "fact_type", "payload_schema_id",
    "payload_contract_hash", "observation_time_method", "known_at_method",
    "transformation_id", "external_event_key", "external_event_group_key",
    "external_event_component_key", "state", "payload_hash", "material_hash",
    "provenance_schema_id", "provenance_hash", "quality_schema_id", "quality_hash",
    "row_hash", "source_identity_key", "source_provider", "source_venue",
    "source_kind", "source_adapter_version",
)
_NULLABLE = frozenset((
    "ingestion_run_id", "source_published_at", "received_at", "external_event_key",
    "external_event_group_key", "external_event_component_key",
))


@cache
def _schema() -> pa.Schema:
    fields = [
        pa.field(name, kind, nullable=name in _NULLABLE)
        for names, kind in (
            (_INTEGER_COLUMNS, pa.int64()),
            (_TIME_COLUMNS, pa.timestamp("us", tz="UTC")),
            (_STRING_COLUMNS, pa.string()),
            (_JSON_COLUMNS, pa.string()),
        )
        for name in names
    ]
    return pa.schema(fields, metadata={
        b"schema_version": FACT_ARCHIVE_SCHEMA_VERSION.encode(),
        b"record_selection": FACT_ARCHIVE_RECORD_SELECTION.encode(),
    })


def _json_default(value: object) -> str:
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    raise TypeError(f"canonical_archive_invalid: unsupported JSON value type={type(value).__name__}")


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, default=_json_default, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def archive_evidence_hash(evidence: Mapping[str, Any]) -> str:
    """Hash versioned archive admission evidence, not a new market Fact identity."""
    return hashlib.sha256(_json_bytes(dict(evidence))).hexdigest()


@dataclass(frozen=True)
class FactArchiveLimits:
    max_rows: int = 10_000
    max_logical_bytes: int = 64 * 1024**2
    max_file_bytes: int = 128 * 1024**2
    row_group_size: int = 512

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if type(value) is not int or value <= 0:
                raise ValueError(f"canonical_archive_limit_invalid: field={name}")
        if self.row_group_size > self.max_rows:
            raise ValueError("canonical_archive_limit_invalid: row_group_size exceeds max_rows")


@dataclass(frozen=True)
class FactArchiveSeriesBounds:
    series_id: int
    row_count: int
    first_observation_at: datetime
    last_observation_at: datetime
    first_known_at: datetime
    last_known_at: datetime
    first_accepted_at: datetime
    last_accepted_at: datetime
    source_ids: tuple[int, ...]
    payload_contracts: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class FactArchiveManifest:
    object_sha256: str
    content_fingerprint: str
    byte_count: int
    logical_byte_count: int
    row_count: int
    first_cursor: tuple[int, str]
    last_cursor: tuple[int, str]
    series: tuple[FactArchiveSeriesBounds, ...]

    @property
    def manifest_id(self) -> str:
        return "cfa_" + self.object_sha256

    @property
    def object_key(self) -> str:
        return f"canonical-facts/v1/{self.object_sha256[:2]}/{self.object_sha256}.parquet"

    def to_dict(self) -> dict[str, Any]:
        return json.loads(_json_bytes({
            **asdict(self),
            "schema_version": FACT_ARCHIVE_SCHEMA_VERSION,
            "record_selection": FACT_ARCHIVE_RECORD_SELECTION,
            "content_version": FACT_ARCHIVE_CONTENT_VERSION,
            "format": "parquet",
            "compression": "zstd",
            "manifest_id": self.manifest_id,
            "object_key": self.object_key,
        }))

    @property
    def manifest_hash(self) -> str:
        return hashlib.sha256(_json_bytes(self.to_dict())).hexdigest()

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any], *, expected_hash: str) -> FactArchiveManifest:
        """Load the exact persisted descriptor, including its independently bound hash."""
        if hashlib.sha256(_json_bytes(dict(payload))).hexdigest() != expected_hash:
            raise ValueError("canonical_archive_manifest_hash_mismatch")
        try:
            series = []
            for value in payload["series"]:
                item = dict(value)
                for name in ("first_observation_at", "last_observation_at", "first_known_at",
                             "last_known_at", "first_accepted_at", "last_accepted_at"):
                    item[name] = datetime.fromisoformat(item[name].replace("Z", "+00:00"))
                    if item[name].tzinfo is None:
                        raise ValueError("manifest clock must be timezone-aware")
                item["source_ids"] = tuple(item["source_ids"])
                item["payload_contracts"] = tuple(tuple(pair) for pair in item["payload_contracts"])
                series.append(FactArchiveSeriesBounds(**item))
            manifest = cls(
                object_sha256=payload["object_sha256"], content_fingerprint=payload["content_fingerprint"],
                byte_count=payload["byte_count"], logical_byte_count=payload["logical_byte_count"],
                row_count=payload["row_count"], first_cursor=tuple(payload["first_cursor"]),
                last_cursor=tuple(payload["last_cursor"]), series=tuple(series),
            )
            for digest in (manifest.object_sha256, manifest.content_fingerprint):
                if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                    raise ValueError("invalid SHA-256")
            integers = [manifest.byte_count, manifest.logical_byte_count, manifest.row_count]
            for bounds in series:
                integers.extend((bounds.series_id, bounds.row_count, *bounds.source_ids))
                if not bounds.source_ids or bounds.source_ids != tuple(sorted(set(bounds.source_ids))):
                    raise ValueError("source ids must be nonempty, sorted and unique")
                for prefix in ("observation", "known", "accepted"):
                    if getattr(bounds, f"first_{prefix}_at") > getattr(bounds, f"last_{prefix}_at"):
                        raise ValueError("reversed series bounds")
            for cursor in (manifest.first_cursor, manifest.last_cursor):
                if len(cursor) != 2 or not isinstance(cursor[1], str) or not cursor[1]:
                    raise ValueError("invalid cursor")
                integers.append(cursor[0])
            if any(type(value) is not int or value <= 0 for value in integers):
                raise ValueError("manifest counts and ids must be positive integers")
            if sum(item.row_count for item in series) != manifest.row_count or manifest.first_cursor > manifest.last_cursor:
                raise ValueError("manifest counts or cursor range disagree")
            if [item.series_id for item in series] != sorted({item.series_id for item in series}):
                raise ValueError("series ids must be sorted and unique")
            # This also rejects unknown/missing fields, changed schema/selection,
            # noncanonical clocks, and a forged derived object key or identity.
            if _json_bytes(manifest.to_dict()) != _json_bytes(dict(payload)):
                raise ValueError("descriptor is not canonical or uses another contract")
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"canonical_archive_manifest_invalid: {exc}") from exc
        return manifest


@dataclass(frozen=True)
class EncodedFactArchive:
    path: Path
    manifest: FactArchiveManifest


def _validated_archive_record(row):
    expected = set(_schema().names)
    if set(row) != expected:
        raise ValueError(
            "canonical_archive_columns_invalid: "
            f"missing={sorted(expected - set(row))} extra={sorted(set(row) - expected)}"
        )
    for name in _TIME_COLUMNS:
        value = row[name]
        if value is None and name in _NULLABLE:
            continue
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError(f"canonical_archive_clock_invalid: field={name} id={row['id']}")
    for name in _INTEGER_COLUMNS:
        if type(row[name]) is not int or row[name] <= 0:
            raise ValueError(f"canonical_archive_integer_invalid: field={name} id={row['id']}")
    for name in _STRING_COLUMNS:
        if row[name] is None and name in _NULLABLE:
            continue
        if not isinstance(row[name], str) or (name == "id" and not row[name]):
            raise ValueError(f"canonical_archive_string_invalid: field={name} id={row['id']}")
    for name in _JSON_COLUMNS:
        if not isinstance(row[name], dict):
            raise ValueError(f"canonical_archive_json_invalid: field={name} id={row['id']}")
    return record_from_storage_row(row)


class _Contents:
    def __init__(self, limits: FactArchiveLimits) -> None:
        self.limits = limits
        self.digest = hashlib.sha256(_json_bytes({
            "schema_version": FACT_ARCHIVE_CONTENT_VERSION,
            "record_selection": FACT_ARCHIVE_RECORD_SELECTION,
        }))
        self.rows = 0
        self.logical_bytes = 0
        self.first: tuple[int, str] | None = None
        self.last: tuple[int, str] | None = None
        self.identities: set[str] = set()
        self.revisions: set[tuple[int, str, int]] = set()
        self.series: dict[int, dict[str, Any]] = {}

    def add(self, row: Mapping[str, Any]) -> None:
        record = _validated_archive_record(row)
        cursor = (record.market_commit_seq, str(record.fact_version_id))
        revision = (record.series_id, record.fact.observation_key, record.revision)
        if self.last is not None and cursor <= self.last:
            raise ValueError(f"canonical_archive_order_invalid: cursor={cursor} previous={self.last}")
        if cursor[1] in self.identities or revision in self.revisions:
            raise ValueError(f"canonical_archive_duplicate_revision: id={cursor[1]}")
        encoded = _json_bytes(dict(row))
        if self.rows + 1 > self.limits.max_rows or self.logical_bytes + len(encoded) > self.limits.max_logical_bytes:
            raise ValueError(
                "canonical_archive_limit_exceeded: "
                f"rows={self.rows + 1} logical_bytes={self.logical_bytes + len(encoded)}"
            )
        self.digest.update(struct.pack(">Q", len(encoded)))
        self.digest.update(encoded)
        self.identities.add(cursor[1])
        self.revisions.add(revision)
        self.first = self.first or cursor
        self.last = cursor
        self.rows += 1
        self.logical_bytes += len(encoded)
        item = self.series.setdefault(record.series_id, {
            "row_count": 0, "source_ids": set(), "payload_contracts": set(),
            **{prefix + name: row[column]
               for name, column in (("observation_at", "observation_time"), ("known_at", "known_at"), ("accepted_at", "accepted_at"))
               for prefix in ("first_", "last_")},
        })
        item["row_count"] += 1
        item["source_ids"].add(record.source_id)
        item["payload_contracts"].add((record.fact.payload_schema_id, record.fact.payload_contract_hash))
        for name, column in (("observation_at", "observation_time"), ("known_at", "known_at"), ("accepted_at", "accepted_at")):
            item["first_" + name] = min(item["first_" + name], row[column])
            item["last_" + name] = max(item["last_" + name], row[column])

    def manifest(self, *, sha256: str, byte_count: int) -> FactArchiveManifest:
        if self.first is None or self.last is None:
            raise ValueError("canonical_archive_empty: at least one revision is required")
        return FactArchiveManifest(
            object_sha256=sha256, content_fingerprint=self.digest.hexdigest(),
            byte_count=byte_count, logical_byte_count=self.logical_bytes, row_count=self.rows,
            first_cursor=self.first, last_cursor=self.last,
            series=tuple(FactArchiveSeriesBounds(
                series_id=series_id,
                **{key: value for key, value in item.items() if key not in {"source_ids", "payload_contracts"}},
                source_ids=tuple(sorted(item["source_ids"])),
                payload_contracts=tuple(sorted(item["payload_contracts"])),
            ) for series_id, item in sorted(self.series.items())),
        )


def _hash_handle(handle, *, max_bytes: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        byte_count += len(chunk)
        if byte_count > max_bytes:
            raise ValueError(f"canonical_archive_file_limit_exceeded: bytes={byte_count}")
        digest.update(chunk)
    return digest.hexdigest(), byte_count


def _timed_archive_phase(phase_observer, phase, operation):
    started = perf_counter()
    try:
        result = operation()
    except BaseException:
        if phase_observer is not None:
            phase_observer(phase, perf_counter() - started, "failed")
        raise
    if phase_observer is not None:
        phase_observer(phase, perf_counter() - started, "completed")
    return result


def verify_canonical_fact_archive_rows(
    rows: Iterable[Mapping[str, Any]], *, expected: FactArchiveManifest,
    limits: FactArchiveLimits = FactArchiveLimits(),
    archived_rows: Iterable[Mapping[str, Any]] | None = None,
) -> None:
    """Prove a source page equals a separately verified object's full contents.

    This checks completeness and exact fields, not the object's bytes. Callers
    must also use read_canonical_fact_archive before committing admission.
    """
    if archived_rows is not None:
        missing = object()
        source_count = 0
        source = iter(rows)
        archived = iter(archived_rows)
        while True:
            execution_checkpoint()
            source_row = next(source, missing)
            archived_row = next(archived, missing)
            if source_row is missing or archived_row is missing:
                if source_row is archived_row is missing:
                    break
                raise RuntimeError(
                    f"canonical_archive_source_mismatch: manifest_id={expected.manifest_id}"
                )
            if source_row != archived_row:
                raise RuntimeError(
                    f"canonical_archive_source_mismatch: manifest_id={expected.manifest_id}"
                )
            source_count += 1
            if source_count > limits.max_rows:
                raise ValueError("canonical_archive_limit_exceeded: source row count")
        if source_count != expected.row_count:
            raise RuntimeError(
                f"canonical_archive_source_mismatch: manifest_id={expected.manifest_id}"
            )
        return

    contents = _Contents(limits)
    for row in rows:
        contents.add(row)
    observed = contents.manifest(sha256=expected.object_sha256, byte_count=expected.byte_count)
    if observed != expected:
        raise RuntimeError(f"canonical_archive_source_mismatch: manifest_id={expected.manifest_id}")


class ArchiveChecksumMismatch(RuntimeError):
    """The selected object's bytes disagree with its immutable manifest."""


class _MeteredArchiveFile(io.RawIOBase):
    def __init__(self, source, *, tier):
        self.source = source
        self.tier = tier
        # Arrow can read chunks on a worker thread outside the calling
        # ContextVar scope. Keep those reads in the originating budget.
        self.control = current_execution_control()

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.source.tell()

    def seek(self, offset, whence=0):
        if self.control is not None:
            self.control.check()
        return self.source.seek(offset, whence)

    def read(self, size=-1):
        if self.control is None:
            return self.source.read(size)
        self.control.check()
        started = perf_counter()
        error = None
        try:
            value = self.source.read(size)
        except BaseException as caught:
            error = caught
            raise
        finally:
            self.control.record_time("archive_read", perf_counter() - started, error=error)
        self.control.consume("archive_read_bytes", len(value))
        self.control.consume("archive_cache_bytes" if self.tier == "cache" else "archive_source_bytes", len(value))
        return value

    def readinto(self, buffer):
        value = self.read(len(buffer))
        buffer[:len(value)] = value
        return len(value)

    # Closing this wrapper leaves ownership of the underlying file with its
    # source/cache context, including the active-reader eviction lock.


class CanonicalArchiveReadPage:
    """Retain one verified decoded page for one caller-owned read operation.

    Every use still hashes the currently selected file. Manifest and limit
    equality permits reuse of decoding only, never reuse of byte verification.
    Defensive copies keep caller mutations out of later batches. This state is
    neither shared between requests nor used by lifecycle/recovery readers.
    """
    def __init__(self):
        self._entry = None

    def clear(self) -> None:
        self._entry = None

    def _reuse(self, expected, limits):
        if self._entry is not None:
            manifest, previous_limits, rows = self._entry
            if manifest == expected and previous_limits == limits:
                execution_checkpoint()
                result = deepcopy(rows)
                execution_checkpoint()
                return result
        self.clear()
        return None

    def _remember(self, expected, limits, rows) -> None:
        retained = deepcopy(rows)
        execution_checkpoint()
        self._entry = (expected, limits, retained)


def read_canonical_fact_archive(
    path: Path, *, expected: FactArchiveManifest,
    limits: FactArchiveLimits = FactArchiveLimits(), cache=None,
    decoded_page: CanonicalArchiveReadPage | None = None,
) -> tuple[dict[str, Any], ...]:
    """Verify manifest-bound bytes and contents, optionally from a disposable copy.

    Lifecycle/recovery callers omit cache and always inspect the durable object.
    A cache hit is never evidence that the HDD source passed a recovery check.
    """
    return _read_archive(path, expected=expected, limits=limits, cache=cache, decoded_page=decoded_page)


def read_admitted_canonical_fact_rows(
    path: Path, *, expected: FactArchiveManifest, ordinals: Sequence[int],
    limits: FactArchiveLimits = FactArchiveLimits(), cache=None,
) -> dict[int, dict[str, Any]]:
    """Read exact ordinals only from a manifest already admitted by a full verification."""
    if (not ordinals or len(ordinals) > limits.max_rows
            or any(type(value) is not int or not 0 <= value < expected.row_count for value in ordinals)):
        raise ValueError("canonical_archive_ordinals_invalid")
    positions = tuple(sorted(set(ordinals)))
    return _read_archive(path, expected=expected, limits=limits, cache=cache, ordinals=positions)


def read_admitted_canonical_fact_range(
    path: Path, *, expected: FactArchiveManifest, matching_positions: Callable[[pa.RecordBatch], Sequence[int]],
    limits: FactArchiveLimits = FactArchiveLimits(), cache=None,
) -> tuple[dict[str, Any], ...]:
    """Read matching revisions from an admitted page in its original ordinal order."""
    if not callable(matching_positions):
        raise ValueError("canonical_archive_range_predicate_invalid")
    rows = _read_archive(path, expected=expected, limits=limits, cache=cache, matching_positions=matching_positions)
    return tuple(rows.values())


def _read_archive(path, *, expected, limits, cache, decoded_page=None, ordinals=None, matching_positions=None):
    admitted = ordinals is not None or matching_positions is not None
    if admitted and (expected.row_count > limits.max_rows or expected.logical_byte_count > limits.max_logical_bytes):
        raise ValueError("canonical_archive_limit_exceeded: admitted manifest rows or logical bytes")
    require_configured_archive_mount(path, require_writable=False)
    if expected.byte_count > limits.max_file_bytes:
        raise ValueError("canonical_archive_file_limit_exceeded: manifest bytes")
    arguments = dict(expected=expected, limits=limits, decoded_page=decoded_page, ordinals=ordinals,
                     matching_positions=matching_positions)
    if cache is not None:
        corrupt = False
        with cache.open_copy(path, digest=expected.object_sha256, size=expected.byte_count) as handle:
            if handle is not None:
                try:
                    return _read_canonical_fact_archive_handle(handle, tier="cache", **arguments)
                except ArchiveChecksumMismatch:
                    corrupt = True
        if corrupt:
            logging.getLogger(__name__).warning(
                "history_cache_corrupt | object_sha256=%s fallback=authoritative_archive", expected.object_sha256)
            cache.invalidate(expected.object_sha256)
    path = Path(path)
    if not admitted:
        with path.open("rb") as handle:
            return _read_canonical_fact_archive_handle(handle, tier="source", **arguments)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(path, flags), "rb") as handle:
        return _read_canonical_fact_archive_handle(handle, tier="source", source_path=path, **arguments)


def _assert_exact_file_unchanged(source, path, stamp, manifest_id):
    try:
        if _stamp(os.fstat(source.fileno())) == stamp and (path is None or _stamp(path.lstat()) == stamp):
            return
    except FileNotFoundError:
        pass
    raise ArchiveChecksumMismatch(f"canonical_archive_changed_during_read: manifest_id={manifest_id}")


def _selected_groups(metadata, ordinals, *, limits):
    """Check all authenticated chunks and resolve selected absolute ordinals."""
    if not 1 <= metadata.num_row_groups <= metadata.num_rows:
        raise RuntimeError("canonical_archive_incomplete: invalid footer row group count")
    pending = iter(ordinals or ())
    position = next(pending, None)
    offset = uncompressed_bytes = 0
    selected = {}
    for index in range(metadata.num_row_groups):
        execution_checkpoint()
        group = metadata.row_group(index)
        if not 1 <= group.num_rows <= metadata.num_rows - offset:
            raise RuntimeError("canonical_archive_incomplete: invalid footer row group extent")
        for column in range(group.num_columns):
            info = group.column(column)
            if info.compression != "ZSTD" or info.file_path or info.total_uncompressed_size < 0:
                raise RuntimeError("canonical_archive_compression_mismatch: expected inline ZSTD")
            uncompressed_bytes += info.total_uncompressed_size
        end = offset + group.num_rows
        if position is not None and position < end:
            members = []
            while position is not None and position < end:
                members.append((position, position - offset))
                position = next(pending, None)
            selected[index] = group.num_rows, members
        offset = end
    if uncompressed_bytes > limits.max_file_bytes:
        raise ValueError("canonical_archive_limit_exceeded: footer uncompressed bytes")
    if offset != metadata.num_rows or position is not None:
        raise RuntimeError("canonical_archive_incomplete: footer row group coverage")
    return selected


def _decode_json(row):
    for name in _JSON_COLUMNS:
        row[name] = json.loads(row[name])
    return row


def _matching_ordinals(parquet, matching_positions, *, limits):
    ordinals = []
    offset = 0
    for batch in parquet.iter_batches(batch_size=limits.row_group_size, columns=_RANGE_COLUMNS, use_threads=False):
        if offset + batch.num_rows > parquet.metadata.num_rows:
            raise RuntimeError("canonical_archive_incomplete: footer/projection row count disagreement")
        if any(column.null_count for column in batch.columns):
            raise RuntimeError("canonical_archive_incomplete: null range projection")
        consume_execution_resource("archive_predicate_rows", batch.num_rows)
        execution_checkpoint()
        previous = -1
        for position in matching_positions(batch):
            if type(position) is not int or not previous < position < batch.num_rows:
                raise RuntimeError("canonical_archive_range_positions_invalid")
            ordinals.append(offset + position)
            previous = position
        offset += batch.num_rows
    if offset != parquet.metadata.num_rows:
        raise RuntimeError("canonical_archive_incomplete: footer/projection row count disagreement")
    return ordinals


def _read_admitted_groups(parquet, selected, *, expected):
    result = {}
    logical_bytes = 0
    bounds = {item.series_id: item for item in expected.series}
    for group_index, (count, members) in selected.items():
        consume_execution_resource("archive_decoded_rows", count)
        consume_execution_resource("archive_decoded_row_groups", 1)
        group = parquet.read_row_group(group_index, use_threads=False)
        if group.num_rows != count:
            raise RuntimeError("canonical_archive_incomplete: footer/content row group disagreement")
        local_ordinals = pa.array([index for _, index in members], type=pa.int64())
        for (ordinal, _), raw in zip(members, group.take(local_ordinals).to_pylist(), strict=True):
            execution_checkpoint()
            row = _decode_json(raw)
            record = _validated_archive_record(row)
            series = bounds.get(record.series_id)
            fact = record.fact
            cursor = (record.market_commit_seq, record.fact_version_id)
            if (series is None or not expected.first_cursor <= cursor <= expected.last_cursor
                    or (ordinal == 0 and cursor != expected.first_cursor)
                    or (ordinal == expected.row_count - 1 and cursor != expected.last_cursor)
                    or record.source_id not in series.source_ids
                    or (fact.payload_schema_id, fact.payload_contract_hash) not in series.payload_contracts
                    or not series.first_observation_at <= fact.observation_time <= series.last_observation_at
                    or not series.first_known_at <= fact.known_at <= series.last_known_at
                    or not series.first_accepted_at <= fact.accepted_at <= series.last_accepted_at):
                raise RuntimeError(f"canonical_archive_manifest_mismatch: manifest_id={expected.manifest_id}")
            logical_bytes += len(_json_bytes(row))
            if logical_bytes > expected.logical_byte_count:
                raise RuntimeError(f"canonical_archive_manifest_mismatch: manifest_id={expected.manifest_id}")
            result[ordinal] = row
    return result


def _read_canonical_fact_archive_handle(source, *, expected, limits, tier, decoded_page=None,
                                        ordinals=None, matching_positions=None, source_path=None):
    stamp = None
    if ordinals is not None or matching_positions is not None:
        observed = os.fstat(source.fileno())
        if not S_ISREG(observed.st_mode) or observed.st_size != expected.byte_count:
            raise ArchiveChecksumMismatch(f"canonical_archive_checksum_mismatch: manifest_id={expected.manifest_id}")
        stamp = _stamp(observed)
        _assert_exact_file_unchanged(source, source_path, stamp, expected.manifest_id)
    with _MeteredArchiveFile(source, tier=tier) as handle:
        with measure_execution_stage("archive_verify"):
            sha256, byte_count = _hash_handle(handle, max_bytes=limits.max_file_bytes)
        if sha256 != expected.object_sha256 or byte_count != expected.byte_count:
            raise ArchiveChecksumMismatch(f"canonical_archive_checksum_mismatch: manifest_id={expected.manifest_id}")
        if stamp is not None:
            _assert_exact_file_unchanged(source, source_path, stamp, expected.manifest_id)
        if decoded_page is not None:
            retained = decoded_page._reuse(expected, limits)
            if retained is not None:
                return retained
        handle.seek(0)
        parquet = pq.ParquetFile(handle, page_checksum_verification=True,
                                 thrift_string_size_limit=8 * 1024**2, thrift_container_size_limit=100_000)
        if not parquet.schema_arrow.equals(_schema(), check_metadata=True):
            raise RuntimeError(f"canonical_archive_schema_mismatch: manifest_id={expected.manifest_id}")
        metadata = parquet.metadata
        if metadata.num_rows > limits.max_rows:
            raise ValueError("canonical_archive_limit_exceeded: footer row count")
        if stamp is not None and metadata.num_rows != expected.row_count:
            raise RuntimeError(f"canonical_archive_manifest_mismatch: manifest_id={expected.manifest_id}")
        selected = _selected_groups(metadata, ordinals, limits=limits)
        with measure_execution_stage("archive_decode"):
            if matching_positions is not None:
                with measure_execution_stage("archive_predicate"):
                    ordinals = _matching_ordinals(parquet, matching_positions, limits=limits)
                if ordinals:
                    selected = _selected_groups(metadata, ordinals, limits=limits)
            if (ordinals is not None and len(ordinals) < metadata.num_rows
                    and all(count <= limits.row_group_size for count, _ in selected.values())):
                with measure_execution_stage("archive_selected_payload"):
                    result = _read_admitted_groups(parquet, selected, expected=expected)
                _assert_exact_file_unchanged(source, source_path, stamp, expected.manifest_id)
                return result
            contents, rows = _Contents(limits), []
            for batch in parquet.iter_batches(batch_size=limits.row_group_size):
                consume_execution_resource("archive_decoded_rows", batch.num_rows)
                for row in batch.to_pylist():
                    _decode_json(row)
                    contents.add(row)
                    rows.append(row)
        if contents.rows != metadata.num_rows:
            raise RuntimeError("canonical_archive_incomplete: footer/content row count disagreement")
        if stamp is not None:
            _assert_exact_file_unchanged(source, source_path, stamp, expected.manifest_id)
    observed = contents.manifest(sha256=sha256, byte_count=byte_count)
    if observed != expected:
        raise RuntimeError(f"canonical_archive_manifest_mismatch: manifest_id={expected.manifest_id}")
    rows = tuple(rows)
    if ordinals is not None:
        return {index: rows[index] for index in ordinals}
    if decoded_page is not None:
        decoded_page._remember(expected, limits, rows)
    return rows


def encode_canonical_fact_archive(
    rows: Iterable[Mapping[str, Any]], *, temporary_directory: Path,
    limits: FactArchiveLimits = FactArchiveLimits(),
    phase_observer: Callable[[str, float, str], None] | None = None,
    decoded_page: CanonicalArchiveReadPage | None = None,
) -> EncodedFactArchive:
    """Encode a strictly commit/ID-ordered bounded page; never truncate a page."""
    root = Path(temporary_directory)
    require_configured_archive_mount(root)
    root.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix="canonical-facts.", suffix=".parquet", dir=root)
    os.close(descriptor)
    path = Path(name)
    contents = _Contents(limits)
    try:
        def encode_compress():
            with pq.ParquetWriter(path, _schema(), compression="zstd", use_dictionary=True,
                                  write_statistics=True, data_page_version="2.0", write_page_checksum=True) as writer:
                batch = []
                for source in rows:
                    row = dict(source)
                    contents.add(row)
                    for column in _JSON_COLUMNS:
                        row[column] = _json_bytes(row[column]).decode("utf-8")
                    batch.append(row)
                    if len(batch) == limits.row_group_size:
                        writer.write_table(pa.Table.from_pylist(batch, schema=_schema()))
                        batch.clear()
                if batch:
                    writer.write_table(pa.Table.from_pylist(batch, schema=_schema()))
        _timed_archive_phase(phase_observer, "archive_encode_compress", encode_compress)

        def fsync_hash():
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
                sha256, byte_count = _hash_handle(handle, max_bytes=limits.max_file_bytes)
            return contents.manifest(sha256=sha256, byte_count=byte_count)
        manifest = _timed_archive_phase(phase_observer, "archive_staging_fsync_hash", fsync_hash)
        _timed_archive_phase(
            phase_observer, "archive_staging_readback",
            lambda: read_canonical_fact_archive(
                path, expected=manifest, limits=limits, decoded_page=decoded_page,
            ),
        )
        return EncodedFactArchive(path=path, manifest=manifest)
    except BaseException:
        path.unlink(missing_ok=True)
        raise

def publish_canonical_fact_archive(
    rows: Iterable[Mapping[str, Any]], *, object_store: RawArchiveObjectStore,
    temporary_directory: Path, limits: FactArchiveLimits = FactArchiveLimits(),
    phase_observer: Callable[[str, float, str], None] | None = None,
    decoded_page: CanonicalArchiveReadPage | None = None,
) -> FactArchiveManifest:
    """Acknowledge verified bytes only; the caller must commit the manifest before retention."""
    encoded = encode_canonical_fact_archive(
        rows, temporary_directory=temporary_directory, limits=limits,
        phase_observer=phase_observer, decoded_page=decoded_page,
    )
    try:
        manifest = encoded.manifest
        acknowledgement = _timed_archive_phase(
            phase_observer, "archive_object_publish",
            lambda: object_store.put_verified(
                object_key=manifest.object_key, source_path=encoded.path,
                expected_sha256=manifest.object_sha256,
            ),
        )
        if (acknowledgement.object_key, acknowledgement.sha256, acknowledgement.byte_count) != (
            manifest.object_key, manifest.object_sha256, manifest.byte_count,
        ):
            raise RuntimeError("canonical_archive_acknowledgement_mismatch")
        _timed_archive_phase(
            phase_observer, "archive_publish_readback",
            lambda: read_canonical_fact_archive(
                object_store.local_path(manifest.object_key), expected=manifest, limits=limits,
                decoded_page=decoded_page,
            ),
        )
        return manifest
    finally:
        encoded.path.unlink(missing_ok=True)
