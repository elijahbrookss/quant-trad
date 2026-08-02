"""Durable local spool and immutable raw-object archive for market streams.

The spool is block-storage/WAL behavior. The archive store is object-storage
behavior with immutable keys and upload verification. A caller must still
commit the returned manifest and record mappings to the canonical database
before any evidence is considered archive-complete or dataset-eligible.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from .structure import RawStreamRecord, build_spool_segment_id


SPOOL_SCHEMA_VERSION = "market.raw_spool.v1"
RAW_ARCHIVE_SCHEMA_VERSION = "market.raw_archive.v1"
RAW_ARCHIVE_FORMAT = "parquet"
RAW_ARCHIVE_COMPRESSION = "zstd"


class SpoolBackpressureError(RuntimeError):
    """Raised before a frame write would violate the configured spool bound."""


@dataclass(frozen=True)
class SpoolRecoveryEvidence:
    spool_segment_id: str
    path: Path
    recovered_record_count: int
    truncated_tail_bytes: int
    last_receive_ordinal: int


@dataclass(frozen=True)
class EncodedRawArchive:
    spool_segment_id: str
    path: Path
    sha256: str
    content_fingerprint: str
    byte_count: int
    record_count: int
    first_receive_ordinal: int
    last_receive_ordinal: int
    first_received_at: datetime
    last_received_at: datetime


@dataclass(frozen=True)
class ArchiveObjectAcknowledgement:
    object_key: str
    object_uri: str
    sha256: str
    byte_count: int
    acknowledged_at: datetime
    reused_existing: bool = False


class RawArchiveObjectStore(Protocol):
    def put_verified(
        self, *, object_key: str, source_path: Path, expected_sha256: str
    ) -> ArchiveObjectAcknowledgement:
        ...

    def local_path(self, object_key: str) -> Path:
        ...


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_component(value: str) -> str:
    raw = str(value or "").strip()
    safe = "".join(char if char.isalnum() or char in "-_." else "_" for char in raw)
    if not safe or safe in {".", ".."}:
        raise ValueError("market_archive_invalid: unsafe path component")
    return safe


def spool_backlog_bytes(root: Path, *, definition_id: str | None = None) -> int:
    root_path = Path(root)
    if definition_id is not None:
        root_path = root_path / _safe_component(definition_id)
    if not root_path.exists():
        return 0
    return sum(
        path.stat().st_size
        for path in root_path.rglob("*")
        if path.is_file() and path.suffix in {".open", ".sealed"}
    )


def require_spool_capacity(
    *,
    root: Path,
    max_backlog_bytes: int,
    next_frame_bytes: int,
    definition_id: str | None = None,
) -> None:
    limit = int(max_backlog_bytes)
    incoming = int(next_frame_bytes)
    if limit <= 0 or incoming < 0:
        raise ValueError("market_spool_invalid: backlog bound and frame size are invalid")
    current = spool_backlog_bytes(root, definition_id=definition_id)
    if current + incoming > limit:
        raise SpoolBackpressureError(
            "market_spool_backpressure: frame would exceed bounded local backlog "
            f"current_bytes={current} next_frame_bytes={incoming} max_backlog_bytes={limit}"
        )


class DurableRawSpoolSegment:
    """Append-only fsynced JSONL WAL segment with crash-tail recovery."""

    def __init__(
        self,
        *,
        root: Path,
        definition_id: str,
        session_id: str,
        connection_epoch: int,
        segment_ordinal: int = 0,
        create: bool = True,
    ) -> None:
        self.root = Path(root)
        self.definition_id = str(definition_id or "").strip()
        self.session_id = str(session_id or "").strip()
        self.connection_epoch = int(connection_epoch)
        self.segment_ordinal = int(segment_ordinal)
        self.spool_segment_id = build_spool_segment_id(
            definition_id=self.definition_id,
            session_id=self.session_id,
            connection_epoch=self.connection_epoch,
            segment_ordinal=self.segment_ordinal,
        )
        directory = (
            self.root
            / _safe_component(self.definition_id)
            / _safe_component(self.session_id)
            / f"epoch={self.connection_epoch}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        self.open_path = directory / f"{self.spool_segment_id}.open"
        self.sealed_path = directory / f"{self.spool_segment_id}.sealed"
        self.ack_path = directory / f"{self.spool_segment_id}.ack.json"
        self._descriptor: int | None = None
        self._record_count = 0
        self._last_receive_ordinal = 0
        self.recovery_evidence: SpoolRecoveryEvidence | None = None
        if self.sealed_path.exists() and create:
            raise RuntimeError(
                f"market_spool_sealed: segment={self.spool_segment_id}"
            )
        if create:
            self._open_or_create()

    @classmethod
    def from_path(cls, path: Path) -> "DurableRawSpoolSegment":
        header, _rows, _tail = _read_spool_file(Path(path), repair_tail=False)
        segment = cls(
            root=_infer_spool_root(Path(path), header),
            definition_id=str(header["definition_id"]),
            session_id=str(header["session_id"]),
            connection_epoch=int(header["connection_epoch"]),
            segment_ordinal=int(header["segment_ordinal"]),
            create=False,
        )
        return segment

    def _open_or_create(self) -> None:
        existed = self.open_path.exists()
        if existed:
            evidence = self.recover_open_tail()
            self.recovery_evidence = evidence
            self._record_count = evidence.recovered_record_count
            self._last_receive_ordinal = evidence.last_receive_ordinal
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        self._descriptor = os.open(self.open_path, flags, 0o600)
        if not existed:
            header = {
                "record_kind": "header",
                "schema_version": SPOOL_SCHEMA_VERSION,
                "spool_segment_id": self.spool_segment_id,
                "definition_id": self.definition_id,
                "session_id": self.session_id,
                "connection_epoch": self.connection_epoch,
                "segment_ordinal": self.segment_ordinal,
                "created_at": datetime.now(UTC).isoformat(),
            }
            self._write_line(_canonical_json_bytes(header) + b"\n")
            _fsync_directory(self.open_path.parent)

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def current_bytes(self) -> int:
        path = self.open_path if self.open_path.exists() else self.sealed_path
        return path.stat().st_size if path.exists() else 0

    def _write_line(self, payload: bytes) -> None:
        if self._descriptor is None:
            raise RuntimeError("market_spool_closed: append rejected")
        offset = 0
        while offset < len(payload):
            written = os.write(self._descriptor, payload[offset:])
            if written <= 0:
                raise OSError("market_spool_write_failed: zero-byte write")
            offset += written
        os.fsync(self._descriptor)

    def append(self, record: RawStreamRecord) -> None:
        if record.spool_segment_id != self.spool_segment_id:
            raise ValueError("market_spool_invalid: record belongs to another segment")
        if (
            record.definition_id != self.definition_id
            or record.session_id != self.session_id
            or record.connection_epoch != self.connection_epoch
        ):
            raise ValueError("market_spool_invalid: record source identity mismatch")
        if record.receive_ordinal <= self._last_receive_ordinal:
            raise ValueError(
                "market_spool_invalid: receive ordinals must be strictly increasing"
            )
        payload = {
            "record_kind": "raw_frame",
            "schema_version": SPOOL_SCHEMA_VERSION,
            "raw_record_id": record.raw_record_id,
            "spool_segment_id": record.spool_segment_id,
            "definition_id": record.definition_id,
            "session_id": record.session_id,
            "connection_epoch": record.connection_epoch,
            "receive_ordinal": record.receive_ordinal,
            "received_at": record.received_at.isoformat(),
            "provider": record.provider,
            "venue": record.venue,
            "provider_product_id": record.provider_product_id,
            "requested_channel": record.requested_channel,
            "observed_channel": record.observed_channel,
            "raw_frame_base64": base64.b64encode(record.raw_frame).decode("ascii"),
            "raw_frame_sha256": record.raw_frame_sha256,
        }
        self._write_line(_canonical_json_bytes(payload) + b"\n")
        self._record_count += 1
        self._last_receive_ordinal = record.receive_ordinal

    def close(self) -> None:
        if self._descriptor is None:
            return
        os.fsync(self._descriptor)
        os.close(self._descriptor)
        self._descriptor = None

    def seal(self) -> Path:
        if self._record_count <= 0:
            raise ValueError("market_spool_empty: cannot seal an empty segment")
        self.close()
        if self.sealed_path.exists() and not self.open_path.exists():
            return self.sealed_path
        if self.sealed_path.exists():
            if _sha256_file(self.sealed_path) != _sha256_file(self.open_path):
                raise RuntimeError("market_spool_conflict: sealed segment differs")
            return self.sealed_path
        os.replace(self.open_path, self.sealed_path)
        _fsync_directory(self.sealed_path.parent)
        return self.sealed_path

    def records(self) -> Iterator[RawStreamRecord]:
        path = self.sealed_path if self.sealed_path.exists() else self.open_path
        _header, rows, tail = _read_spool_file(path, repair_tail=False)
        if tail:
            raise RuntimeError("market_spool_corrupt: incomplete tail")
        for row in rows:
            yield _raw_record_from_spool_row(row)

    def recover_open_tail(self) -> SpoolRecoveryEvidence:
        if not self.open_path.exists():
            raise FileNotFoundError(self.open_path)
        _header, rows, truncated = _read_spool_file(
            self.open_path, repair_tail=True
        )
        return SpoolRecoveryEvidence(
            spool_segment_id=self.spool_segment_id,
            path=self.open_path,
            recovered_record_count=len(rows),
            truncated_tail_bytes=truncated,
            last_receive_ordinal=(int(rows[-1]["receive_ordinal"]) if rows else 0),
        )

    def mark_database_acknowledged(
        self, *, manifest_id: str, object_key: str, object_sha256: str
    ) -> None:
        """Write a disposable local projection only after DB mapping commit."""

        payload = {
            "schema_version": "market.raw_spool_ack.v1",
            "spool_segment_id": self.spool_segment_id,
            "manifest_id": str(manifest_id),
            "object_key": str(object_key),
            "object_sha256": str(object_sha256),
            "acknowledged_at": datetime.now(UTC).isoformat(),
        }
        temporary = self.ack_path.with_suffix(".partial")
        with temporary.open("wb") as handle:
            handle.write(_canonical_json_bytes(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.ack_path)
        _fsync_directory(self.ack_path.parent)

    def discard_acknowledged_spool(self) -> None:
        """Remove local WAL bytes only after the DB-commit acknowledgement exists."""

        if not self.ack_path.exists():
            raise RuntimeError(
                "market_spool_cleanup_forbidden: database acknowledgement is missing"
            )
        if self.open_path.exists():
            raise RuntimeError(
                "market_spool_cleanup_forbidden: open segment cannot be discarded"
            )
        if self.sealed_path.exists():
            self.sealed_path.unlink()
            _fsync_directory(self.sealed_path.parent)


def _infer_spool_root(path: Path, header: Mapping[str, Any]) -> Path:
    expected_tail = Path(
        _safe_component(str(header["definition_id"])),
        _safe_component(str(header["session_id"])),
        f"epoch={int(header['connection_epoch'])}",
        path.name,
    )
    root = path
    for _part in expected_tail.parts:
        root = root.parent
    return root


def _read_spool_file(
    path: Path, *, repair_tail: bool
) -> tuple[Mapping[str, Any], list[Mapping[str, Any]], int]:
    raw = Path(path).read_bytes()
    truncated = 0
    if raw and not raw.endswith(b"\n"):
        final_newline = raw.rfind(b"\n")
        if final_newline < 0:
            raise RuntimeError("market_spool_corrupt: missing complete header")
        if not repair_tail:
            return {}, [], len(raw) - final_newline - 1
        truncated = len(raw) - final_newline - 1
        raw = raw[: final_newline + 1]
        with Path(path).open("r+b") as handle:
            handle.truncate(len(raw))
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(Path(path).parent)
    parsed: list[Mapping[str, Any]] = []
    for ordinal, line in enumerate(raw.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"market_spool_corrupt: invalid JSON line={ordinal + 1}"
            ) from exc
        if not isinstance(value, Mapping):
            raise RuntimeError("market_spool_corrupt: line must be an object")
        parsed.append(value)
    if not parsed or parsed[0].get("record_kind") != "header":
        raise RuntimeError("market_spool_corrupt: header is missing")
    header = parsed[0]
    if header.get("schema_version") != SPOOL_SCHEMA_VERSION:
        raise RuntimeError("market_spool_corrupt: schema version mismatch")
    rows = parsed[1:]
    previous_ordinal = 0
    for row in rows:
        if row.get("record_kind") != "raw_frame":
            raise RuntimeError("market_spool_corrupt: unexpected record kind")
        record = _raw_record_from_spool_row(row)
        if record.spool_segment_id != header.get("spool_segment_id"):
            raise RuntimeError("market_spool_corrupt: segment identity mismatch")
        if record.receive_ordinal <= previous_ordinal:
            raise RuntimeError("market_spool_corrupt: receive ordinal is not increasing")
        previous_ordinal = record.receive_ordinal
    return header, rows, truncated


def _raw_record_from_spool_row(row: Mapping[str, Any]) -> RawStreamRecord:
    try:
        frame = base64.b64decode(str(row["raw_frame_base64"]), validate=True)
    except Exception as exc:  # noqa: BLE001 - normalized as corrupt evidence
        raise RuntimeError("market_spool_corrupt: invalid frame encoding") from exc
    return RawStreamRecord(
        raw_record_id=str(row["raw_record_id"]),
        spool_segment_id=str(row["spool_segment_id"]),
        definition_id=str(row["definition_id"]),
        session_id=str(row["session_id"]),
        connection_epoch=int(row["connection_epoch"]),
        receive_ordinal=int(row["receive_ordinal"]),
        received_at=str(row["received_at"]),
        provider=str(row["provider"]),
        venue=str(row["venue"]),
        provider_product_id=str(row["provider_product_id"]),
        requested_channel=str(row["requested_channel"]),
        observed_channel=str(row["observed_channel"]),
        raw_frame=frame,
        raw_frame_sha256=str(row["raw_frame_sha256"]),
    )


class FilesystemRawArchiveObjectStore:
    """Local immutable object-store semantics for implementation and tests."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def local_path(self, object_key: str) -> Path:
        parts = Path(str(object_key or "")).parts
        if not parts or any(part in {"", ".", ".."} for part in parts):
            raise ValueError("market_archive_invalid: object key is unsafe")
        target = (self.root / Path(*parts)).resolve()
        if self.root not in target.parents:
            raise ValueError("market_archive_invalid: object key escapes root")
        return target

    def put_verified(
        self, *, object_key: str, source_path: Path, expected_sha256: str
    ) -> ArchiveObjectAcknowledgement:
        source = Path(source_path)
        expected = str(expected_sha256 or "").strip().lower()
        if _sha256_file(source) != expected:
            raise ValueError("market_archive_upload_invalid: source checksum mismatch")
        destination = self.local_path(object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            existing_hash = _sha256_file(destination)
            if existing_hash != expected:
                raise RuntimeError(
                    "market_archive_object_conflict: immutable key has different bytes"
                )
            return ArchiveObjectAcknowledgement(
                object_key=str(object_key),
                object_uri=f"market-archive://{object_key}",
                sha256=expected,
                byte_count=destination.stat().st_size,
                acknowledged_at=datetime.now(UTC),
                reused_existing=True,
            )
        temporary = destination.with_name(
            f".{destination.name}.{os.getpid()}.partial"
        )
        try:
            with source.open("rb") as source_handle, temporary.open("xb") as target:
                shutil.copyfileobj(source_handle, target, length=1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            if _sha256_file(temporary) != expected:
                raise RuntimeError("market_archive_upload_invalid: copied checksum mismatch")
            os.replace(temporary, destination)
            _fsync_directory(destination.parent)
        finally:
            if temporary.exists():
                temporary.unlink()
        if _sha256_file(destination) != expected:
            raise RuntimeError("market_archive_upload_invalid: acknowledgement checksum mismatch")
        return ArchiveObjectAcknowledgement(
            object_key=str(object_key),
            object_uri=f"market-archive://{object_key}",
            sha256=expected,
            byte_count=destination.stat().st_size,
            acknowledged_at=datetime.now(UTC),
        )


def archive_object_key(
    *, record: RawStreamRecord, spool_segment_id: str, captured_date: str | None = None
) -> str:
    date = captured_date or record.received_at.date().isoformat()
    return "/".join(
        (
            "raw",
            f"provider={_safe_component(record.provider.lower())}",
            f"venue={_safe_component(record.venue.lower())}",
            f"channel={_safe_component(record.requested_channel)}",
            f"product={_safe_component(record.provider_product_id)}",
            f"date={_safe_component(date)}",
            f"session={_safe_component(record.session_id)}",
            f"epoch={record.connection_epoch}",
            f"{_safe_component(spool_segment_id)}.parquet",
        )
    )


def encode_spool_segment_to_parquet(
    segment: DurableRawSpoolSegment, *, temporary_directory: Path | None = None
) -> EncodedRawArchive:
    """Encode a sealed segment deterministically as typed Parquet/ZSTD."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - project dependency guard
        raise RuntimeError("market_archive_requires_pyarrow") from exc
    if not segment.sealed_path.exists():
        raise ValueError("market_archive_invalid: spool segment must be sealed")
    records = list(segment.records())
    if not records:
        raise ValueError("market_archive_invalid: no records")
    schema = pa.schema(
        [
            pa.field("raw_record_id", pa.string(), nullable=False),
            pa.field("spool_segment_id", pa.string(), nullable=False),
            pa.field("definition_id", pa.string(), nullable=False),
            pa.field("session_id", pa.string(), nullable=False),
            pa.field("connection_epoch", pa.int64(), nullable=False),
            pa.field("receive_ordinal", pa.int64(), nullable=False),
            pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("provider", pa.string(), nullable=False),
            pa.field("venue", pa.string(), nullable=False),
            pa.field("provider_product_id", pa.string(), nullable=False),
            pa.field("requested_channel", pa.string(), nullable=False),
            pa.field("observed_channel", pa.string(), nullable=False),
            pa.field("raw_frame", pa.binary(), nullable=False),
            pa.field("raw_frame_sha256", pa.string(), nullable=False),
        ],
        metadata={
            b"schema_version": RAW_ARCHIVE_SCHEMA_VERSION.encode("ascii"),
            b"spool_segment_id": segment.spool_segment_id.encode("ascii"),
        },
    )
    table = pa.Table.from_pylist(
        [
            {
                "raw_record_id": record.raw_record_id,
                "spool_segment_id": record.spool_segment_id,
                "definition_id": record.definition_id,
                "session_id": record.session_id,
                "connection_epoch": record.connection_epoch,
                "receive_ordinal": record.receive_ordinal,
                "received_at": record.received_at,
                "provider": record.provider,
                "venue": record.venue,
                "provider_product_id": record.provider_product_id,
                "requested_channel": record.requested_channel,
                "observed_channel": record.observed_channel,
                "raw_frame": record.raw_frame,
                "raw_frame_sha256": record.raw_frame_sha256,
            }
            for record in records
        ],
        schema=schema,
    )
    temporary_root = Path(temporary_directory) if temporary_directory else Path(tempfile.gettempdir())
    temporary_root.mkdir(parents=True, exist_ok=True)
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f"{segment.spool_segment_id}.", suffix=".parquet", dir=temporary_root
    )
    os.close(descriptor)
    path = Path(raw_path)
    try:
        pq.write_table(
            table,
            path,
            compression=RAW_ARCHIVE_COMPRESSION,
            use_dictionary=True,
            write_statistics=True,
            data_page_version="2.0",
        )
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        replayed = read_raw_archive_parquet(path)
        if [item.raw_record_id for item in replayed] != [
            item.raw_record_id for item in records
        ]:
            raise RuntimeError("market_archive_encode_invalid: replay identity mismatch")
    except Exception:
        if path.exists():
            path.unlink()
        raise
    content_fingerprint = hashlib.sha256(
        _canonical_json_bytes(
            {
                "schema_version": "market.raw_archive_content.v1",
                "raw_record_ids": [record.raw_record_id for record in records],
                "raw_frame_sha256": [record.raw_frame_sha256 for record in records],
            }
        )
    ).hexdigest()
    return EncodedRawArchive(
        spool_segment_id=segment.spool_segment_id,
        path=path,
        sha256=_sha256_file(path),
        content_fingerprint=content_fingerprint,
        byte_count=path.stat().st_size,
        record_count=len(records),
        first_receive_ordinal=records[0].receive_ordinal,
        last_receive_ordinal=records[-1].receive_ordinal,
        first_received_at=records[0].received_at,
        last_received_at=records[-1].received_at,
    )


def read_raw_archive_parquet(path: Path) -> list[RawStreamRecord]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("market_archive_requires_pyarrow") from exc
    # Read one physical file. Dataset discovery would reinterpret object-layout
    # components such as ``provider=coinbase`` as Hive partition columns.
    table = pq.ParquetFile(Path(path)).read()
    metadata = table.schema.metadata or {}
    if metadata.get(b"schema_version") != RAW_ARCHIVE_SCHEMA_VERSION.encode("ascii"):
        raise RuntimeError("market_archive_replay_invalid: schema version mismatch")
    rows: list[RawStreamRecord] = []
    for value in table.to_pylist():
        rows.append(
            RawStreamRecord(
                raw_record_id=value["raw_record_id"],
                spool_segment_id=value["spool_segment_id"],
                definition_id=value["definition_id"],
                session_id=value["session_id"],
                connection_epoch=value["connection_epoch"],
                receive_ordinal=value["receive_ordinal"],
                received_at=value["received_at"],
                provider=value["provider"],
                venue=value["venue"],
                provider_product_id=value["provider_product_id"],
                requested_channel=value["requested_channel"],
                observed_channel=value["observed_channel"],
                raw_frame=value["raw_frame"],
                raw_frame_sha256=value["raw_frame_sha256"],
            )
        )
    if rows != sorted(rows, key=lambda item: item.receive_ordinal):
        raise RuntimeError("market_archive_replay_invalid: record order mismatch")
    return rows


def publish_spool_archive(
    segment: DurableRawSpoolSegment,
    *,
    object_store: RawArchiveObjectStore,
    temporary_directory: Path | None = None,
) -> tuple[EncodedRawArchive, ArchiveObjectAcknowledgement, tuple[RawStreamRecord, ...]]:
    """Encode, upload, verify, and return evidence awaiting DB manifest commit."""

    encoded = encode_spool_segment_to_parquet(
        segment, temporary_directory=temporary_directory
    )
    records = tuple(segment.records())
    key = archive_object_key(
        record=records[0], spool_segment_id=segment.spool_segment_id
    )
    try:
        acknowledgement = object_store.put_verified(
            object_key=key,
            source_path=encoded.path,
            expected_sha256=encoded.sha256,
        )
    finally:
        if encoded.path.exists():
            encoded.path.unlink()
    replayed = read_raw_archive_parquet(object_store.local_path(key))
    if tuple(item.raw_record_id for item in replayed) != tuple(
        item.raw_record_id for item in records
    ):
        raise RuntimeError("market_archive_upload_invalid: object replay mismatch")
    return encoded, acknowledgement, records


def discover_spool_segments(root: Path) -> tuple[Path, ...]:
    root_path = Path(root)
    if not root_path.exists():
        return ()
    return tuple(
        sorted(
            (
                path
                for path in root_path.rglob("*")
                if path.is_file() and path.suffix in {".open", ".sealed"}
            ),
            key=str,
        )
    )


__all__ = [
    "ArchiveObjectAcknowledgement",
    "DurableRawSpoolSegment",
    "EncodedRawArchive",
    "FilesystemRawArchiveObjectStore",
    "RAW_ARCHIVE_COMPRESSION",
    "RAW_ARCHIVE_FORMAT",
    "RAW_ARCHIVE_SCHEMA_VERSION",
    "RawArchiveObjectStore",
    "SPOOL_SCHEMA_VERSION",
    "SpoolBackpressureError",
    "SpoolRecoveryEvidence",
    "archive_object_key",
    "discover_spool_segments",
    "encode_spool_segment_to_parquet",
    "publish_spool_archive",
    "read_raw_archive_parquet",
    "require_spool_capacity",
    "spool_backlog_bytes",
]
