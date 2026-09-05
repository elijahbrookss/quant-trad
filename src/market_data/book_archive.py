"""Immutable deterministic checkpoint objects for reconstructed Level 2 books."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from core.storage_mounts import require_configured_archive_mount
from .archive import ArchiveObjectAcknowledgement, RawArchiveObjectStore
from .order_book import (
    BOOK_CHECKPOINT_SCHEMA_VERSION,
    BOOK_RECONSTRUCTION_VERSION,
    BookCheckpointFact,
    BookSide,
    BookSourcePosition,
    BookValidityIntervalVersion,
    BookValidityStatus,
    L2ProductContract,
    Level2BookReconstructor,
    checkpoint_canonical_rows,
    book_checkpoint_content_fingerprint,
)
from .structure import OrderingAssurance, ProviderSizeUnit


BOOK_CHECKPOINT_FORMAT = "parquet"
BOOK_CHECKPOINT_COMPRESSION = "zstd"


@dataclass(frozen=True)
class BookCheckpointReadLimits:
    max_rows: int = 1_000_000
    max_file_bytes: int = 1024**3
    max_logical_bytes: int = 2 * 1024**3
    max_row_group_bytes: int = 256 * 1024**2
    batch_rows: int = 128
    max_decimal_chars: int = 256

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"market_book_checkpoint_read_limit_invalid: field={name}")


def _checkpoint_schema(metadata=None):
    import pyarrow as pa
    return pa.schema([
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("checkpoint_id", pa.string(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("level_ordinal", pa.int64(), nullable=False),
        pa.field("price", pa.string(), nullable=False),
        pa.field("quantity", pa.string(), nullable=False),
        pa.field("provider_size_unit", pa.string(), nullable=False),
    ], metadata=metadata)


@dataclass(frozen=True)
class EncodedBookCheckpoint:
    checkpoint_id: str
    path: Path
    sha256: str
    content_fingerprint: str
    byte_count: int
    level_count: int


def _sha256_file(path: Path, *, max_bytes=None, check_budget=None) -> str:
    digest = hashlib.sha256()
    count = 0
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if check_budget is not None:
                check_budget()
            count += len(chunk)
            if max_bytes is not None and count > max_bytes:
                raise RuntimeError("market_book_checkpoint_read_file_budget_exceeded")
            digest.update(chunk)
    return digest.hexdigest()


def checkpoint_object_key(checkpoint: BookCheckpointFact) -> str:
    return "/".join(
        (
            "checkpoints",
            f"series={checkpoint.series_id}",
            f"date={checkpoint.effective_at.date().isoformat()}",
            f"{checkpoint.checkpoint_id}.parquet",
        )
    )


def encode_book_checkpoint_parquet(
    checkpoint: BookCheckpointFact, *, temporary_directory: Path | None = None
) -> EncodedBookCheckpoint:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("market_book_checkpoint_requires_pyarrow") from exc
    rows = checkpoint_canonical_rows(checkpoint)
    if not rows:
        raise ValueError("market_book_checkpoint_invalid: checkpoint has no levels")
    schema = _checkpoint_schema(
        metadata={
            b"schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION.encode("ascii"),
            b"checkpoint_id": checkpoint.checkpoint_id.encode("ascii"),
            b"state_hash": checkpoint.state_hash.encode("ascii"),
            b"content_fingerprint": checkpoint.content_fingerprint.encode("ascii"),
        },
    )
    table = pa.Table.from_pylist(list(rows), schema=schema)
    temporary_root = Path(temporary_directory or tempfile.gettempdir())
    require_configured_archive_mount(temporary_root)
    temporary_root.mkdir(parents=True, exist_ok=True)
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f"{checkpoint.checkpoint_id}.",
        suffix=".parquet",
        dir=temporary_root,
    )
    os.close(descriptor)
    path = Path(raw_path)
    try:
        pq.write_table(
            table,
            path,
            compression=BOOK_CHECKPOINT_COMPRESSION,
            use_dictionary=True,
            write_statistics=True,
            data_page_version="2.0",
        )
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        replay = read_book_checkpoint_parquet(path)
        if replay != rows:
            raise RuntimeError("market_book_checkpoint_encode_invalid: replay differs")
    except Exception:
        if path.exists():
            path.unlink()
        raise
    return EncodedBookCheckpoint(
        checkpoint_id=checkpoint.checkpoint_id,
        path=path,
        sha256=_sha256_file(path),
        content_fingerprint=checkpoint.content_fingerprint,
        byte_count=path.stat().st_size,
        level_count=len(rows),
    )


def read_book_checkpoint_parquet(path: Path, *, limits: BookCheckpointReadLimits = BookCheckpointReadLimits(),
                                 expected: Mapping[str, object] | None = None,
                                 check_budget=None) -> tuple[dict[str, object], ...]:
    """Bounded physical-file read and full level/fingerprint admission.

    Optional expected metadata is the immutable database checkpoint manifest.
    With expected metadata, the reader also checks CURRENT object SHA and file
    stability. Reconstruction still belongs to Level2BookReconstructor; a valid
    content fingerprint is not a state proof.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("market_book_checkpoint_requires_pyarrow") from exc
    def check():
        if check_budget is not None:
            check_budget()
    check()
    path = Path(path)
    before = path.stat()
    byte_count = before.st_size
    if byte_count > limits.max_file_bytes:
        raise RuntimeError("market_book_checkpoint_read_file_budget_exceeded")
    if expected is not None:
        if type(expected.get("byte_count")) is not int or expected["byte_count"] != byte_count:
            raise RuntimeError(f"market_book_checkpoint_manifest_mismatch: checkpoint_id={expected.get('id')} field=byte_count")
        expected_sha = expected.get("object_sha256")
        if not isinstance(expected_sha, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None:
            raise RuntimeError(f"market_book_checkpoint_manifest_mismatch: checkpoint_id={expected.get('id')} field=object_sha256")
        if _sha256_file(path, max_bytes=limits.max_file_bytes, check_budget=check_budget) != expected_sha:
            raise RuntimeError(f"market_book_checkpoint_manifest_mismatch: checkpoint_id={expected.get('id')} field=object_sha256")
    with pq.ParquetFile(path, page_checksum_verification=True,
                        thrift_string_size_limit=8 * 1024**2, thrift_container_size_limit=100_000) as parquet:
        if not parquet.schema_arrow.remove_metadata().equals(_checkpoint_schema()):
            raise RuntimeError("market_book_checkpoint_replay_invalid: physical schema mismatch")
        metadata = parquet.schema_arrow.metadata or {}
        try:
            identity = metadata[b"checkpoint_id"].decode("ascii")
            state_hash = metadata[b"state_hash"].decode("ascii")
            fingerprint = metadata[b"content_fingerprint"].decode("ascii")
        except (KeyError, UnicodeDecodeError) as exc:
            raise RuntimeError("market_book_checkpoint_replay_invalid: identity metadata missing") from exc
        if (metadata.get(b"schema_version") != BOOK_CHECKPOINT_SCHEMA_VERSION.encode("ascii") or not identity
                or re.fullmatch(r"[0-9a-f]{64}", state_hash) is None
                or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None):
            raise RuntimeError("market_book_checkpoint_replay_invalid: identity metadata mismatch")
        if not 0 < parquet.metadata.num_rows <= limits.max_rows:
            raise RuntimeError("market_book_checkpoint_read_row_budget_exceeded")
        declared_bytes = 0
        for index in range(parquet.metadata.num_row_groups):
            check()
            group = parquet.metadata.row_group(index)
            group_bytes = sum(group.column(column).total_uncompressed_size for column in range(group.num_columns))
            if any(group.column(column).compression != "ZSTD" for column in range(group.num_columns)):
                raise RuntimeError("market_book_checkpoint_replay_invalid: compression mismatch")
            if group_bytes > limits.max_row_group_bytes:
                raise RuntimeError("market_book_checkpoint_read_row_group_budget_exceeded")
            declared_bytes += group_bytes
            if declared_bytes > limits.max_logical_bytes:
                raise RuntimeError("market_book_checkpoint_read_logical_budget_exceeded")
        rows, counts = [], {BookSide.BID: 0, BookSide.ASK: 0}
        previous_side = previous_price = unit = None
        logical_bytes = 0
        for batch in parquet.iter_batches(batch_size=limits.batch_rows, use_threads=False):
            check()
            logical_bytes += batch.nbytes
            if batch.nbytes > limits.max_row_group_bytes or logical_bytes > limits.max_logical_bytes:
                raise RuntimeError("market_book_checkpoint_read_logical_budget_exceeded")
            for row in batch.to_pylist():
                check()
                if row["schema_version"] != BOOK_CHECKPOINT_SCHEMA_VERSION or row["checkpoint_id"] != identity:
                    raise RuntimeError("market_book_checkpoint_replay_invalid: row identity mismatch")
                try:
                    side = BookSide(row["side"])
                    row_unit = ProviderSizeUnit(row["provider_size_unit"])
                except (TypeError, ValueError) as exc:
                    raise RuntimeError("market_book_checkpoint_replay_invalid: malformed side or unit") from exc
                if unit is not None and row_unit is not unit:
                    raise RuntimeError("market_book_checkpoint_replay_invalid: mixed units")
                unit = row_unit
                # The writer emits fixed-point strings. Reject exponents before
                # Decimal formatting so a tiny '1e999999999' cannot allocate GBs.
                for name in ("price", "quantity"):
                    value = row[name]
                    if (not isinstance(value, str) or len(value) > limits.max_decimal_chars
                            or re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value) is None):
                        raise RuntimeError(f"market_book_checkpoint_replay_invalid: malformed decimal field={name}")
                price, quantity = Decimal(row["price"]), Decimal(row["quantity"])
                if not price.is_finite() or not quantity.is_finite() or price <= 0 or quantity <= 0:
                    raise RuntimeError("market_book_checkpoint_replay_invalid: nonpositive level")
                if (type(row["level_ordinal"]) is not int or row["level_ordinal"] != counts[side]
                        or (previous_side is side and price <= previous_price)
                        or (previous_side is BookSide.ASK and side is BookSide.BID)):
                    raise RuntimeError("market_book_checkpoint_replay_invalid: level order or ordinal mismatch")
                counts[side] += 1
                rows.append(row)
                if len(rows) > limits.max_rows:
                    raise RuntimeError("market_book_checkpoint_read_row_budget_exceeded")
                previous_side, previous_price = side, price
        if len(rows) != parquet.metadata.num_rows or not all(counts.values()):
            raise RuntimeError("market_book_checkpoint_replay_invalid: incomplete level coverage")
        def levels():
            for row in rows:
                check()
                yield row["side"], Decimal(row["price"]), Decimal(row["quantity"])
        actual_fingerprint = book_checkpoint_content_fingerprint(checkpoint_id=identity, state_hash=state_hash, levels=levels())
        if fingerprint != actual_fingerprint:
            raise RuntimeError("market_book_checkpoint_replay_invalid: content fingerprint mismatch")
        if expected is not None:
            actual = {"id": identity, "schema_version": BOOK_CHECKPOINT_SCHEMA_VERSION,
                "format": BOOK_CHECKPOINT_FORMAT, "compression": BOOK_CHECKPOINT_COMPRESSION,
                "state_hash": state_hash, "content_fingerprint": actual_fingerprint,
                "byte_count": byte_count, "level_count": len(rows), "bid_level_count": counts[BookSide.BID],
                "ask_level_count": counts[BookSide.ASK], "provider_size_unit": unit.value}
            for name, value in actual.items():
                if name not in expected or type(expected[name]) is not type(value) or expected[name] != value:
                    raise RuntimeError(f"market_book_checkpoint_manifest_mismatch: checkpoint_id={identity} field={name}")
        check()
        after = path.stat()
        if any(getattr(before, name) != getattr(after, name)
               for name in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")):
            raise RuntimeError(f"market_book_checkpoint_changed_during_verification: checkpoint_id={identity}")
        return tuple(rows)


def restore_book_checkpoint_parquet(
    path: Path, *, expected: Mapping, opening: Mapping, definition_id: str,
    contract: L2ProductContract, limits: BookCheckpointReadLimits = BookCheckpointReadLimits(),
    check_budget=None,
) -> tuple[BookCheckpointFact, Level2BookReconstructor]:
    """Restore verified bytes through the single book-state owner.

    Recovery and retention share the same immutable manifest/validity hydration.
    This proves checkpoint state consistency, not reconstruction of its raw
    prefix; callers must separately preserve and admit that source lineage.
    """
    context = f"checkpoint_id={expected.get('id')}"
    if (expected.get("reconstruction_version") != BOOK_RECONSTRUCTION_VERSION
            or opening.get("reconstruction_version") != BOOK_RECONSTRUCTION_VERSION):
        raise RuntimeError(f"market_book_checkpoint_reconstruction_version_mismatch: {context}")
    if (opening.get("interval_id") != expected.get("validity_interval_id")
            or opening.get("series_id") != expected.get("series_id")
            or opening.get("revision") != 1
            or opening.get("status") != BookValidityStatus.OPEN_VALID.value
            or opening.get("opening_session_id") != expected.get("session_id")):
        raise RuntimeError(f"market_book_checkpoint_validity_mismatch: {context}")

    def position(row, prefix=""):
        field = lambda name: row[prefix + name]
        sequence = row.get(prefix + ("sequence_num" if prefix else "provider_sequence_num"))
        return BookSourcePosition(
            definition_id=definition_id, session_id=str(field("session_id")),
            connection_epoch=int(field("connection_epoch")), provider_product_id=contract.provider_product_id,
            provider_sequence_num=int(sequence) if sequence is not None else None,
            receive_ordinal=int(field("receive_ordinal")), event_ordinal=int(field("event_ordinal")))

    opening_position = position(opening, "opening_")
    checkpoint_position = position(expected)
    if (opening_position.connection_epoch != checkpoint_position.connection_epoch
            or (opening_position.receive_ordinal, opening_position.event_ordinal)
            > (checkpoint_position.receive_ordinal, checkpoint_position.event_ordinal)):
        raise RuntimeError(f"market_book_checkpoint_validity_position_mismatch: {context}")
    validity = BookValidityIntervalVersion(
        version_id=str(opening["id"]), interval_id=str(opening["interval_id"]), revision=int(opening["revision"]),
        series_id=int(opening["series_id"]), status=BookValidityStatus(str(opening["status"])),
        ordering_assurance=OrderingAssurance(str(opening["ordering_assurance"])),
        opening_snapshot_id=str(opening["opening_snapshot_id"]), opening_position=opening_position,
        opening_effective_at=opening["opening_effective_at"], opening_known_at=opening["opening_known_at"],
        last_valid_position=position(opening, "last_"), last_valid_effective_at=opening["last_valid_effective_at"],
        last_state_hash=str(opening["last_state_hash"]), known_at=opening["known_at"])
    rows = read_book_checkpoint_parquet(path, expected=expected, limits=limits, check_budget=check_budget)
    checkpoint = BookCheckpointFact(
        checkpoint_id=str(expected["id"]), series_id=int(expected["series_id"]),
        validity_interval_id=str(expected["validity_interval_id"]), source_position=checkpoint_position,
        product_definition_version_id=str(expected["product_definition_version_id"]),
        provider_size_unit=ProviderSizeUnit(str(expected["provider_size_unit"])),
        ordering_assurance=validity.ordering_assurance, effective_at=expected["effective_at"],
        known_at=expected["known_at"], state_hash=str(expected["state_hash"]),
        bids=tuple((Decimal(row["price"]), Decimal(row["quantity"])) for row in rows if row["side"] == BookSide.BID.value),
        asks=tuple((Decimal(row["price"]), Decimal(row["quantity"])) for row in rows if row["side"] == BookSide.ASK.value),
        mutation_count_since_prior=int(expected["mutation_count_since_prior"]))
    if check_budget is not None:
        check_budget()
    return checkpoint, Level2BookReconstructor.from_checkpoint(checkpoint, contract=contract, validity=validity)


def publish_book_checkpoint(
    checkpoint: BookCheckpointFact,
    *,
    object_store: RawArchiveObjectStore,
    temporary_directory: Path | None = None,
) -> tuple[EncodedBookCheckpoint, ArchiveObjectAcknowledgement]:
    encoded = encode_book_checkpoint_parquet(
        checkpoint, temporary_directory=temporary_directory
    )
    object_key = checkpoint_object_key(checkpoint)
    try:
        acknowledgement = object_store.put_verified(
            object_key=object_key,
            source_path=encoded.path,
            expected_sha256=encoded.sha256,
        )
    finally:
        if encoded.path.exists():
            encoded.path.unlink()
    replayed = read_book_checkpoint_parquet(object_store.local_path(object_key))
    if len(replayed) != encoded.level_count:
        raise RuntimeError("market_book_checkpoint_upload_invalid: level count differs")
    return encoded, acknowledgement


__all__ = [
    "BOOK_CHECKPOINT_COMPRESSION",
    "BOOK_CHECKPOINT_FORMAT",
    "BookCheckpointReadLimits",
    "EncodedBookCheckpoint",
    "checkpoint_object_key",
    "encode_book_checkpoint_parquet",
    "publish_book_checkpoint",
    "read_book_checkpoint_parquet",
    "restore_book_checkpoint_parquet",
]
