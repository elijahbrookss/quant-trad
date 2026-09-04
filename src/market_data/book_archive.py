"""Immutable deterministic checkpoint objects for reconstructed Level 2 books."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from core.storage_mounts import require_configured_archive_mount
from .archive import ArchiveObjectAcknowledgement, RawArchiveObjectStore
from .order_book import (
    BOOK_CHECKPOINT_SCHEMA_VERSION,
    BookCheckpointFact,
    BookSide,
    checkpoint_canonical_rows,
)


BOOK_CHECKPOINT_FORMAT = "parquet"
BOOK_CHECKPOINT_COMPRESSION = "zstd"


@dataclass(frozen=True)
class EncodedBookCheckpoint:
    checkpoint_id: str
    path: Path
    sha256: str
    content_fingerprint: str
    byte_count: int
    level_count: int


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
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
    schema = pa.schema(
        [
            pa.field("schema_version", pa.string(), nullable=False),
            pa.field("checkpoint_id", pa.string(), nullable=False),
            pa.field("side", pa.string(), nullable=False),
            pa.field("level_ordinal", pa.int64(), nullable=False),
            pa.field("price", pa.string(), nullable=False),
            pa.field("quantity", pa.string(), nullable=False),
            pa.field("provider_size_unit", pa.string(), nullable=False),
        ],
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


def read_book_checkpoint_parquet(path: Path) -> tuple[dict[str, object], ...]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("market_book_checkpoint_requires_pyarrow") from exc
    table = pq.ParquetFile(Path(path)).read()
    metadata = table.schema.metadata or {}
    if metadata.get(b"schema_version") != BOOK_CHECKPOINT_SCHEMA_VERSION.encode("ascii"):
        raise RuntimeError("market_book_checkpoint_replay_invalid: schema mismatch")
    rows = tuple(dict(row) for row in table.to_pylist())
    previous_side: BookSide | None = None
    previous_price: Decimal | None = None
    side_rank = {BookSide.BID: 0, BookSide.ASK: 1}
    for row in rows:
        try:
            side = BookSide(str(row["side"]))
            price = Decimal(str(row["price"]))
            quantity = Decimal(str(row["quantity"]))
        except Exception as exc:  # noqa: BLE001 - normalized archive failure
            raise RuntimeError(
                "market_book_checkpoint_replay_invalid: malformed typed level"
            ) from exc
        if price <= 0 or quantity <= 0:
            raise RuntimeError(
                "market_book_checkpoint_replay_invalid: nonpositive level"
            )
        if previous_side is side and previous_price is not None and price <= previous_price:
            raise RuntimeError(
                "market_book_checkpoint_replay_invalid: levels are not sorted"
            )
        if previous_side is not None and side_rank[side] < side_rank[previous_side]:
            raise RuntimeError(
                "market_book_checkpoint_replay_invalid: sides are not sorted"
            )
        if previous_side is not side:
            previous_price = None
        previous_side = side
        previous_price = price
    return rows


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
    "EncodedBookCheckpoint",
    "checkpoint_object_key",
    "encode_book_checkpoint_parquet",
    "publish_book_checkpoint",
    "read_book_checkpoint_parquet",
]
