from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import (
    DurableRawSpoolSegment,
    FilesystemRawArchiveObjectStore,
    SpoolBackpressureError,
    encode_spool_segment_to_parquet,
    publish_spool_archive,
    read_raw_archive_parquet,
    require_spool_capacity,
    spool_backlog_bytes,
)
from market_data.structure import RawStreamRecord


def _record(segment: DurableRawSpoolSegment, ordinal: int) -> RawStreamRecord:
    frame = (
        '{"channel":"market_trades","sequence_num":%d,'
        '"events":[{"type":"update","trades":[]}]}' % ordinal
    )
    message = ProviderRawMessage.build(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        stream_session_id=segment.session_id,
        connection_epoch=segment.connection_epoch,
        receive_ordinal=ordinal,
        received_at=f"2026-08-02T07:20:{ordinal:02d}Z",
        raw_frame=frame,
    )
    return RawStreamRecord.from_provider_message(
        message,
        definition_id=segment.definition_id,
        spool_segment_id=segment.spool_segment_id,
        provider_product_id="BTC-USD",
        requested_channel="market_trades",
        observed_channel="market_trades",
    )


def _segment(root: Path) -> DurableRawSpoolSegment:
    return DurableRawSpoolSegment(
        root=root,
        definition_id="coinbase-btc-trades",
        session_id="session-a",
        connection_epoch=0,
    )


def test_spool_fsync_recovery_truncates_only_incomplete_crash_tail(tmp_path: Path) -> None:
    segment = _segment(tmp_path / "spool")
    first = _record(segment, 1)
    second = _record(segment, 2)
    segment.append(first)
    segment.append(second)
    segment.close()
    with segment.open_path.open("ab") as handle:
        handle.write(b'{"record_kind":"raw_frame","raw_record_id":"partial')
        handle.flush()

    recovered = _segment(tmp_path / "spool")
    evidence = recovered.recovery_evidence
    assert evidence is not None
    assert evidence.recovered_record_count == 2
    assert evidence.truncated_tail_bytes > 0
    assert list(recovered.records()) == [first, second]


def test_spool_rejects_duplicate_or_reordered_receive_positions(tmp_path: Path) -> None:
    segment = _segment(tmp_path / "spool")
    segment.append(_record(segment, 1))
    with pytest.raises(ValueError, match="strictly increasing"):
        segment.append(_record(segment, 1))


def test_sealed_spool_encodes_and_replays_exact_frames_deterministically(
    tmp_path: Path,
) -> None:
    segment = _segment(tmp_path / "spool")
    records = [_record(segment, 1), _record(segment, 2)]
    for record in records:
        segment.append(record)
    segment.seal()
    first = encode_spool_segment_to_parquet(
        segment, temporary_directory=tmp_path / "tmp"
    )
    second = encode_spool_segment_to_parquet(
        segment, temporary_directory=tmp_path / "tmp"
    )
    try:
        assert first.sha256 == second.sha256
        assert first.content_fingerprint == second.content_fingerprint
        assert read_raw_archive_parquet(first.path) == records
    finally:
        first.path.unlink(missing_ok=True)
        second.path.unlink(missing_ok=True)


def test_object_acknowledgement_requires_verified_immutable_bytes(
    tmp_path: Path,
) -> None:
    segment = _segment(tmp_path / "spool")
    records = [_record(segment, 1), _record(segment, 2)]
    for record in records:
        segment.append(record)
    segment.seal()
    store = FilesystemRawArchiveObjectStore(tmp_path / "objects")
    encoded, acknowledgement, archived_records = publish_spool_archive(
        segment,
        object_store=store,
        temporary_directory=tmp_path / "tmp",
    )
    assert archived_records == tuple(records)
    assert acknowledgement.sha256 == encoded.sha256
    object_path = store.local_path(acknowledgement.object_key)
    assert hashlib.sha256(object_path.read_bytes()).hexdigest() == acknowledgement.sha256
    assert read_raw_archive_parquet(object_path) == records
    assert not segment.ack_path.exists(), "object ack alone is not database archive-complete"

    with pytest.raises(RuntimeError, match="database acknowledgement is missing"):
        segment.discard_acknowledged_spool()
    segment.mark_database_acknowledged(
        manifest_id="manifest-a",
        object_key=acknowledgement.object_key,
        object_sha256=acknowledgement.sha256,
    )
    segment.discard_acknowledged_spool()
    assert not segment.sealed_path.exists()

    same = store.put_verified(
        object_key=acknowledgement.object_key,
        source_path=object_path,
        expected_sha256=acknowledgement.sha256,
    )
    assert same.reused_existing is True


def test_failed_object_upload_leaves_sealed_spool_recoverable(tmp_path: Path) -> None:
    segment = _segment(tmp_path / "spool")
    segment.append(_record(segment, 1))
    segment.seal()

    class FailingStore(FilesystemRawArchiveObjectStore):
        def put_verified(self, **_kwargs):
            raise RuntimeError("injected upload failure")

    with pytest.raises(RuntimeError, match="injected upload failure"):
        publish_spool_archive(
            segment,
            object_store=FailingStore(tmp_path / "objects"),
            temporary_directory=tmp_path / "tmp",
        )
    assert segment.sealed_path.exists()
    assert list(segment.records()) == [_record(segment, 1)]
    assert not segment.ack_path.exists()


def test_spool_backpressure_fails_before_exceeding_bound(tmp_path: Path) -> None:
    segment = _segment(tmp_path / "spool")
    segment.append(_record(segment, 1))
    current = segment.open_path.stat().st_size
    require_spool_capacity(
        root=tmp_path / "spool",
        max_backlog_bytes=current + 10,
        next_frame_bytes=10,
    )
    with pytest.raises(SpoolBackpressureError, match="bounded local backlog"):
        require_spool_capacity(
            root=tmp_path / "spool",
            max_backlog_bytes=current + 10,
            next_frame_bytes=11,
        )


def test_spool_backlog_and_capacity_are_definition_scoped(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    first = _segment(root)
    first.append(_record(first, 1))
    second = DurableRawSpoolSegment(
        root=root,
        definition_id="coinbase-bip-trades",
        session_id="session-b",
        connection_epoch=0,
    )
    second.append(_record(second, 1))

    first_bytes = first.open_path.stat().st_size
    second_bytes = second.open_path.stat().st_size
    assert spool_backlog_bytes(
        root, definition_id=first.definition_id
    ) == first_bytes
    assert spool_backlog_bytes(
        root, definition_id=second.definition_id
    ) == second_bytes
    assert spool_backlog_bytes(root) == first_bytes + second_bytes
    require_spool_capacity(
        root=root,
        definition_id=first.definition_id,
        max_backlog_bytes=first_bytes + 1,
        next_frame_bytes=1,
    )
