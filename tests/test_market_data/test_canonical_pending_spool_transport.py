"""Recover two native sealed Level 2 spool segments onto separate fixture media."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import shutil

import pytest

from data_providers.streams.contracts import ProviderRawMessage
from market_data.archive import DurableRawSpoolSegment
from market_data.structure import RawStreamRecord
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits
from scripts.db.canonical_cold_recovery_pending_spool import (
    PendingSpoolScope, measure_pending_spool, stage_pending_spool, verify_pending_spool,
)


def _seal(root, definition, session, segment_ordinal, positions):
    segment = DurableRawSpoolSegment(root=root, definition_id=definition, session_id=session,
                                    connection_epoch=1, segment_ordinal=segment_ordinal)
    records = []
    for position in positions:
        timestamp = datetime(2026, 10, 30, 3, 21, tzinfo=UTC) + timedelta(microseconds=position)
        message = ProviderRawMessage.build(provider="COINBASE", venue="COINBASE_DIRECT", stream_session_id=session,
            connection_epoch=1, receive_ordinal=position, received_at=timestamp.isoformat(),
            raw_frame=f'{{"channel":"l2_data","sequence_num":{position}}}')
        record = RawStreamRecord.from_provider_message(message, definition_id=definition, spool_segment_id=segment.spool_segment_id,
            provider_product_id="BIP-FIXTURE", requested_channel="level2", observed_channel="l2_data")
        segment.append(record)
        records.append(record)
    segment.seal()
    return segment, records


def test_two_pending_sealed_segments_survive_encrypted_isolated_transfer(tmp_path, monkeypatch):
    receive_root = os.environ.get("QT_PENDING_SPOOL_TEST_RECEIVER")
    binary = shutil.which("restic")
    if receive_root is None or binary is None:
        pytest.skip("requires pinned Restic and a private separately mounted fixture root")
    receiving = Path(receive_root) / "pending-spool-receiver"
    receiving.mkdir(mode=0o700)
    for key in ("MARKET_STRUCTURE_WORKING_ROOT", "QT_MARKET_DATA_WORKING_EXPECTED_UUID", "QT_MARKET_DATA_EXPECTED_UUID"):
        monkeypatch.delenv(key, raising=False)
    source = tmp_path / "spool"
    scratch = tmp_path / "scratch"
    inactive = tmp_path / "inactive"
    for path in (source, scratch, inactive):
        path.mkdir(mode=0o700)
    definition, session = "coinbase-bip-level2-fixture", "mss_" + "a" * 32
    early, early_records = _seal(source, definition, session, 8, (650, 651))
    later, later_records = _seal(source, definition, session, 9, (652, 654))
    expected = {seg.sealed_path.relative_to(source): seg.sealed_path.read_bytes() for seg in (early, later)}
    scope = PendingSpoolScope(definition, session, 1, "COINBASE", "COINBASE_DIRECT", "BIP-FIXTURE", "level2",
                              2, sum(map(len, expected.values())))
    boundary = {"database_identity": "12345/6789", "source_commit_seq": 42, "anchor_sha256": "e" * 64}
    key = tmp_path / "key"
    key.touch(mode=0o600)
    key.write_text("d" * 64)
    common = dict(receiving=receiving, scope=scope, boundary=boundary, restic=Path(binary),
                  limits=TransferLimits(seconds=45, bytes=32 * 1024**2, files=100), max_entries=128)
    held = True

    def check_held():
        assert held

    stage = dict(source=source, private_scratch=scratch, encryption_key=key, check_held=check_held, **common)
    unknown, _ = _seal(source, definition, "mss_" + "b" * 32, 10, (1,))
    with pytest.raises(RuntimeError, match="unexpected_file"):
        stage_pending_spool(**stage)
    unknown.sealed_path.unlink()
    # Failed scope discovery publishes no bundle. Reuse the same receiver.
    assert not (receiving / "pending-spool").exists()
    assert not (receiving / ".pending-spool").exists()
    measured = measure_pending_spool(source=source, scope=scope, boundary=boundary, timeout_seconds=15,
                                     max_entries=128, check_held=check_held)
    staged = stage_pending_spool(**stage)
    assert {key: staged[key] for key in ("scope", "boundary", "segments")} == {
        key: measured[key] for key in ("scope", "boundary", "segments")}
    assert stage_pending_spool(**stage) == staged
    held = False
    with pytest.raises(AssertionError):
        stage_pending_spool(**stage)
    held = True
    first = early.sealed_path
    clean = first.read_bytes()
    try:
        first.write_bytes(clean[:-1] + b"!")
        with pytest.raises(RuntimeError, match="incomplete tail"):
            stage_pending_spool(**stage)
    finally:
        first.write_bytes(clean)

    receiver = dict(inactive_restore=inactive, expected_transfer_sha256=staged["transfer_sha256"], decryption_key=key, **common)
    with monkeypatch.context() as patch:
        patch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(inactive))
        with pytest.raises(ValueError, match="inactive_path_required"):
            verify_pending_spool(**receiver)
    with pytest.raises(RuntimeError, match="receipt_scope_changed"):
        verify_pending_spool(**(receiver | {"scope": replace(scope, session_id="mss_" + "b" * 32)}))
    config = receiving / "pending-spool" / "restic" / "config"
    original_config = receiving / "retained-test-only-config"
    config.rename(original_config)
    data = original_config.read_bytes()
    try:
        config.write_bytes(bytes([data[0] ^ 1]) + data[1:])
        with pytest.raises(RuntimeError, match="ciphertext_changed"):
            verify_pending_spool(**receiver)
    finally:
        original_config.replace(config)
    hidden_source = source.with_name("not-available-during-restore")
    source.rename(hidden_source)
    verified = verify_pending_spool(**receiver)
    assert verified == verify_pending_spool(**receiver)
    assert verified["boundary"] == boundary
    assert [item["first_receive_ordinal"] for item in verified["segments"]] == [650, 652]
    assert [item["last_receive_ordinal"] for item in verified["segments"]] == [651, 654]
    results = []
    for relative, raw in expected.items():
        path = inactive / "spool" / relative
        assert path.read_bytes() == raw == (hidden_source / relative).read_bytes()
        assert not path.with_suffix(".ack.json").exists()
        results.extend(DurableRawSpoolSegment.from_path(path).records())
    assert results == early_records + later_records
    print(f"QT_PENDING_SPOOL files={scope.sealed_files} bytes={scope.sealed_bytes} frames={len(results)}")
