"""Preserve live spool paths on source storage while archives use history."""
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from core.storage_mounts import StorageMountError
from market_data.archive import (
    DurableRawSpoolSegment, FilesystemRawArchiveObjectStore,
    publish_spool_archive, read_raw_archive_parquet,
)
from tests.test_market_data.test_market_structure_archive import _segment, _record

pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_STORAGE_DEMO") != "1",
                      reason="requires owned two-filesystem storage-demo topology"),
]


def test_existing_spool_recovers_on_source_and_publishes_to_history(tmp_path, monkeypatch):
    token = uuid4().hex
    working = Path("/qt-source/pgdata")/("qt_working_"+token)
    archive = Path("/qt-history")/("qt_archive_"+token)
    working.mkdir()
    archive.mkdir()
    source_device = working.stat().st_dev
    history_device = archive.stat().st_dev
    assert source_device != history_device
    udev = tmp_path/"udev"
    udev.mkdir()
    for device, identity in ((source_device, "working-source"), (history_device, "working-history")):
        (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID="+identity+"\n")
    monkeypatch.setenv("QT_STORAGE_UDEV_ROOT", str(udev))
    monkeypatch.delenv("MARKET_STRUCTURE_WORKING_ROOT", raising=False)
    monkeypatch.delenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", raising=False)
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(working))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "working-source")
    segment = _segment(working/"spool")
    records = [_record(segment, 1), _record(segment, 2)]
    for record in records:
        segment.append(record)
    segment.close()
    retained_path = segment.open_path
    with retained_path.open("ab") as handle:
        handle.write(b'{"record_kind":"interrupted')
    interrupted_bytes = retained_path.read_bytes()

    # Relocate archives only; existing absolute spool paths stay untouched.
    monkeypatch.setenv("MARKET_STRUCTURE_STORAGE_ROOT", str(archive))
    monkeypatch.setenv("QT_MARKET_DATA_EXPECTED_UUID", "working-history")
    monkeypatch.setenv("MARKET_STRUCTURE_WORKING_ROOT", str(working))
    monkeypatch.setenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", "wrong-working")
    with pytest.raises(StorageMountError, match="identity_mismatch"):
        DurableRawSpoolSegment.from_path(retained_path)
    assert retained_path.read_bytes() == interrupted_bytes
    monkeypatch.setenv("QT_MARKET_DATA_WORKING_EXPECTED_UUID", "working-source")
    recovered = DurableRawSpoolSegment.from_path(retained_path)
    assert recovered.open_path == retained_path
    assert recovered.recovery_evidence.truncated_tail_bytes > 0
    assert list(recovered.records()) == records
    recovered.seal()
    store = FilesystemRawArchiveObjectStore(archive/"objects")
    staging_devices = []
    publish = store.put_verified
    def observe_source(**kwargs):
        staging_devices.append(Path(kwargs["source_path"]).stat().st_dev)
        return publish(**kwargs)
    monkeypatch.setattr(store, "put_verified", observe_source)
    _, receipt, published = publish_spool_archive(recovered,
        object_store=store, temporary_directory=working/"tmp")
    destination = store.local_path(receipt.object_key)
    assert staging_devices == [source_device]
    assert destination.stat().st_dev == history_device
    assert tuple(records) == published
    assert read_raw_archive_parquet(destination) == records
    assert recovered.sealed_path.stat().st_dev == source_device

    # Publication alone is not a DB acknowledgement and cannot delete intake.
    with pytest.raises(RuntimeError, match="database acknowledgement"):
        recovered.discard_acknowledged_spool()
    restarted = DurableRawSpoolSegment.from_path(recovered.sealed_path)
    _, retried, _ = publish_spool_archive(restarted,
        object_store=store, temporary_directory=working/"tmp")
    assert retried.reused_existing and retried.sha256 == receipt.sha256
    assert list(restarted.records()) == records
    assert not (archive/"spool").exists()
    assert list((working/"tmp").iterdir()) == []
    print("QT_WORKING_ROOT_REPORT="+json.dumps({
        "retained_spool_path_preserved": True, "interrupted_tail_recovered": True,
        "wrong_mount_rejected_before_repair": True,
        "spool_and_raw_staging_on_source": True, "immutable_archive_on_history": True,
        "raw_replay_unchanged": True, "retry_reuses_published_object": True,
        "no_spool_deletion_without_database_acknowledgement": True,
        "limitation": "disposable filesystems, not physical HDD or full provider load",
    }, sort_keys=True))
