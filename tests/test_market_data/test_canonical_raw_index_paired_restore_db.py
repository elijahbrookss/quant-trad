"""Recover the moved raw mapping PK from a paired copy at its original SQL path."""

from functools import partial
import os
from pathlib import Path
from time import monotonic, sleep

import pytest
from sqlalchemy import text

from tests.test_market_data.test_canonical_cold_activate_db import native_raw_index_target  # noqa: F401
from tests.test_market_data.test_canonical_postwrite_recovery_db import _exercise_postwrite_pair, _restored_access
from tests.test_market_data.test_fact_storage_tiers_db import storage  # noqa: F401


pytestmark = [
    pytest.mark.db,
    pytest.mark.skipif(os.getenv("QT_SAME_PATH_RECOVERY_TEST") != "1", reason="requires separate restore volume"),
]


def _restore_same_path(worker, copied, selected, *target, tablespace):
    assert target == ("--type=immediate",)
    source = Path("/qt-history")
    restored = Path("/qt-restored-history")
    root = copied.parent
    source_id, restore_id = source.stat(), restored.stat()
    same_device = os.getenv("QT_SAME_DEVICE_RECOVERY_TEST") == "1"
    assert (source_id.st_dev == restore_id.st_dev) == same_device
    assert (source_id.st_dev, source_id.st_ino) != (restore_id.st_dev, restore_id.st_ino)
    assert (source / "source-volume").read_text() == "source\n"
    assert not (source / "restore-volume").exists()
    assert not (restored / "source-volume").exists()
    if same_device:
        print(f"QT_SAME_DEVICE_ISOLATED_HISTORY device={source_id.st_dev} source_inode={source_id.st_ino} restore_inode={restore_id.st_ino}")
    _restored_access(worker, copied)
    request = root / "restore-request.tmp"
    request.write_text(f"{selected['database_label']}\n{tablespace}\n")
    request.rename(root / "restore-request")
    deadline = monotonic() + 45
    while not (root / "restore-complete").is_file():
        assert not (root / "restore-failed").exists(), "isolated pgBackRest restore failed"
        assert monotonic() < deadline, "isolated pgBackRest restore timed out"
        sleep(0.1)
    assert (restored / "restore-volume").read_text() == "restore\n"
    assert (source / "source-volume").read_text() == "source\n"
    assert not (source / "restore-volume").exists()
    assert not (restored / "source-volume").exists()


def test_paired_restore_keeps_raw_index_on_same_logical_path(storage, tmp_path, monkeypatch, request):
    source = Path("/qt-history")
    location = source / "raw-index"
    location.mkdir(mode=0o700)
    (source / "source-volume").write_text("source\n")
    monkeypatch.setenv("QT_TRANSITION_RAW_INDEX_TABLESPACE_DIRECTORY", str(location))
    scope = request.getfixturevalue("native_raw_index_target")
    with storage.database._engine.begin() as conn:
        spaces = conn.execute(text("""
            SELECT oid::bigint,spcname,pg_tablespace_location(oid) FROM pg_tablespace
            WHERE spcname NOT IN ('pg_default','pg_global')
        """)).all()
        [(oid, tablespace, path)] = spaces
        assert (oid, path) == (scope["tablespace_oid"], str(location))
    _exercise_postwrite_pair(storage, tmp_path, monkeypatch, raw_scope=scope,
                             restore_database=partial(_restore_same_path, tablespace=tablespace))
