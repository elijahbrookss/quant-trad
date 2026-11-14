"""Recover the moved raw mapping PK from a paired copy at its original SQL path."""

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


def _restore_same_path(worker, copied, selected, *target):
    assert target == ("--type=immediate",)
    source = Path("/qt-history")
    restored = Path("/qt-restored-history")
    root = copied.parent
    assert source.stat().st_dev != restored.stat().st_dev
    assert (source / "source-volume").read_text() == "source\n"
    assert not (source / "restore-volume").exists()
    assert not (restored / "source-volume").exists()
    _restored_access(worker, copied)
    request = root / "restore-request.tmp"
    request.write_text(selected["database_label"])
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
        spaces = conn.scalars(text("SELECT oid::bigint FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')"))
        assert spaces.all() == [scope["tablespace_oid"]]
    _exercise_postwrite_pair(storage, tmp_path, monkeypatch, raw_scope=scope, restore_database=_restore_same_path)
