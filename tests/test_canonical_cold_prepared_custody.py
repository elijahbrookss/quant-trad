import os

import pytest


if os.name != "posix":
    pytest.skip("native recovery custody requires POSIX file descriptors", allow_module_level=True)

from scripts.db import canonical_cold_prepared_custody as prepared


@pytest.fixture
def materialized(tmp_path):
    pgdata, history, archives = [tmp_path / name for name in ("data", "history", "base-archive")]
    for directory in (pgdata, history, archives):
        directory.mkdir(mode=0o700)
    (pgdata / "pg_tblspc").mkdir()
    (pgdata / "pg_tblspc" / "48261").symlink_to(history, target_is_directory=True)
    (pgdata / "PG_VERSION").write_bytes(b"15\n")
    (history / "relation").write_bytes(b"fact")
    (archives / "file").write_bytes(b"data")
    return {"pgdata": pgdata, "tablespaces": {"fixture-history": history}, "archive_root": archives,
            "max_entries": 25, "check": lambda: None}


def test_held_roots_have_repeatable_identity_and_no_file_content_reads(materialized, monkeypatch):
    open_path = os.open

    def only_open_directories(path, flags, *args, **kwargs):
        assert flags & os.O_DIRECTORY
        return open_path(path, flags, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(prepared.os, "open", only_open_directories)
        with prepared.capture_prepared_custody(**materialized) as custody:
            assert custody.entries == 8 and len(custody.sha256) == 64
            custody.assert_held(materialized["check"])
            custody.assert_archive_held(materialized["check"])
            with pytest.raises(RuntimeError, match="custody_busy"):
                with prepared.capture_prepared_custody(**materialized):
                    pytest.fail("the first owner still holds the roots")
        with prepared.capture_prepared_custody(**materialized) as later:
            assert later.sha256 == custody.sha256
    with pytest.raises(RuntimeError, match="custody_closed"):
        custody.assert_held(materialized["check"])


def test_archive_custody_survives_intentional_postmaster_writes_only(materialized):
    with prepared.capture_prepared_custody(**materialized) as custody:
        (materialized["pgdata"] / "PG_VERSION").write_bytes(b"16\n")
        with pytest.raises(RuntimeError, match="custody_tree_changed"):
            custody.assert_held(materialized["check"])
        custody.assert_archive_held(materialized["check"])
        archive = materialized["archive_root"] / "file"
        before = archive.stat()
        archive.write_bytes(b"edit")
        os.utime(archive, ns=(before.st_atime_ns, before.st_mtime_ns))
        with pytest.raises(RuntimeError, match="custody_tree_changed"):
            custody.assert_archive_held(materialized["check"])


def test_root_replacement_is_refused_even_with_equal_file_metadata(materialized):
    root = materialized["archive_root"]
    with prepared.capture_prepared_custody(**materialized) as custody:
        root.rename(root.with_name("old"))
        root.mkdir(mode=0o700)
        (root / "file").write_bytes(b"data")
        with pytest.raises(RuntimeError, match="custody_root_changed"):
            custody.assert_archive_held(materialized["check"])


@pytest.mark.parametrize(("hazard", "error"), [
    ("hardlink", "unsafe_member"),
    ("symlink", "unsafe_member"),
    ("missing_tablespace", "tablespace_links_incomplete"),
    ("wrong_tablespace", "tablespace_link_changed"),
    ("running_postmaster", "unsafe_member"),
    ("budget", "entry_budget_exceeded"),
])
def test_refuses_unowned_or_unbounded_members(materialized, hazard, error):
    root = materialized["archive_root"]
    pgdata = materialized["pgdata"]
    link = pgdata / "pg_tblspc" / "48261"
    if hazard == "hardlink":
        os.link(root / "file", root / "alias")
    elif hazard == "symlink":
        (root / "alias").symlink_to(root / "file")
    elif hazard == "missing_tablespace":
        link.unlink()
    elif hazard == "wrong_tablespace":
        link.unlink()
        link.symlink_to(root, target_is_directory=True)
    elif hazard == "running_postmaster":
        (pgdata / "postmaster.pid").touch()
    else:
        materialized["max_entries"] = 7
    with pytest.raises(RuntimeError, match=error):
        with prepared.capture_prepared_custody(**materialized):
            pytest.fail("preparation must fail before acquiring a custody receipt")
