"""Terminal namespace plumbing; real process/filesystem qualification is separate."""
from datetime import date
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import shutil
import tempfile

import pytest
from core.storage_targets import StorageTarget
from scripts.db import fact_header_v2_placement as placement


def test_lookup_placement_preserves_legacy_binding_and_requires_explicit_boolean():
    plan = placement.CopyPlacement(
        StorageTarget("recent", "Recent", "uuid-one", "/recent", "ssd"),
        StorageTarget("history", "History", "uuid-two", "/history", "hdd"),
        456, date(2026, 9, 1), Path("/trusted/pg_controldata"))
    legacy = plan.describe()
    assert "recent_lookup_indexes" not in legacy
    assert placement._restore(legacy).describe() == legacy
    selected = replace(plan, recent_lookup_indexes=True).describe()
    assert selected == {**legacy, "recent_lookup_indexes": True}
    assert placement._restore(selected).describe() == selected
    for invalid in (None, 1, "true", []):
        with pytest.raises(ValueError, match="placement_invalid"):
            replace(plan, recent_lookup_indexes=invalid)


def test_readonly_terminal_preserves_binding_and_requires_writable_peer(tmp_path,monkeypatch):
    history=Path(tempfile.mkdtemp(prefix="qt-terminal-placement-",dir="/dev/shm"))
    try:
        recent=tmp_path/"pgdata"; recent.mkdir(mode=0o700)
        assert recent.stat().st_dev!=history.stat().st_dev
        space=history/"space";space.mkdir()
        directory=space/"PG_15_123";directory.mkdir()
        (recent/"pg_tblspc").mkdir();(recent/"pg_tblspc"/"456").symlink_to(space)
        plan=placement.CopyPlacement(StorageTarget("recent","Recent","uuid-one",str(recent),"ssd"),
            StorageTarget("history","History","uuid-two",str(history),"hdd"),456,date(2026,9,1),Path("/trusted/pg_controldata"))
        context=dict(identity="123/789",database_oid=789,catalog_version=123)
        cluster=(recent,7,11)
        monkeypatch.setattr(placement,"_observe_database",lambda *a:(context,"process",cluster,"binary"))
        monkeypatch.setattr(placement,"_cluster_identity",lambda *a:cluster)
        monkeypatch.setattr(placement,"_process_readlink",lambda pid,path:str(space))
        reads=[];writable=[];mode=[False];peer=[True]
        def inspect(target,*,require_writable):
            reads.append(require_writable)
            if require_writable and mode[0]:raise RuntimeError("local target read only")
            return SimpleNamespace(path=target.root,device_id=placement._device_id(Path(target.root).stat().st_dev),
                                   filesystem_uuid=target.filesystem_uuid,read_only=mode[0])
        def same(pid,path,info,*,require_writable=False):
            if require_writable:
                writable.append(path)
                if not peer[0]:raise RuntimeError("database target read only")
        monkeypatch.setattr(StorageTarget,"inspect",inspect)
        monkeypatch.setattr(placement,"_same_process_file",same)
        destination=dict(can_create=True,location=str(space),spcname="history")
        class Result:
            def mappings(self):return self
            def one_or_none(self):return destination
        conn=SimpleNamespace(execute=lambda *a:Result())
        original,_=placement.observe(conn,plan)
        mode[0]=True;reads.clear();writable.clear()
        observed,_=placement.observe(conn,plan,read_only_namespace=True)
        assert observed==original and reads and not any(reads)
        assert recent in writable and writable.count(directory)==2
        with pytest.raises(RuntimeError,match="local target read only"):
            placement.observe(conn,plan)
        peer[0]=False
        with pytest.raises(RuntimeError,match="database target read only"):
            placement.observe(conn,plan,read_only_namespace=True)
        mode[0]=False;peer[0]=True
        with pytest.raises(RuntimeError,match="readonly_namespace_required"):
            placement.observe(conn,plan,read_only_namespace=True)
    finally:
        shutil.rmtree(history)
