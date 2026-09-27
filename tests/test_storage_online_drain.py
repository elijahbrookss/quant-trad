"""Read-only WAL observation: bounds, pending bytes, races and no authority."""
import os
from pathlib import Path
from time import monotonic

import pytest

from scripts.automation import storage_online_drain as drain


def observe(root, **overrides):
    return drain.inspect_spool(root, **({"deadline": monotonic()+5, "max_entries": 100,
                                           "check": lambda: None} | overrides))


def test_absent_and_acknowledged_spool_are_observations_only(tmp_path):
    assert observe(tmp_path)["spool_empty_at_observation"]
    spool = tmp_path/"spool"/"definition"/"session"/"epoch=0"
    spool.mkdir(parents=True)
    ack = spool/"segment.ack.json"
    ack.write_bytes(b"not relied on as a database certificate")
    result = observe(tmp_path)
    assert result["acknowledgement_files"] == 1 and result["spool_empty_at_observation"]
    assert not any(result[k] for k in ("publisher_drain_authorized", "final_switch_authorized",
                                       "collection_resume_authorized"))
    assert ack.read_bytes() == b"not relied on as a database certificate"


@pytest.mark.parametrize("name", ["a.open", "a.sealed", "a.ack.partial", "unknown"])
def test_pending_wal_never_deleted_even_with_acknowledgement(tmp_path, name):
    spool = tmp_path/"spool"; spool.mkdir()
    (spool/name).write_bytes(b"durable WAL")
    (spool/"a.ack.json").write_text("{}")
    before = {p.name: p.read_bytes() for p in spool.iterdir()}
    result = observe(tmp_path)
    assert not result["spool_empty_at_observation"]
    assert result["pending_files"] == 1 and result["pending_bytes"] == 11
    assert {p.name: p.read_bytes() for p in spool.iterdir()} == before


@pytest.mark.parametrize("kind", ["symlink", "fifo", "depth", "entries", "deadline"])
def test_invalid_or_unbounded_walk_refuses_and_closes_fds(tmp_path, kind):
    spool = tmp_path/"spool";spool.mkdir()
    kwargs = {}
    if kind == "symlink": (spool/"link").symlink_to(tmp_path, target_is_directory=True)
    elif kind == "fifo": os.mkfifo(spool/"fifo")
    elif kind == "depth": spool.joinpath(*[str(i) for i in range(10)]).mkdir(parents=True)
    elif kind == "entries":
        (spool/"a.ack.json").write_text("{}");(spool/"b.ack.json").write_text("{}")
        kwargs["max_entries"] = 1
    else: kwargs["deadline"] = monotonic()-1
    before = len(list(Path("/proc/self/fd").iterdir()))
    with pytest.raises(RuntimeError): observe(tmp_path, **kwargs)
    assert len(list(Path("/proc/self/fd").iterdir())) == before


def test_directory_replacement_during_observation_refuses(tmp_path):
    spool = tmp_path/"spool";spool.mkdir();(spool/"a.ack.json").write_text("{}")
    calls = [0]
    def change():
        calls[0] += 1
        if calls[0] == 3:
            spool.rename(tmp_path/"retained")
            spool.mkdir()
    with pytest.raises(RuntimeError, match="spool_changed"):
        observe(tmp_path, check=change)
    assert (tmp_path/"retained"/"a.ack.json").read_text() == "{}"
