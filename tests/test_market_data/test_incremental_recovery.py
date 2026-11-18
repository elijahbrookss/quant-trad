"""Encrypted recovery key/configuration and bounded subprocess failure paths."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tracemalloc
from contextlib import nullcontext
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from time import monotonic, sleep

import pytest

from portal.backend.service.storage.incremental_recovery import (
    CutoverCapture, EncryptedRecoveryCopies, _private_bytes, _root_label,
)
from portal.backend.service.storage import incremental_recovery as recovery


def test_archive_parent_uses_latest_published_point_instead_of_orphan():
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.identity = "disposable/15"
    host = "qt-"+hashlib.sha256(worker.identity.encode()).hexdigest()[:32]
    receipts = [{"archive_snapshot": n*64, "name": "copy_"+n*32} for n in "123"]
    snapshots = [{"id": r["archive_snapshot"], "hostname": host, "tags": [r["name"]]}
                 for r in receipts]
    worker.completed = lambda: []
    assert worker._archive_parent(snapshots) is None
    worker.completed = lambda: [(None, None, r) for r in receipts[:2]]
    assert worker._archive_parent(snapshots) == receipts[1]["archive_snapshot"]


@pytest.mark.parametrize("invalid", ["missing", "foreign_host", "wrong_generation", "duplicate"])
def test_archive_parent_refuses_unavailable_or_misbound_snapshot(invalid):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.identity = "disposable/15"
    receipt = {"archive_snapshot": "a"*64, "name": "copy_"+"1"*32}
    worker.completed = lambda: [(None, None, receipt)]
    snapshot = {"id": receipt["archive_snapshot"],
                "hostname": "qt-"+hashlib.sha256(worker.identity.encode()).hexdigest()[:32],
                "tags": [receipt["name"]]}
    if invalid == "foreign_host":
        snapshot["hostname"] = "another-database"
    elif invalid == "wrong_generation":
        snapshot["tags"] = ["copy_"+"2"*32]
    snapshots = [] if invalid == "missing" else [snapshot]*(2 if invalid == "duplicate" else 1)
    with pytest.raises(RuntimeError, match="incremental_archive_parent_not_owned_or_available"):
        worker._archive_parent(snapshots)


@pytest.mark.skipif(shutil.which("restic") is None, reason="requires pinned native restic")
def test_native_restic_keeps_summary_without_periodic_progress_and_reports_failure(tmp_path):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root = tmp_path
    worker.restic = Path(shutil.which("restic"))
    worker.env = {"PATH": os.defpath, "HOME": str(tmp_path),
                  "RESTIC_PASSWORD": "disposable-test-key"}
    worker.deadline = monotonic()+30

    def check(*args):
        if monotonic() >= worker.deadline:
            raise RuntimeError("recovery_time_budget_exceeded")

    worker.check = check
    assert worker._run([str(worker.restic), "version"]).startswith(b"restic 0.19.1 ")
    worker._run(worker._rs("init"))
    # With --no-scan, progress starts only after a chunk/item is processed.
    # Exceed the native maximum chunk size before waiting; a small stdin input
    # would finish without exercising periodic progress at all.
    output = worker._run(worker._rs("backup", "--no-scan", "--stdin-from-command",
        "--stdin-filename", "probe", "--", sys.executable, "-c",
        "import sys,time; sys.stdout.buffer.write(b'a'*(16*1024*1024)); "
        "sys.stdout.flush(); time.sleep(1)"))
    messages = [json.loads(line) for line in output.splitlines()]
    assert [item["message_type"] for item in messages] == ["summary"]
    snapshot = messages[0]["snapshot_id"]
    assert len(snapshot) == 64

    with pytest.raises(RuntimeError, match="incremental_tool_failed:restic:exit=") as failure:
        worker._run(worker._rs("backup", "--stdin-from-command", "--",
            sys.executable, "-c",
            "import sys; sys.stderr.write('private diagnostic'); sys.exit(7)"))
    assert "private diagnostic" not in str(failure.value)
    snapshots = json.loads(worker._run(worker._rs("snapshots")))
    assert [item["id"] for item in snapshots] == [snapshot]


@pytest.mark.parametrize("stream", ["out", "err"])
def test_tool_output_limit_names_stream_without_exposing_content(tmp_path, stream):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root, worker.env = tmp_path, {"PATH": os.defpath}
    worker.deadline = monotonic()+10
    worker.check = lambda *args: None
    descriptor = 1 if stream == "out" else 2
    script = f"import os; os.write({descriptor}, b'private diagnostic'*500000)"
    with pytest.raises(RuntimeError) as failure:
        worker._run([sys.executable, "-c", script])
    assert str(failure.value) == (
        f"incremental_tool_output_limit:{Path(sys.executable).name}:stream={stream}")


@pytest.mark.skipif(shutil.which("restic") is None, reason="requires pinned native restic")
def test_native_snapshot_metadata_streams_large_path_lists(tmp_path):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root, worker.restic = tmp_path, Path(shutil.which("restic"))
    worker.env = {"PATH": os.defpath, "HOME": str(tmp_path),
                  "RESTIC_PASSWORD": "disposable-test-key"}
    worker.deadline = monotonic()+30

    def check(*args):
        if monotonic() >= worker.deadline:
            raise RuntimeError("recovery_time_budget_exceeded")

    worker.check = check
    worker._run(worker._rs("init"))
    source = tmp_path/("f"*230)
    source.write_bytes(b"preserved archive")
    listing = tmp_path/"files.list"
    # Native snapshots preserve input paths, even repeated ones. Exercise real
    # large metadata with a single tiny file rather than thousands of test files.
    listing.write_bytes((os.fsencode(source)+b"\0")*32000)
    tag = "copy_"+"1"*32
    output = worker._run(worker._rs("backup", "--no-scan", "--files-from-raw",
        str(listing), "--host", "qt-disposable", "--tag", tag))
    snapshot_id = json.loads(output)["snapshot_id"]
    with pytest.raises(RuntimeError, match="incremental_tool_output_limit:restic:stream=out"):
        worker._run(worker._rs("snapshots"))
    tracemalloc.start()
    try:
        snapshots = worker._native_snapshots()
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert snapshots == [{"id": snapshot_id, "hostname": "qt-disposable", "tags": [tag]}]
    assert peak < 8*1024*1024


@pytest.mark.parametrize("document", [
    b'{"secret": "private diagnostic"}', b'[1]', b'[{"id":null}]',
    b'[{"id":"invalid"}]', b'[{"id":"private diagnostic"}',
    b'[{"id":"'+b'a'*64+b'","id":"'+b'b'*64+b'"}]',
    b'[{"id":"'+b'a'*64+b'","tags":[{}]}]',
    b'[{"id":"'+b'a'*64+b'","hostname":{}}]',
])
def test_snapshot_metadata_rejects_malformed_or_ambiguous_output(document):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker._rs = lambda *args: list(args)

    def run(command, *, stdout_consumer):
        for offset in range(0, len(document), 7):
            stdout_consumer(document[offset:offset+7])

    worker._run = run
    with pytest.raises(RuntimeError, match="incremental_archive_snapshot_") as failure:
        worker._native_snapshots()
    assert "private diagnostic" not in str(failure.value)


def test_private_keys_reject_aliases_permissions_and_unbounded_content(tmp_path):
    path = tmp_path/"key"
    path.write_bytes(b"a"*64)
    path.chmod(0o600)
    assert _private_bytes(path, limit=64) == b"a"*64
    path.chmod(0o644)
    with pytest.raises(RuntimeError, match="permissions"):
        _private_bytes(path, limit=64)
    path.chmod(0o600)
    alias = tmp_path/"alias"
    alias.symlink_to(path)
    with pytest.raises(ValueError, match="unaliased"):
        _private_bytes(alias, limit=64)
    with pytest.raises(RuntimeError, match="size_limit"):
        _private_bytes(path, limit=63)
    os.link(path, tmp_path/"hardlink")
    with pytest.raises(RuntimeError, match="permissions"):
        _private_bytes(path, limit=64)


@pytest.mark.parametrize("label", ["", "../backup", "20260924-130900F_suffix", None, 1])
def test_dependency_retirement_requires_exact_native_labels(label):
    with pytest.raises(RuntimeError, match="label_invalid"):
        _root_label(label)


@pytest.mark.skipif(sys.platform != "linux", reason="POSIX process-group supervision")
def test_deadline_kills_owned_child_even_after_leader_exits(tmp_path):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root, worker.env = tmp_path, {"PATH": os.defpath}
    worker.deadline = monotonic()+1
    def check(*args):
        if monotonic() >= worker.deadline:
            raise RuntimeError("recovery_time_budget_exceeded")
    worker.check = check
    marker = tmp_path/"child.pid"
    script = (
        "import os,time,pathlib\n"
        "pid=os.fork()\n"
        "if pid: os._exit(0)\n"
        f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    with pytest.raises(RuntimeError, match="time_budget"):
        worker._run([sys.executable, "-c", script])
    assert marker.exists()
    child = int(marker.read_text())
    # A reparented child may remain a zombie until the host reaps it, but cannot
    # remain a live writer after the owning operation fails.
    deadline = monotonic()+3
    while True:
        status = Path(f"/proc/{child}/stat")
        try:
            state = status.read_text().split()[2]
        except FileNotFoundError:
            # Reaping may occur between an existence check and the read.
            # A gone child is the successful termination this test requires.
            break
        if state == "Z":
            break
        if monotonic() >= deadline:
            os.kill(child, signal.SIGKILL)
            pytest.fail("owned backup child survived deadline")
        sleep(0.01)


@pytest.fixture
def cutover_worker(tmp_path, monkeypatch):
    worker = object.__new__(EncryptedRecoveryCopies)
    worker.root, worker.device, worker.identity = tmp_path, tmp_path.stat().st_dev, "123/456"
    worker.target = SimpleNamespace(filesystem_uuid="fixture-hdd")
    worker.config = SimpleNamespace(max_chain_backups=1, pg_path=tmp_path/"source",
                                    pg_socket_path=tmp_path/"socket")
    worker.url = SimpleNamespace(port=5432, username="fixture", database="fixture")
    worker.pgbackrest, worker.restic = Path("/trusted/pgbackrest"), Path("/trusted/restic")
    worker.env, worker.peak_allocated_bytes = {"PATH": os.defpath}, 0
    worker.check = lambda *args: None
    worker.lock = nullcontext
    worker._assert_fence = lambda session: None
    layout = {"layout_version": "market.fact_storage_tiers.v2", "certificate_sha256": "d"*64}
    monkeypatch.setattr(recovery, "_snapshot_layout", lambda session: layout)
    previous = "copy_" + "1"*32
    request = CutoverCapture("copy_" + "2"*32, previous, 100)
    native = tmp_path/"database"/"backup"/"qt"
    native.mkdir(parents=True)
    old_label, new_label = "20261118-000000F", "20261118-000000F_20261118-010000I"
    (native/old_label).mkdir()
    receipt = {"schema_version": worker.schema_version, "name": previous,
               "database_identity": worker.identity, "filesystem_uuid": "fixture-hdd",
               "storage_layout": layout, "completed_at": datetime(2026, 11, 17, tzinfo=UTC).isoformat(),
               "database_label": old_label, "database_type": "full", "archive_snapshot": "a"*64,
               "inventory_sha256": "c"*64, "tools": {"pgbackrest": "pgBackRest 2.59.1"}}
    directory = tmp_path/previous
    directory.mkdir(mode=0o700)
    recovery._json_write(directory/"owner.json", {key: receipt[key] for key in (
        "schema_version", "name", "database_identity", "filesystem_uuid")})
    recovery._json_write(directory/"complete.json", receipt)
    original_receipt = (directory/"complete.json").read_bytes()
    backups = [{"label": old_label, "type": "full", "annotation": {"qt-generation": previous}}]
    host = "qt-"+hashlib.sha256(worker.identity.encode()).hexdigest()[:32]
    snapshots = [{"id": "a"*64, "hostname": host, "tags": [previous]}]
    worker._native_backups = lambda: deepcopy(backups)
    worker._native_snapshots = lambda: deepcopy(snapshots)
    calls, pruning, fail = [], [], []
    worker._prune = lambda keep: pruning.append(keep)
    def run(command):
        calls.append(command)
        if command[-1] == "version":
            return b"pgBackRest 2.59.3" if command[0] == str(worker.pgbackrest) else b"restic 0.19.1 fixture"
        if command[0] == str(worker.pgbackrest) and command[-1] == "backup":
            generation = next(arg.split("=", 2)[2] for arg in command if arg.startswith("--annotation="))
            assert worker.env.get("TMPDIR") == (str(tmp_path) if generation == request.generation else None)
            if fail:
                raise RuntimeError("injected_native_failure")
            kind = next(arg.split("=", 1)[1] for arg in command if arg.startswith("--type="))
            label = new_label if kind == "incr" else "20261118-010000F"
            (native/label).mkdir()
            backups.append({"label": label, "type": kind, "prior": old_label if kind == "incr" else None,
                            "annotation": {"qt-generation": generation}})
            return b""
        if command[0] == str(worker.restic) and "backup" in command:
            snapshots.append({"id": "b"*64, "hostname": host, "tags": [command[command.index("--tag")+1]]})
            return json.dumps({"message_type": "summary", "snapshot_id": "b"*64}).encode()
        if command[0] == str(worker.restic) and command[-1] == "unlock":
            return b""
        pytest.fail("unexpected native mutation: " + repr(command))
    worker._run = run
    def inventory(session, objects, partial):
        (partial/"objects.jsonl").write_bytes(b"")
        (partial/"files.list").write_bytes(b"")
        return "c"*64, 0, 0
    worker._inventory = inventory
    objects = SimpleNamespace(root=tmp_path/"objects")
    objects.root.mkdir()
    session = SimpleNamespace(in_transaction=lambda: False)
    return SimpleNamespace(worker=worker, request=request, session=session, objects=objects,
                           calls=calls, pruning=pruning, fail=fail, backups=backups, snapshots=snapshots,
                           previous=directory, original_receipt=original_receipt, native=native)


def test_named_capture_publishes_once_without_rollover_or_pruning(cutover_worker):
    f = cutover_worker
    receipt = f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request)
    assert receipt["name"] == f.request.generation and receipt["database_type"] == "incr"
    assert not f.pruning and len(f.backups) == 2 and len(f.snapshots) == 2
    assert (f.previous/"complete.json").read_bytes() == f.original_receipt
    assert "TMPDIR" not in f.worker.env
    sidecar = json.loads((f.worker.root/f.request.generation/"cutover-request.json").read_text())
    assert sidecar["previous_receipt_sha256"] == hashlib.sha256(f.original_receipt).hexdigest()
    assert "source_reserve_bytes" not in receipt and "cutover_capture" not in receipt
    calls = deepcopy(f.calls)
    assert f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request) == receipt
    assert f.calls == calls and not f.pruning


def test_normal_capture_retains_rollover_and_pruning(cutover_worker):
    f = cutover_worker
    receipt = f.worker.create(f.session, objects=f.objects, keep_copies=1)
    assert receipt["database_type"] == "full" and f.pruning == [1, 1]
    assert not (f.worker.root/receipt["name"]/"cutover-request.json").exists()
    assert "TMPDIR" not in f.worker.env


def test_named_capture_changed_request_or_receipt_refuses_replay(cutover_worker):
    f = cutover_worker
    f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request)
    with pytest.raises(RuntimeError, match="cutover_request_changed"):
        f.worker.create(f.session, objects=SimpleNamespace(root=f.objects.root/"changed"),
                        keep_copies=1, cutover_capture=f.request)
    with pytest.raises(RuntimeError, match="cutover_request_changed"):
        f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=CutoverCapture(
            f.request.generation, f.request.expected_previous_generation, 101))
    path = f.worker.root/f.request.generation/"complete.json"
    receipt = json.loads(path.read_text())
    receipt["archive_bytes"] = 1
    path.write_text(json.dumps(receipt))
    with pytest.raises(RuntimeError, match="cutover_request_changed"):
        f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request)
    assert len(f.backups) == 2 and not f.pruning


@pytest.mark.parametrize("failure", ["qt_partial", "native_partial", "missing_predecessor", "native_predecessor"])
def test_named_capture_refuses_unowned_or_partial_state_before_native_mutation(cutover_worker, failure):
    f = cutover_worker
    if failure == "qt_partial":
        (f.worker.root/("."+f.request.generation)).mkdir(mode=0o700)
    elif failure == "native_partial":
        (f.native/"20261118-000000F_20261118-000001I").mkdir()
    elif failure == "missing_predecessor":
        f.request = CutoverCapture(f.request.generation, "copy_"+"3"*32, 100)
    else:
        f.backups[0]["annotation"]["qt-generation"] = "copy_"+"3"*32
    with pytest.raises(RuntimeError, match="incremental_cutover_"):
        f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request)
    assert not f.calls and not f.pruning
    assert (f.previous/"complete.json").read_bytes() == f.original_receipt


def test_named_failed_attempt_preserves_evidence_and_never_restarts(cutover_worker):
    f = cutover_worker
    f.fail.append(True)
    with pytest.raises(RuntimeError, match="injected_native_failure"):
        f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request)
    partial = f.worker.root/("."+f.request.generation)
    assert (partial/"owner.json").is_file()
    calls = deepcopy(f.calls)
    with pytest.raises(RuntimeError, match="partial_requires_inspection"):
        f.worker.create(f.session, objects=f.objects, keep_copies=1, cutover_capture=f.request)
    assert f.calls == calls and not f.pruning
    assert (f.previous/"complete.json").read_bytes() == f.original_receipt


@pytest.mark.parametrize("generation,previous,reserve", [
    ("../copy_x", "copy_"+"1"*32, 100), ("copy_"+"1"*32, "copy_"+"1"*32, 100),
    ("copy_"+"2"*32, "copy_"+"1"*32, 0), ("copy_"+"2"*32, "copy_"+"1"*32, True),
])
def test_cutover_request_requires_explicit_distinct_names_and_source_floor(generation, previous, reserve):
    with pytest.raises(ValueError, match="cutover_request_invalid"):
        CutoverCapture(generation, previous, reserve)
