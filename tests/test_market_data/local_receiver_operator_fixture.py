"""Drive the standalone receiver inside its network-disabled test container."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep

from scripts.automation.canonical_cold_named_recovery import RecoverySitePolicy, receiver_host_sha256
from scripts.automation.canonical_cold_receiver_operator import PLAN_VERSION
from scripts.automation.canonical_cold_recovery_preflight import ReceiverPath, measure_receiver_preflight
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits
from scripts.automation.storage_host_boundary import load_receipt


CONTROL = Path("/qt-restore/receiver-cli")
ARCHIVE_STORAGE = Path("/qt-archive-input")
RECEIVER_MACHINE_ID = b"8c98330843e24928992933cadcf07a42"
RECEIVER_HOST = hashlib.sha256(b"qt.recovery.receiver.machine-id.v1\0" + RECEIVER_MACHINE_ID).hexdigest()
FILESYSTEM_UUID = "postwrite-independent-fixture"


def publish_bytes(path, content):
    with TemporaryDirectory(prefix=".fixture-", dir=path.parent) as directory:
        temporary = Path(directory) / path.name
        with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish(path, value):
    content = (json.dumps(value, sort_keys=True) + "\n").encode()
    publish_bytes(path, content)
    return hashlib.sha256(content).hexdigest()


class LocalReceiverOperator:
    def __init__(self, *, workspace, pgdata, tablespace):
        self.workspace, self.pgdata, self.tablespace = workspace, pgdata, tablespace
        self.socket = workspace / "socket"
        self.archive = CONTROL.parent / "operator-archive"
        self.inbox = CONTROL / "approval"
        self.log = CONTROL / "output.log"
        self.status = CONTROL / "exit.status"
        for path in (CONTROL, self.inbox, self.socket, self.archive):
            path.mkdir(mode=0o700)
        self.pgdata_identity = self._identity(pgdata)

    @staticmethod
    def _identity(path):
        info = path.stat()
        return info.st_dev, info.st_ino

    def _read_log(self):
        if not self.log.exists():
            return ""
        with self.log.open("rb") as handle:
            size = os.fstat(handle.fileno()).st_size
            handle.seek(max(0, size - 65_536))
            return handle.read(65_536).decode(errors="replace")

    def check(self):
        assert self._identity(self.pgdata) == self.pgdata_identity
        if self.status.exists() and self.status.read_text().strip() != "0":
            raise AssertionError(f"standalone receiver failed\n{self._read_log()[-6000:]}")

    def _wait(self, observe, *, label, seconds):
        deadline = monotonic() + seconds
        while monotonic() < deadline:
            self.check()
            value = observe()
            if value is not None:
                return value
            if self.status.exists():
                raise AssertionError(f"standalone receiver exited before {label}\n{self._read_log()[-6000:]}")
            sleep(0.1)
        raise TimeoutError(f"standalone receiver deadline for {label}\n{self._read_log()[-6000:]}")

    def phase(self, expected, *, seconds=150):
        def observe():
            for line in self._read_log().splitlines():
                if not line.startswith("{"):
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict) and value.get("phase") == expected:
                    return value
            return None

        value = self._wait(observe, label=expected, seconds=seconds)
        print(f"QT_STANDALONE_RECEIVER_PHASE {json.dumps(value, sort_keys=True)}", flush=True)
        return value

    def preflight(self, *, policy, receiver_signing_key, worker, pair):
        path = CONTROL / "preflight.json"
        _publish(CONTROL / "preflight-request.json", {
            "site": asdict(policy) | {"receiving": str(policy.receiving)},
            "receiver_signing_key": str(receiver_signing_key), "expected_host_sha256": RECEIVER_HOST,
            "source_host_sha256": receiver_host_sha256(), "source_filesystem_uuid": pair["filesystem_uuid"],
            "workspace": str(self.workspace), "pgdata": str(self.pgdata), "tablespace": str(self.tablespace),
            "pgbackrest": str(worker.pgbackrest), "restic": str(worker.restic),
            "database_key": str(worker.config.database_key_path), "archive_key": str(worker.config.archive_key_path),
        })
        envelope = self._wait(lambda: load_receipt(path) if path.exists() else None, label="preflight", seconds=60)
        assert envelope["body"]["receiver_host_sha256"] == RECEIVER_HOST
        assert envelope["body"]["source_host_sha256"] != RECEIVER_HOST
        return envelope

    def start(self, *, policy, measured, credentials, target, role, role_oid, tablespace_oid):
        source_archive = ARCHIVE_STORAGE / "objects"
        source_device, source_inode = self._identity(source_archive)
        postgres, controldata = Path("/native/bin/postgres"), Path("/native/bin/pg_controldata")
        plan = {"schema": PLAN_VERSION, "site": asdict(policy) | {"receiving": str(policy.receiving)},
                "receiver_preflight": measured, "paths": {
                    "workspace": str(self.workspace), "pgdata": str(self.pgdata), "socket": str(self.socket),
                    "tablespaces": {"qt_history_hdd-01": str(self.tablespace)}, "archive_source": str(source_archive),
                    "archive_root": str(self.archive), "approval_inbox": str(self.inbox),
                }, "tools": {
                    "postgres": str(postgres), "postgres_sha256": hashlib.sha256(postgres.read_bytes()).hexdigest(),
                    "pg_controldata": str(controldata), "pg_controldata_sha256": hashlib.sha256(controldata.read_bytes()).hexdigest(),
                    "pgbackrest": "/usr/local/bin/pgbackrest", "restic": "/usr/local/bin/restic",
                }, "credentials": {key: str(value) for key, value in credentials.items()}, "replay": {
                    "database": target["database"], "role": role, "role_oid": role_oid, "port": 55492,
                    "tablespace_oids": {"qt_history_hdd-01": tablespace_oid}, "source_device": source_device,
                    "source_inode": source_inode, "archive_max_work_bytes": 8 * 1024**2,
                }, "runtime": {
                    "source_checkout": os.environ["QT_EXACT_OLD_SOURCE_ROOT"],
                    "expected_revision": "afafda39b4347adf8c88d01df6ae4b849796935a",
                    "expected_source_tree_sha256": os.environ["QT_EXACT_OLD_SOURCE_HASH"], "timeout_seconds": 70,
                }, "lifecycle": {"prepare_seconds": 150, "approval_seconds": 90, "ready_seconds": 120,
                                 "recover_seconds": 105, "max_custody_entries": 10_000}}
        digest = _publish(CONTROL / "plan.json", plan)
        publish_bytes(CONTROL / "start.sha256", (digest + "\n").encode())
        return digest

    def finish(self):
        self._wait(lambda: self.status.read_text().strip() if self.status.exists() else None,
                   label="clean operator exit", seconds=15)
        assert self.status.read_text().strip() == "0"
        assert not (self.pgdata / "postmaster.pid").exists() and not (self.pgdata / "recovery.signal").exists()


def _measure(path):
    request = load_receipt(path)
    site = request["site"]
    limits = {name: TransferLimits(**site[name]) for name in ("wal", "archive", "pending", "database")}
    policy = RecoverySitePolicy(**(site | limits | {"receiving": Path(site["receiving"])}))
    udev = Path(os.environ["QT_STORAGE_UDEV_ROOT"])
    assert udev == CONTROL / "udev" and path == CONTROL / "preflight-request.json"
    udev.mkdir(mode=0o700)

    def mount(path):
        root = Path(path)
        device = root.stat().st_dev
        record = udev / f"b{os.major(device)}:{os.minor(device)}"
        if not record.exists():
            publish_bytes(record, f"E:ID_FS_UUID={FILESYSTEM_UUID}\n".encode())
        return ReceiverPath(root, FILESYSTEM_UUID)

    receipt = measure_receiver_preflight(site=policy, receiver_signing_key=request["receiver_signing_key"],
        expected_host_sha256=request["expected_host_sha256"], source_host_sha256=request["source_host_sha256"],
        source_filesystem_uuids=(request["source_filesystem_uuid"],), receiving=mount(policy.receiving),
        workspace=mount(request["workspace"]), pgdata=mount(request["pgdata"]),
        tablespaces={"qt_history_hdd-01": mount(request["tablespace"])}, pgbackrest=request["pgbackrest"],
        restic=request["restic"], database_key=request["database_key"], archive_key=request["archive_key"],
        pending_expected=True, runtime_bytes=16 * 1024**2, runtime_entries=128, max_manifest_entries=10_000, timeout_seconds=45)
    _publish(CONTROL / "preflight.json", receipt)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3 or sys.argv[1] != "--preflight":
        raise SystemExit("usage: --preflight /qt-restore/receiver-cli/preflight-request.json")
    _measure(Path(sys.argv[2]))
