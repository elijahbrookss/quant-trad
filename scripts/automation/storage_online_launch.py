"""Explicit-mount launcher for an already prepared online storage controller.

No preparation, chown, publisher pause, database switch or runtime activation.
The caller retains this context and its deployment lock while driving the
bounded worker pipe. Interrupted background work retains its original attempt.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
from urllib.parse import quote, unquote, urlsplit

from scripts.automation import storage_handoff_pause as held

_COMMAND = ["-m", "scripts.automation.storage_online_worker"]
_CAPS = ["DAC_READ_SEARCH", "SETGID", "SETUID"]
_TMPFS = {"/tmp": "rw,nosuid,nodev,size=67108864,uid=70,gid=70,mode=1770",
          "/app/logs": "rw,nosuid,nodev,size=16777216,uid=70,gid=70,mode=0750"}
_STATE = "storage-online-worker.json"


def _canonical(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path or "," in str(path):
        raise ValueError("storage_online_canonical_host_path_required")
    return path


def _overlap(first, second):
    return first == second or first.is_relative_to(second) or second.is_relative_to(first)


def _explicit_mounts(database, collector, inventory_path, request_path, udev):
    """Select only required roots, never inherit the database's secret mounts."""
    by_target = {}
    for mount in database["mounts"]:
        target = mount["Destination"]
        if target in by_target:
            raise RuntimeError("storage_online_duplicate_database_mount")
        by_target[target] = mount
    # Physical placement verification follows /proc/<postgres>/root. With the
    # read capability, omitting a peer mount from this container is insufficient:
    # that same mount can remain reachable through the admitted PID namespace.
    # No recovery key/config/socket mount is admitted by this online launcher.
    if set(by_target) != {"/var/lib/postgresql/data", "/qt-history"}:
        raise RuntimeError("storage_online_peer_mount_exposure_refused")
    result = {}
    for target in ("/var/lib/postgresql/data", "/qt-history"):
        mount = by_target.get(target)
        if not mount or mount["Type"] not in ("bind", "volume") or not mount["RW"]:
            raise RuntimeError("storage_online_prepared_database_mount_required")
        if any(other != target and Path(other).is_relative_to(Path(target))
               for other in by_target):
            raise RuntimeError("storage_online_nested_database_mount_refused")
        # Mount the exact volume or bind, not every mount owned by the peer.
        source = mount["Name"] if mount["Type"] == "volume" else mount["Source"]
        if not source or "," in source:
            raise RuntimeError("storage_online_mount_source_invalid")
        result[target] = {"type": mount["Type"], "source": source,
                          "host_source": mount["Source"], "readonly": False}
    working = [m for m in collector["mounts"]
               if m["Destination"] == "/app/logs/market-structure"]
    if len(working) != 1 or working[0]["Type"] != "bind" or not working[0]["RW"]:
        raise RuntimeError("storage_online_existing_source_bind_required")
    source = _canonical(working[0]["Source"])
    for value in result.values():
        if _overlap(source, Path(value["host_source"])):
            raise RuntimeError("storage_online_writable_source_alias_refused")
    result["/app/logs/market-structure"] = {
        "type": "bind", "source": str(source), "host_source": str(source), "readonly": True}
    for target, path in (("/run/qt-online/inventory.json", inventory_path),
                         ("/run/qt-online/request.json", request_path),
                         ("/run/qt-online/udev", udev)):
        path = _canonical(path)
        if any(_overlap(path, Path(v["host_source"])) for v in result.values()):
            raise RuntimeError("storage_online_control_mount_overlap")
        result[target] = {"type": "bind", "source": str(path),
                          "host_source": str(path), "readonly": True}
    return result, source


def _arguments(name, image, database_id, mounts, overrides, descriptor_limit, memory_bytes, digest):
    args = ["create", "--name", name, "--pull", "never", "--user", "0:0", "--init",
            "--restart", "no", "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--memory", str(memory_bytes),
            "--memory-swap", str(memory_bytes), "--cpus", "2", "--pids-limit", "128",
            "--ulimit", f"nofile={descriptor_limit}:{descriptor_limit}",
            "--network", "container:"+database_id, "--pid", "container:"+database_id,
            "--entrypoint", "python", "--interactive",
            "--label", "qt.storage.online="+digest]
    for capability in _CAPS:
        args += ["--cap-add", capability]
    for target, options in _TMPFS.items():
        args += ["--tmpfs", target+":"+options]
    for target, mount in mounts.items():
        args += ["--mount", "type="+mount["type"]+",source="+mount["source"]+
                 ",target="+target+(",readonly" if mount["readonly"] else "")]
    for key, value in overrides.items():
        args += ["--env", key if key == "PG_DSN" else key+"="+value]
    return args+[image, *_COMMAND]


def _admit(identity, binding, previous=None):
    details = held._database_details(identity)
    config, host = details["config"], details["host"]
    mounts = [m for m in details["mounts"] if m["Type"] != "tmpfs"]
    observed = {m["Destination"]: [m["Type"], m["Source"], m["RW"]] for m in mounts}
    expected = {target: [m["type"], m["host_source"], not m["readonly"]]
                for target, m in binding["mounts"].items()}
    limits = host.get("Ulimits") or []
    valid = (
        details["image"] == binding["image"] and config["User"] == "0:0"
        and config["Entrypoint"] == ["python"] and config["Cmd"] == _COMMAND
        and config.get("OpenStdin") is True and not config.get("Tty")
        and config["Hostname"] in (identity[:12], binding["database_hostname"])
        and config["Labels"].get("qt.storage.online") == binding["request_sha256"]
        and held._digest(sorted(config["Env"])) == binding["environment_sha256"]
        and host["NetworkMode"] == "container:"+binding["database_id"]
        and host["PidMode"] == "container:"+binding["database_id"]
        and host["ReadonlyRootfs"] and not host["Privileged"]
        and host["RestartPolicy"]["Name"] == "no" and host["Init"] is True
        and host["CapDrop"] == ["ALL"] and sorted(c.removeprefix("CAP_") for c in host.get("CapAdd") or []) == _CAPS
        and host["SecurityOpt"] == ["no-new-privileges"]
        and not host.get("Devices") and not host.get("DeviceRequests")
        and not host.get("GroupAdd") and not host.get("Sysctls")
        and not host.get("VolumesFrom")
        and host["Memory"] == host["MemorySwap"] == binding["memory_bytes"]
        and host["NanoCpus"] == 2*10**9 and host["PidsLimit"] == 128
        and limits == [{"Name": "nofile", "Hard": binding["descriptor_limit"],
                        "Soft": binding["descriptor_limit"]}]
        and host["Tmpfs"] == _TMPFS
        and len(mounts) == len(observed) == len(expected) and observed == expected
        and {m["Destination"] for m in details["mounts"] if m["Type"] == "tmpfs"} <= set(_TMPFS))
    normalized = {**details, "config": {**config, "Hostname": binding["database_hostname"]}}
    contract = held._database_contract(normalized)
    if not valid or (previous is not None and previous != contract):
        raise RuntimeError("storage_online_container_binding_changed")
    return contract


def _dsn(database, collector):
    db_env = dict(v.split("=", 1) for v in database["config"]["Env"])
    old_env = dict(v.split("=", 1) for v in collector["config"].get("Env") or [])
    url = urlsplit(old_env.get("PG_DSN", ""))
    if (url.scheme != "postgresql+psycopg2" or url.hostname not in ("tsdb", "tsdb.quanttrad")
            or url.port not in (None, 5432) or url.query or url.fragment
            or unquote(url.username or "") != db_env["POSTGRES_USER"]
            or unquote(url.password or "") != db_env["POSTGRES_PASSWORD"]
            or unquote(url.path.removeprefix("/")) != db_env["POSTGRES_DB"]
            or db_env.get("PGDATA") != "/var/lib/postgresql/data"):
        raise RuntimeError("storage_online_database_connection_binding_changed")
    return ("postgresql+psycopg2://"+quote(db_env["POSTGRES_USER"], safe="")+":"+
            quote(db_env["POSTGRES_PASSWORD"], safe="")+"@127.0.0.1:5432/"+
            quote(db_env["POSTGRES_DB"], safe=""))


@contextmanager
def launched_online_worker(state_root, *, project, source_revision, image,
                           request, inventory_path, descriptor_limit, memory_bytes):
    """Launch only background commands beside the exact existing source clients.

    The descriptor/cgroup limits are explicit inputs, not measured admission by
    themselves. Caller must qualify them before production. No stopped source
    is restarted, no capture is created, and no host preparation is performed.
    """
    state_root, inventory_path = _canonical(state_root), _canonical(inventory_path)
    if (not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project)
            or not re.fullmatch(r"[0-9a-f]{40}", source_revision)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", image)
            or not isinstance(request, dict)
            or type(request.get("max_objects")) is not int
            or type(descriptor_limit) is not int
            or not request["max_objects"]+512 <= descriptor_limit <= 1_001_024
            or type(memory_bytes) is not int or not 512*1024**2 <= memory_bytes <= 8*1024**3):
        raise ValueError("storage_online_launch_inputs_invalid")
    data = (json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode()
    if len(data) > 65536:
        raise ValueError("storage_online_request_budget_exceeded")
    digest = hashlib.sha256(data).hexdigest()
    with held._deployment_lock(state_root):
        for name in (held.HOLD, "promotion.env", "alert-preview.env"):
            if os.path.lexists(state_root/name):
                raise RuntimeError("storage_online_unfinished_host_operation")
        if re.findall(r"^current_revision=(.*)$", (state_root/"release.env").read_text(),
                      flags=re.MULTILINE) != [source_revision]:
            raise RuntimeError("storage_online_source_release_changed")
        state_path = state_root/_STATE
        saved = held._load(state_path) if os.path.lexists(state_path) else None
        name = project+"-storage-online"
        found = held._docker("ps", "-aq", "--no-trunc", "--filter", "name=^/"+name+"$").split()
        if len(found) > 1 or (found and (not saved or saved.get("container_id") not in (None, found[0]))):
            raise RuntimeError("storage_online_unowned_container")
        rows = held._inventory(project, operator_id=found[0] if found else None)
        if any(not rows[service]["running"] for service in held.STOP):
            raise RuntimeError("storage_online_serving_source_required")
        identities = held._identities(rows)
        database_id = rows["tsdb"]["id"]
        database = held._database_details(database_id)
        collector = held._database_details(rows["market-data-collector"]["id"])
        dsn = _dsn(database, collector)
        source_env = dict(v.split("=", 1) for v in collector["config"].get("Env") or [])
        if source_env.get("QT_IMAGE_SOURCE_REVISION") != source_revision:
            raise RuntimeError("storage_online_serving_image_revision_changed")
        image_info = json.loads(held._docker("image", "inspect", "--format", "{{json .}}", image))
        image_env = dict(v.split("=", 1) for v in image_info["Config"].get("Env") or [])
        if (image_info["Id"] != image
                or image_env.get("QT_IMAGE_SOURCE_REVISION") != request.get("source_revision")
                or image_env.get("QT_IMAGE_SOURCE_TREE_HASH") != request.get("source_tree_hash")):
            raise RuntimeError("storage_online_candidate_image_changed")
        request_path = state_root/"storage-online-request.json"
        if not os.path.lexists(request_path):
            descriptor = os.open(request_path, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data); stream.flush(); os.fsync(stream.fileno())
            held._sync_directory(state_root)
        if (request_path.is_symlink() or request_path.read_bytes() != data
                or request_path.stat().st_uid != os.getuid()
                or stat.S_IMODE(request_path.stat().st_mode) != 0o600):
            raise RuntimeError("storage_online_saved_request_changed")
        udev = _canonical(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
        mounts, working = _explicit_mounts(database, collector, inventory_path, request_path, udev)
        source = _canonical(working/"objects").stat()
        if (source.st_dev, source.st_ino) != (request.get("source_device"), request.get("source_inode")):
            raise RuntimeError("storage_online_source_root_changed")
        # Derive the host ceiling from the SAME original persisted attempt.
        observed = json.loads(held._database_query(database_id,
            "SELECT json_build_object('started_at',prepared_at,'seconds',"
            "COALESCE((to_jsonb(c)->>'attempt_seconds')::int,86400))::text "
            "FROM qt_fact_header_cutover_v2.capture c WHERE id=1"))
        started = datetime.fromisoformat(observed["started_at"])
        if (started.tzinfo is None
                or started != datetime.fromisoformat(request["expected_started_at"])
                or type(observed["seconds"]) is not int or not 1 <= observed["seconds"] <= 96*3600):
            raise RuntimeError("storage_online_original_attempt_changed")
        deadline = started.timestamp()+observed["seconds"]
        if deadline <= time.time():
            raise RuntimeError("storage_online_original_attempt_expired")
        overrides = {"PG_DSN": dsn, "QT_DISABLE_DOTENV": "1", "QT_LOGGING_LOKI_URL": "",
                     "QT_ONLINE_REQUEST_SHA256": digest, "QT_STORAGE_UDEV_ROOT": "/run/qt-online/udev"}
        binding = dict(project=project, source_revision=source_revision, clients=identities,
            image=image, database_id=database_id, database_hostname=database["config"]["Hostname"],
            database_contract=held._database_contract(database),
            collector_contract=held._database_contract(collector),
            request_sha256=digest, inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
            mounts=mounts, descriptor_limit=descriptor_limit, memory_bytes=memory_bytes,
            environment_sha256=held._digest(sorted(k+"="+v for k,v in {**image_env,**overrides}.items())))
        if saved:
            if (set(saved) != {"binding", "container_id", "contract", "deadline"}
                    or saved["binding"] != binding or saved["deadline"] != deadline
                    or (saved["container_id"] is not None and not found)):
                raise RuntimeError("storage_online_saved_launch_changed")
        else:
            saved = dict(binding=binding, container_id=None, contract=None, deadline=deadline)
            held._save(state_path, saved, initial=True)
        if not found:
            found = [held._docker(*_arguments(name, image, database_id, mounts, overrides,
                     descriptor_limit, memory_bytes, digest), env={**os.environ,"PG_DSN":dsn}).strip()]
        contract = _admit(found[0], binding, saved["contract"])
        saved.update(container_id=found[0], contract=contract)
        held._save(state_path, saved, initial=False)
        current = json.loads(held._docker("inspect", "--format", "{{json .State}}", found[0]))
        if current["Running"] or current["Paused"] or current["Restarting"] or current["OOMKilled"]:
            raise RuntimeError("storage_online_existing_worker_requires_reconciliation")
        if (held._identities(held._inventory(project, operator_id=found[0])) != identities
                or hashlib.sha256(inventory_path.read_bytes()).hexdigest() != binding["inventory_sha256"]):
            raise RuntimeError("storage_online_source_changed_before_start")
        # The private DSN exists only in Docker's environment; command arguments
        # and durable receipts contain hashes, never the resolved secret.
        process = subprocess.Popen(["docker", "start", "--attach", "--interactive", found[0]],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=None, bufsize=0)
        try:
            yield process, {"container_id": found[0], "request_sha256": digest,
                            "deadline": deadline, "source_clients_unchanged": True,
                            "final_switch_authorized": False}
        finally:
            if process.stdin:
                process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # This wire cannot switch. Stopping this exact background worker
                # closes proof and leaves source clients/data intact.
                held._docker("stop", "--time", "5", found[0], timeout=15)
                process.wait(timeout=10)
            finally:
                if process.stdout:
                    process.stdout.close()
            _admit(found[0], binding, contract)
            if held._identities(held._inventory(project, operator_id=found[0])) != identities:
                raise RuntimeError("storage_online_source_changed_during_worker")
