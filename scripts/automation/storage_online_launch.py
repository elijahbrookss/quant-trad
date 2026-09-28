"""Explicit-mount launcher for the fixed online capture and copy controller.

No chown, publisher pause, database switch or runtime activation.
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
import sys
import time
from urllib.parse import quote, unquote, urlsplit

from scripts.automation import storage_host_boundary as host_boundary
from scripts.automation.storage_online_worker import archive_group_override, capture_preparation, admit_capture

_COMMAND = ["-m", "scripts.automation.storage_online_worker"]
_CAPS = ["DAC_READ_SEARCH", "SETGID", "SETUID"]
_TMPFS = {"/tmp": "rw,nosuid,nodev,size=67108864,uid=70,gid=70,mode=1770",
          "/app/logs": "rw,nosuid,nodev,size=16777216,uid=70,gid=70,mode=0750"}
_STATE = "storage-online-worker.json"


def _retire_worker(process, identity, binding, contract):
    """Reap the attach CLI AND verify the exact read-capability worker stopped.

    A detached/failed CLI is not daemon completion. Keep the launcher's existing
    lock throughout this cleanup. The old 10+15+10 second cleanup ceiling is one
    absolute bound, never a new source-stop, copy, switch or recovery allowance.
    Failure leaves retirement unproven; no recovery mount authority is returned.
    """
    deadline = time.monotonic()+35
    def remaining(limit):
        value = min(limit, deadline-time.monotonic())
        if value <= 0:
            raise RuntimeError("storage_online_worker_retirement_expired")
        return value
    def observe():
        _admit(identity, binding, contract)
        return json.loads(host_boundary.docker("inspect", "--format", "{{json .State}}", identity))
    try:
        if process.stdin:
            try:
                process.stdin.close()
            except BrokenPipeError:
                print("event=storage_online_worker_input_closed reason=broken_pipe",
                      file=sys.stderr, flush=True)
        with host_boundary.docker_deadline(deadline):
            try:
                process.wait(timeout=remaining(10))
            except subprocess.TimeoutExpired:
                pass  # Inspect the daemon; CLI lifetime does not prove retirement.
            current = observe()
            if current.get("Running") is True:
                # Only the exact admitted migration worker. Its source peers
                # remain serving/held according to their unchanged final intent.
                host_boundary.docker("stop", "--time", "5", identity, timeout=remaining(15))
            if process.poll() is None:
                process.wait(timeout=remaining(10))
            current = observe()
            if (current.get("Running") is not False or current.get("Paused") is not False
                    or current.get("Restarting") is not False or current.get("Dead") is not False
                    or type(current.get("Pid")) is not int or current["Pid"] != 0
                    or current.get("Status") not in {"created", "exited"}):
                raise RuntimeError("storage_online_worker_retirement_unproven")
            remaining(1)
            print("event=storage_online_worker_retired recovery_activation_authorized=false",
                  file=sys.stderr, flush=True)
    finally:
        # This only reaps the owned local CLI, including failed daemon cleanup.
        # It never claims that killing the CLI cancels a daemon operation.
        if process.poll() is None:
            process.kill()
        process.wait(timeout=1)
        if process.stdout:
            process.stdout.close()


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
    details = host_boundary.database_details(identity)
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
        and host_boundary.digest(sorted(config["Env"])) == binding["environment_sha256"]
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
    contract = host_boundary.database_contract(normalized)
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


def _capture_observation(database_id):
    if host_boundary.database_query(database_id,
            "SELECT to_regclass('qt_fact_header_cutover_v2.capture') IS NOT NULL").strip() != "t":
        return None
    return json.loads(host_boundary.database_query(database_id,
        "SELECT json_build_object('started_at',prepared_at,'seconds',"
        "COALESCE((to_jsonb(c)->>'attempt_seconds')::int,86400))::text "
        "FROM qt_fact_header_cutover_v2.capture c WHERE id=1"))


def observe_owned_worker(state_root, project):
    """Resolve only the existing named worker journal; no launch/reentry authority."""
    state_path = state_root/_STATE
    saved = host_boundary.load_receipt(state_path) if os.path.lexists(state_path) else None
    name = project+"-storage-online"
    found = host_boundary.docker("ps", "-aq", "--no-trunc", "--filter", "name=^/"+name+"$").split()
    if len(found) > 1 or (found and (not saved or saved.get("container_id") not in (None, found[0]))):
        raise RuntimeError("storage_online_unowned_container")
    return saved, name, found


@contextmanager
def launched_online_worker_locked(state_root, *, project, source_revision, image,
                           request, inventory_path, descriptor_limit, memory_bytes):
    """Retain one worker while the caller owns the continuous deployment lock.

    The caller may keep that lock after this context verifies worker retirement
    for a separately admitted preserving recovery transition. This function
    never activates recovery or restarts source. Limits remain explicit inputs,
    not measured production admission. The ordinary wrapper below owns its lock.
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
    archive_group = archive_group_override(request)
    capture_plan = capture_preparation(request)
    data = (json.dumps(request, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode()
    if len(data) > 65536:
        raise ValueError("storage_online_request_budget_exceeded")
    digest = hashlib.sha256(data).hexdigest()
    if os.path.lexists(state_root/"storage-online-final.json"):
        raise RuntimeError("storage_online_final_requires_reconciliation")
    for name in ("promotion.env", "alert-preview.env"):
        if os.path.lexists(state_root/name):
            raise RuntimeError("storage_online_unfinished_host_operation")
    if re.findall(r"^current_revision=(.*)$", (state_root/"release.env").read_text(),
                  flags=re.MULTILINE) != [source_revision]:
        raise RuntimeError("storage_online_source_release_changed")
    state_path = state_root/_STATE
    saved, name, found = observe_owned_worker(state_root, project)
    if (os.path.lexists(state_root/host_boundary.HOLD)
            or os.path.lexists(state_root/"storage-online-preparation.json")):
        from scripts.automation.storage_online_prepare import admit_serving_source
        admit_serving_source(state_root, project=project, source_revision=source_revision,
                             operator_id=found[0] if found else None)
    rows = host_boundary.inventory(project, operator_id=found[0] if found else None)
    if not host_boundary.source_clients_serving(rows):
        raise RuntimeError("storage_online_serving_source_required")
    identities = host_boundary.identities(rows)
    database_id = rows["tsdb"]["id"]
    database = host_boundary.database_details(database_id)
    collector = host_boundary.database_details(rows["market-data-collector"]["id"])
    dsn = _dsn(database, collector)
    source_env = dict(v.split("=", 1) for v in collector["config"].get("Env") or [])
    if source_env.get("QT_IMAGE_SOURCE_REVISION") != source_revision:
        raise RuntimeError("storage_online_serving_image_revision_changed")
    image_info = json.loads(host_boundary.docker("image", "inspect", "--format", "{{json .}}", image))
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
        host_boundary.sync_directory(state_root)
    if (request_path.is_symlink() or request_path.read_bytes() != data
            or request_path.stat().st_uid != os.getuid()
            or stat.S_IMODE(request_path.stat().st_mode) != 0o600):
        raise RuntimeError("storage_online_saved_request_changed")
    udev = _canonical(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
    mounts, working = _explicit_mounts(database, collector, inventory_path, request_path, udev)
    source = _canonical(working/"objects").stat()
    if (source.st_dev, source.st_ino) != (request.get("source_device"), request.get("source_inode")):
        raise RuntimeError("storage_online_source_root_changed")
    # The optional first capture stays inside the ORIGINAL initial preparation
    # window. The capture itself owns the later cumulative migration clock.
    if capture_plan is not None:
        from scripts.automation.storage_online_prepare import admit_serving_source
        preparation = admit_serving_source(state_root, project=project,
            source_revision=source_revision, operator_id=found[0] if found else None)
        if (capture_plan["deadline"] != preparation["deadline"]
                or not preparation["completed_at"] <= capture_plan["requested_at"] <= time.time()):
            raise RuntimeError("storage_online_capture_preparation_binding_changed")
        observed = _capture_observation(database_id)
        if saved is None and observed is not None:
            raise RuntimeError("storage_online_initial_capture_already_exists")
        if observed is None:
            if capture_plan["deadline"] <= time.time() or (saved and saved.get("capture") is not None):
                raise RuntimeError("storage_online_capture_preparation_expired_or_missing")
            deadline = None
        else:
            deadline = admit_capture(request, observed)
    else:
        observed = json.loads(host_boundary.database_query(database_id,
            "SELECT json_build_object('started_at',prepared_at,'seconds',"
            "COALESCE((to_jsonb(c)->>'attempt_seconds')::int,86400))::text "
            "FROM qt_fact_header_cutover_v2.capture c WHERE id=1"))
        deadline = admit_capture(request, observed)
    overrides = {"PG_DSN": dsn, "QT_DISABLE_DOTENV": "1",
                 "QT_ARCHIVE_SHARED_GROUP_ID": archive_group, "QT_LOGGING_LOKI_URL": "",
                 "QT_ONLINE_REQUEST_SHA256": digest, "QT_STORAGE_UDEV_ROOT": "/run/qt-online/udev"}
    binding = dict(project=project, source_revision=source_revision, clients=identities,
        image=image, database_id=database_id, database_hostname=database["config"]["Hostname"],
        database_contract=host_boundary.database_contract(database),
        collector_contract=host_boundary.database_contract(collector),
        request_sha256=digest, inventory_sha256=hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
        mounts=mounts, descriptor_limit=descriptor_limit, memory_bytes=memory_bytes,
        environment_sha256=host_boundary.digest(sorted(k+"="+v for k,v in {**image_env,**overrides}.items())))
    if saved:
        expected_fields = {"binding", "container_id", "contract", "deadline"}
        if capture_plan is not None:
            expected_fields.add("capture")
        if (set(saved) != expected_fields or saved["binding"] != binding
                or (saved["deadline"] is not None and saved["deadline"] != deadline)
                or (capture_plan is not None and saved["capture"] is not None and saved["capture"] != observed)
                or (saved["container_id"] is not None and not found)):
            raise RuntimeError("storage_online_saved_launch_changed")
    else:
        saved = dict(binding=binding, container_id=None, contract=None, deadline=deadline)
        if capture_plan is not None:
            saved["capture"] = None
        # This is the existing durable worker intent, before any Docker start.
        host_boundary.save_receipt(state_path, saved, initial=True)
    if not found:
        found = [host_boundary.docker(*_arguments(name, image, database_id, mounts, overrides,
                 descriptor_limit, memory_bytes, digest), env={**os.environ,"PG_DSN":dsn}).strip()]
    contract = _admit(found[0], binding, saved["contract"])
    saved.update(container_id=found[0], contract=contract)
    host_boundary.save_receipt(state_path, saved, initial=False)
    current = json.loads(host_boundary.docker("inspect", "--format", "{{json .State}}", found[0]))
    if current["Running"] or current["Paused"] or current["Restarting"] or current["OOMKilled"]:
        raise RuntimeError("storage_online_existing_worker_requires_reconciliation")
    if (host_boundary.identities(host_boundary.inventory(project, operator_id=found[0])) != identities
            or hashlib.sha256(inventory_path.read_bytes()).hexdigest() != binding["inventory_sha256"]):
        raise RuntimeError("storage_online_source_changed_before_start")
    # The private DSN exists only in Docker's environment; command arguments
    # and durable receipts contain hashes, never the resolved secret.
    process = subprocess.Popen(["docker", "start", "--attach", "--interactive", found[0]],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=None, bufsize=0)
    try:
        if capture_plan is not None:
            startup_deadline = time.monotonic()+max(0, capture_plan["deadline"]-time.time())
            while observed is None:
                if process.poll() is not None:
                    raise RuntimeError("storage_online_capture_worker_exited")
                if time.monotonic() >= startup_deadline or time.time() >= capture_plan["deadline"]:
                    raise RuntimeError("storage_online_capture_preparation_expired")
                with host_boundary.docker_deadline(startup_deadline):
                    admit_serving_source(state_root, project=project,
                        source_revision=source_revision, operator_id=found[0])
                    _admit(found[0], binding, contract)
                    observed = _capture_observation(database_id)
                if observed is None:
                    time.sleep(min(.1, max(0, startup_deadline-time.monotonic())))
            deadline = admit_capture(request, observed)
            # Exact capture identity, not process exit or a saved negative read.
            # Lost reply before this write retains the initial intent; a later
            # admission must inspect the same persisted capture and owned worker.
            saved.update(capture=observed, deadline=deadline)
            host_boundary.save_receipt(state_path, saved, initial=False)
        yield process, {"container_id": found[0], "request_sha256": digest,
                        "deadline": deadline, "source_clients_unchanged": True,
                        "final_switch_authorized": False}
    finally:
        _retire_worker(process, found[0], binding, contract)
        if host_boundary.identities(host_boundary.inventory(project, operator_id=found[0])) != identities:
            raise RuntimeError("storage_online_source_changed_during_worker")


@contextmanager
def launched_online_worker(state_root, *, project, source_revision, image,
                           request, inventory_path, descriptor_limit, memory_bytes):
    """Standalone worker lifetime under the same existing deployment lock."""
    state_root = _canonical(state_root)
    with host_boundary.deployment_lock(state_root):
        with launched_online_worker_locked(state_root, project=project,
                source_revision=source_revision, image=image, request=request,
                inventory_path=inventory_path, descriptor_limit=descriptor_limit,
                memory_bytes=memory_bytes) as worker:
            yield worker
