"""Private deployment configuration for the completed online migration.

The existing operation owns admission and the existing deployer owns release
publication. This internal adapter preserves and publishes their exact environment
handoff under the final-state owner. It never starts services or replays migration.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import logging
import io
import os
from pathlib import Path
import re
import stat
import subprocess
import time
import tempfile

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_handoff_pause as preserving
from scripts.automation import storage_online_runtime as runtime

logger = logging.getLogger(__name__)

SOURCE_ENVIRONMENT = "storage-online-source.env"
PREPARED_ENVIRONMENT = "storage-online-deployment.env"


def read_private_environment(path):
    raw = preserving._runtime_configuration_bytes(path)
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise RuntimeError("storage_online_deployment_environment_not_private")
    return raw


def _environment_bytes(raw, bindings):
    """Preserve unrelated definitions/comments verbatim, including credential bytes.

    Use the existing dotenv parser rather than guessing at multiline quoting.
    Duplicate or malformed input refuses: Compose and application precedence must
    not disagree. New host bindings use the deployer's existing unquoted literal format.
    """
    from dotenv.parser import parse_stream
    try:
        text = raw.decode("utf-8")
    except UnicodeError:
        raise RuntimeError("storage_online_deployment_environment_encoding") from None
    if "\x00" in text:
        raise RuntimeError("storage_online_deployment_environment_invalid")
    seen = set()
    retained = []
    for item in parse_stream(io.StringIO(text)):
        if item.error or item.key is not None and item.key in seen:
            raise RuntimeError("storage_online_deployment_environment_ambiguous")
        if item.key is not None:
            seen.add(item.key)
        if item.key not in bindings and item.key != "QT_STORAGE_SOURCE_FENCE_ROOT":
            retained.append(item.original.string)
    prefix = "".join(retained)
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    for key, value in sorted(bindings.items()):
        if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_./:@+-]+", value)
                or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key)):
            raise RuntimeError("storage_online_deployment_binding_invalid")
        prefix += key + "=" + value + "\n"
    result = prefix.encode()
    if len(result) > 128*1024:
        raise RuntimeError("storage_online_deployment_environment_too_large")
    return result


def _preserve_file(path, raw):
    """Create a private artifact once; interruption or changed bytes never overwrite it."""
    if os.path.lexists(path):
        if read_private_environment(path) != raw:
            raise RuntimeError("storage_online_deployment_artifact_changed")
        return
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    host.sync_directory(path.parent)


def deployment_bindings(state_root, saved, model):
    """Fixed deployment inputs derived from the already admitted runtime."""
    services = model["services"]
    db = services["tsdb"]
    backend = services["backend"]
    maintenance = services["storage-maintenance"]
    literal = lambda value: value.replace("$$", "$")
    def mount(service, target):
        matches = [m["source"] for m in service["volumes"] if m["target"] == target]
        if len(matches) != 1:
            raise RuntimeError("storage_online_deployment_mount_missing")
        return literal(matches[0])
    history = mount(db, "/qt-history")
    environment = backend["environment"]
    group = str(environment["QT_ARCHIVE_SHARED_GROUP_ID"])
    sockets = [str(g) for g in backend["group_add"] if str(g) != group]
    if (len(sockets) != 1 or not sockets[0].isdecimal()
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", db["image"])):
        raise RuntimeError("storage_online_deployment_identity_binding_invalid")
    return {
        "QT_COMPOSE_PROJECT_NAME": saved["binding"]["project"],
        "QT_SINGLE_NODE_STATE_ROOT": str(state_root),
        "QT_STORAGE_HDD_ROOT": history,
        "QT_MARKET_DATA_ROOT": history + "/archives",
        "QT_MARKET_DATA_WORKING_ROOT": saved["runtime_spool"]["destination"],
        "QT_MARKET_DATA_EXPECTED_UUID": environment["QT_MARKET_DATA_EXPECTED_UUID"],
        "QT_MARKET_DATA_WORKING_EXPECTED_UUID": environment["QT_MARKET_DATA_WORKING_EXPECTED_UUID"],
        "QT_STORAGE_INVENTORY_HOST_PATH": mount(backend, "/run/quanttrad/storage-inventory.json"),
        "QT_STORAGE_RECOVERY_SECRETS_ROOT": mount(db, "/run/quanttrad/recovery"),
        "QT_STORAGE_MAINTENANCE_LIMITS_HOST_PATH": mount(maintenance, "/run/quanttrad/storage-maintenance.json"),
        "QT_ARCHIVE_SHARED_GROUP_ID": group,
        "QT_DOCKER_SOCKET_GID": sockets[0],
        "QT_STORAGE_DATABASE_IMAGE": db["image"],
        "QT_STORAGE_POSTGRES_VOLUME": model["volumes"]["postgres-data"]["name"],
        "QT_STORAGE_RECOVERY_SOCKET_VOLUME": model["volumes"]["storage-recovery-socket"]["name"],
        "QT_STORAGE_NETWORK": model["networks"]["quanttrad"]["name"],
    }


def prepare_deployment_environment(state_root, *, environment_path, saved, execute=False):
    """Stage exact resource bindings after fresh completed-runtime/pair admission.

    The caller holds the deployment lock and has freshly verified completion.
    Files are proposals only: normal deployment remains blocked by the retained
    final receipt until canonical configuration and release publication qualify.
    """
    if type(execute) is not bool or saved.get("phase") != "recovery_runtime_ready":
        raise RuntimeError("storage_online_deployment_completion_required")
    state_root = Path(state_root)
    source = Path(environment_path)
    if source in {state_root/SOURCE_ENVIRONMENT, state_root/PREPARED_ENVIRONMENT}:
        raise RuntimeError("storage_online_deployment_environment_alias")
    before = read_private_environment(source)
    model = host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288)
    if host.digest(model) != saved["runtime"]["admission"]["recipe_sha256"]:
        raise RuntimeError("storage_online_deployment_recipe_changed")
    bindings = deployment_bindings(state_root, saved, model)
    prepared = _environment_bytes(before, bindings)
    def check():
        if (read_private_environment(source) != before
                or host.load_receipt(state_root/"storage-online-final.json") != saved
                or host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288) != model):
            raise RuntimeError("storage_online_deployment_input_changed")
    check()
    if execute:
        _preserve_file(state_root/SOURCE_ENVIRONMENT, before)
        _preserve_file(state_root/PREPARED_ENVIRONMENT, prepared)
        check()
    return dict(source_environment_sha256=hashlib.sha256(before).hexdigest(),
        prepared_environment_sha256=hashlib.sha256(prepared).hexdigest(),
        recipe_sha256=host.digest(model), environment_prepared=execute,
        active_environment_changed=False, ordinary_relaunch_authorized=False)


_COMPOSE_FILES = ("docker/docker-compose.server.yml", "docker/docker-compose.storage-server.yml")


def _service_definition(service, *, image_environment):
    """Normalize only Compose defaults and literal escaping, never access controls."""
    value = deepcopy(service)
    environment = dict(image_environment)
    for key, item in value.pop("environment", {}).items():
        if item is None:
            environment.pop(key, None)
        else:
            environment[key] = str(item).replace("$$", "$")
    value["environment"] = environment
    volumes = []
    for original in value.get("volumes", []):
        mount = deepcopy(original)
        if mount.get("read_only") is False:
            mount.pop("read_only")
        if mount.get("volume") == {}:
            mount.pop("volume")
        if "source" in mount:
            mount["source"] = mount["source"].replace("$$", "$")
        volumes.append(mount)
    if len({m["target"] for m in volumes}) != len(volumes):
        raise RuntimeError("storage_online_deployment_duplicate_mount")
    value["volumes"] = sorted(volumes, key=lambda item: item["target"])
    value["networks"] = {k: v or {} for k, v in value.get("networks", {}).items()}
    return value


def compare_deployment_configuration(proposed, *, admitted, bindings, request,
        source_environment, prepared_environment, image_environments):
    """Require preservation of the complete admitted storage-service definitions.

    The public deployer owns the other fixed UI/observability services. No unknown
    drift in SQL settings, credentials, provider flags, privileges, limits, mounts,
    lifecycle, healthchecks or dependencies is accepted for the storage services.
    """
    if proposed.get("name") != admitted["name"]:
        raise RuntimeError("storage_online_deployment_project_changed")
    for key in ("postgres-data", "storage-recovery-socket"):
        if proposed.get("volumes", {}).get(key) != admitted["volumes"][key]:
            raise RuntimeError("storage_online_deployment_volume_changed")
    def network(model):
        value = deepcopy(model["networks"]["quanttrad"])
        if value.get("ipam") == {}:
            value.pop("ipam")
        return value
    if network(proposed) != network(admitted):
        raise RuntimeError("storage_online_deployment_network_changed")
    for name in ("tsdb", *runtime._APPLICATIONS):
        if name not in proposed.get("services", {}):
            raise RuntimeError("storage_online_deployment_service_missing: service="+name)
        current = deepcopy(admitted["services"][name])
        candidate = deepcopy(proposed["services"][name])
        image = current["image"]
        if name == "tsdb":
            if candidate.get("image") != image or "build" in candidate:
                raise RuntimeError("storage_online_deployment_database_image_changed")
        else:
            if candidate.get("image") != "quanttrad-backend:"+request["source_revision"]:
                raise RuntimeError("storage_online_deployment_application_image_changed")
            candidate["image"] = image
            # The exact clean checkout owns builds. Only these three existing
            # services build the shared backend; maintenance consumes that image.
            if name in ("backend", "initialize", "market-data-collector"):
                candidate.pop("build", None)
        if candidate.get("pull_policy") != "never":
            raise RuntimeError("storage_online_deployment_pull_policy_changed")
        for mount in candidate.get("volumes", []):
            if (mount.get("type") == "bind" and mount.get("target") == "/app/secrets.env"
                    and mount.get("source") == str(prepared_environment)):
                mount["source"] = str(source_environment)
            if (mount.get("type") == "bind" and mount.get("source") == "/run/udev"
                    and mount.get("target") == "/run/qt-host-udev" and mount.get("read_only") is True
                    and mount.get("bind") == {"create_host_path":False}):
                mount.update(source="/run/udev/data", target="/run/qt-host-udev/data")
        current = _service_definition(current, image_environment=image_environments[image])
        candidate = _service_definition(candidate, image_environment=image_environments[image])
        expected_env = current["environment"]
        expected_env.update(bindings)
        expected_env.pop("QT_STORAGE_SOURCE_FENCE_ROOT", None)
        # The existing maintenance overlay explicitly uses the HDD as its working
        # filesystem, overriding the host application's SSD binding from env_file.
        if name == "storage-maintenance":
            expected_env["QT_MARKET_DATA_WORKING_EXPECTED_UUID"] = bindings["QT_MARKET_DATA_EXPECTED_UUID"]
        if candidate != current:
            # Names only: differences may include secrets, so never print values.
            changed = sorted(k for k in set(current)|set(candidate) if current.get(k)!=candidate.get(k))
            raise RuntimeError("storage_online_deployment_service_changed: service="+name+" fields="+",".join(changed))
    return dict(storage_configuration_sha256=host.digest(admitted),
                canonical_configuration_sha256=host.digest(proposed))


def deployment_storage_fingerprint(model, *, environment_path, prepared_path):
    """Bind the five storage definitions while unrelated UI profiles stay deployer-owned."""
    value = dict(name=model["name"], services={name: deepcopy(model["services"][name])
        for name in ("tsdb", *runtime._APPLICATIONS)},
        volumes={name: model["volumes"][name] for name in ("postgres-data", "storage-recovery-socket")},
        networks={"quanttrad":model["networks"]["quanttrad"]})
    for service in value["services"].values():
        for mount in service.get("volumes", []):
            if (mount.get("type") == "bind" and mount.get("target") == "/app/secrets.env"
                    and mount.get("source") == str(environment_path)):
                mount["source"] = str(prepared_path)
    return host.digest(value)


def inspect_deployment_configuration(state_root, *, repository, environment_path, saved, timeout_seconds=60):
    """Render only, against the exact checkout and staged private environment.

    Caller owns the deployment lock and has freshly observed runtime/pair health.
    No active file, receipt, image, container, volume or network is modified here.
    """
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("storage_online_deployment_timeout_invalid")
    state_root, repository, environment_path = map(Path, (state_root, repository, environment_path))
    if repository.resolve(strict=True) != repository or not repository.is_absolute():
        raise RuntimeError("storage_online_deployment_repository_alias")
    request = host.load_receipt(state_root/"storage-online-request.json")
    admitted = host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288)
    if host.digest(admitted) != saved["runtime"]["admission"]["recipe_sha256"]:
        raise RuntimeError("storage_online_deployment_recipe_changed")
    deadline = time.monotonic()+timeout_seconds
    def git(*args, strip=True):
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise RuntimeError("storage_online_deployment_inspection_expired")
        result = subprocess.run(["git", "-C", str(repository), *args], capture_output=True,
                                text=True, timeout=remaining)
        if result.returncode:
            raise RuntimeError("storage_online_deployment_checkout_unavailable")
        if len(result.stdout.encode()) > 524288:
            raise RuntimeError("storage_online_deployment_checkout_output_too_large")
        return result.stdout.strip() if strip else result.stdout
    if git("rev-parse", "HEAD") != request["source_revision"] or git("status", "--porcelain=v1"):
        raise RuntimeError("storage_online_deployment_exact_clean_checkout_required")
    from scripts.provenance.source_tree_hash import working_tree_hash
    if working_tree_hash(repository) != request["source_tree_hash"]:
        raise RuntimeError("storage_online_deployment_source_hash_changed")
    def checkout_files():
        # Public tracked source is not a private runtime recipe. Read the exact
        # committed blobs and require the worktree to stay clean around Docker's
        # render; do not change repository permissions to fit a secret-file rule.
        for name in _COMPOSE_FILES:
            path = repository/name
            if path.resolve(strict=True) != path or not path.is_file():
                raise RuntimeError("storage_online_deployment_checkout_file_alias")
        return {name: hashlib.sha256(git("show", "HEAD:"+name, strip=False).encode()).hexdigest()
                for name in _COMPOSE_FILES}
    files = checkout_files()
    original = read_private_environment(environment_path)
    preserved = read_private_environment(state_root/SOURCE_ENVIRONMENT)
    prepared_path = state_root/PREPARED_ENVIRONMENT
    prepared = read_private_environment(prepared_path)
    bindings = deployment_bindings(state_root, saved, admitted)
    if (original not in ((preserved, prepared) if "release" in saved else (preserved,))
            or prepared != _environment_bytes(preserved, bindings)):
        raise RuntimeError("storage_online_deployment_proposal_changed")
    env = {k:v for k,v in os.environ.items() if not k.startswith(("QT_", "PG_", "POSTGRES_", "COMPOSE_"))}
    env.update(QT_SERVER_ENV_FILE=str(prepared_path), QT_SINGLE_NODE_ENV_FILE=str(prepared_path),
               QT_RELEASE_REVISION=request["source_revision"], QT_SOURCE_TREE_HASH=request["source_tree_hash"])
    with host.docker_deadline(deadline):
        image_environments = {}
        for image in sorted({v["image"] for v in admitted["services"].values()}):
            config = json.loads(host.docker("image", "inspect", "--format", "{{json .}}", image))
            if config["Id"] != image:
                raise RuntimeError("storage_online_deployment_local_image_changed")
            image_environments[image] = dict(v.split("=",1) for v in config["Config"].get("Env") or [])
        proposed = json.loads(host.docker("compose", "--env-file", str(prepared_path),
            "--file", str(repository/_COMPOSE_FILES[0]), "--file", str(repository/_COMPOSE_FILES[1]),
            "config", "--format", "json", env=env))
        result = compare_deployment_configuration(proposed, admitted=admitted, bindings=bindings,
            request=request, source_environment=environment_path, prepared_environment=prepared_path,
            image_environments=image_environments)
    if (time.monotonic() >= deadline or git("rev-parse", "HEAD") != request["source_revision"]
            or git("status", "--porcelain=v1") or read_private_environment(environment_path) != original
            or read_private_environment(prepared_path) != prepared
            or host.load_receipt(state_root/"storage-online-final.json") != saved
            or host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288) != admitted
            or host.load_receipt(state_root/"storage-online-request.json") != request
            or checkout_files() != files):
        raise RuntimeError("storage_online_deployment_inspection_changed")
    result = dict(**result, request_sha256=host.digest(request), deployment_storage_sha256=deployment_storage_fingerprint(proposed,
        environment_path=environment_path, prepared_path=prepared_path),
        repository=str(repository), files=files, ordinary_relaunch_authorized=False)
    if "release" in saved and result != saved["release"]["configuration"]:
        raise RuntimeError("storage_online_deployment_configuration_changed")
    return result


SOURCE_RELEASE = "storage-online-source-release.env"
BRIDGE_RELEASE = "storage-online-deployment-release.env"
_RELEASE_FIELDS = {"status", "repository", "environment_path", "candidate_revision", "candidate_source_hash",
    "source_environment_sha256", "prepared_environment_sha256", "source_release_sha256", "bridge_release_sha256",
    "configuration", "requested_at", "published_at", "deployed_release_sha256"}


def validate_release_journal(saved):
    value = saved["release"]
    if not isinstance(value, dict) or set(value) != _RELEASE_FIELDS:
        raise RuntimeError("storage_online_release_journal_invalid")
    configuration = value["configuration"]
    if (not isinstance(configuration, dict) or set(configuration) != {
            "storage_configuration_sha256", "canonical_configuration_sha256", "deployment_storage_sha256", "request_sha256", "repository", "files", "ordinary_relaunch_authorized"}
            or not isinstance(configuration["files"], dict) or set(configuration["files"]) != set(_COMPOSE_FILES)
            or any(not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{64}", v) for v in
                [configuration["storage_configuration_sha256"], configuration["canonical_configuration_sha256"], configuration["deployment_storage_sha256"], configuration["request_sha256"], *configuration["files"].values()])
            or configuration["storage_configuration_sha256"] != saved["runtime"]["admission"]["recipe_sha256"]):
        raise RuntimeError("storage_online_release_configuration_invalid")
    if (saved["phase"] != "recovery_runtime_ready" or not isinstance(value, dict)
            or set(value) != _RELEASE_FIELDS or value["status"] not in {"publishing", "published", "deployed"}
            or not re.fullmatch(r"[0-9a-f]{40}", str(value["candidate_revision"]))
            or any(not re.fullmatch(r"[0-9a-f]{64}", str(value[k])) for k in
                ("candidate_source_hash", "source_environment_sha256", "prepared_environment_sha256",
                 "source_release_sha256", "bridge_release_sha256"))
            or any(not isinstance(value[k],str) or not Path(value[k]).is_absolute() for k in ("repository","environment_path"))
            or type(value["requested_at"]) not in (int,float) or not saved["runtime"]["finished_at"] <= value["requested_at"] < 1e12
            or (value["status"] == "publishing" and value["published_at"] is not None)
            or (value["status"] != "publishing" and (type(value["published_at"]) not in (int,float)
                or not value["requested_at"] <= value["published_at"] < 1e12))
            or (value["status"] == "deployed" and not re.fullmatch(r"[0-9a-f]{64}",str(value["deployed_release_sha256"])))
            or (value["status"] != "deployed" and value["deployed_release_sha256"] is not None)
            or not isinstance(value["configuration"],dict)
            or value["configuration"].get("repository") != value["repository"]
            or value["configuration"].get("ordinary_relaunch_authorized") is not False):
        raise RuntimeError("storage_online_release_journal_invalid")


def _release_values(raw):
    try:
        lines=raw.decode().splitlines()
        values=dict(line.split("=",1) for line in lines)
    except (ValueError,UnicodeError):
        raise RuntimeError("storage_online_release_metadata_invalid") from None
    if len(values)!=len(lines):
        raise RuntimeError("storage_online_release_metadata_invalid")
    return values


def _publish_file(path, *, before, after):
    """Reconcile exact filesystem outcomes only; never repeat a daemon/SQL action."""
    current=read_private_environment(path)
    if current==after:
        # A prior rename may have completed just before process loss, without
        # its directory fsync. Reconcile durability before recording completion.
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
        host.sync_directory(path.parent)
        return
    if current!=before:
        raise RuntimeError("storage_online_release_publication_conflict")
    descriptor, temporary=tempfile.mkstemp(prefix=".storage-release-",dir=path.parent)
    try:
        with os.fdopen(descriptor,"wb") as stream:
            stream.write(after);stream.flush();os.fsync(stream.fileno())
        if read_private_environment(path)!=before:
            raise RuntimeError("storage_online_release_publication_conflict")
        os.replace(temporary,path)
        host.sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)


def publish_configuration(state_root, *, repository, environment_path, saved, configuration):
    """Publish only terminal configuration, under the existing final-state owner.

    The caller freshly admitted runtime/pair/configuration under the deployment
    lock. Intent precedes either atomic file replacement. The bridge explicitly
    records no completed fleet release and authorizes only the exact next deploy.
    """
    state_root,repository,environment_path=map(Path,(state_root,repository,environment_path))
    if "release" in saved:
        raise RuntimeError("storage_online_release_requires_file_reconciliation")
    reserved = {state_root/name for name in (SOURCE_ENVIRONMENT, PREPARED_ENVIRONMENT, SOURCE_RELEASE,
        BRIDGE_RELEASE, "release.env", "storage-online-final.json", "storage-online-request.json", runtime.RUNTIME_RECIPE)}
    if environment_path in reserved or environment_path.resolve(strict=True) != environment_path:
        raise RuntimeError("storage_online_release_environment_alias")
    original=read_private_environment(state_root/SOURCE_ENVIRONMENT)
    prepared=read_private_environment(state_root/PREPARED_ENVIRONMENT)
    if read_private_environment(environment_path)!=original:
        raise RuntimeError("storage_online_release_environment_changed")
    source_release=read_private_environment(state_root/"release.env")
    values=_release_values(source_release)
    if (set(values)!={"current_revision","current_source_tree_hash","previous_revision","deployed_at","storage_layout"}
            or values["current_revision"]!=saved["binding"]["source_revision"]
            or values["storage_layout"]!=""):
        raise RuntimeError("storage_online_release_source_metadata_changed")
    request=host.load_receipt(state_root/"storage-online-request.json")
    if host.digest(request) != configuration["request_sha256"]:
        raise RuntimeError("storage_online_release_request_changed")
    bridge=("current_revision=\ncurrent_source_tree_hash=\nprevious_revision=\ndeployed_at=\n"
            "storage_layout=ssd-hdd-v1\npending_storage_revision="+request["source_revision"]+"\n").encode()
    _preserve_file(state_root/SOURCE_RELEASE,source_release)
    _preserve_file(state_root/BRIDGE_RELEASE,bridge)
    if host.load_receipt(state_root/"storage-online-final.json")!=saved:
        raise RuntimeError("storage_online_release_journal_changed")
    saved=deepcopy(saved)
    saved["release"]=dict(status="publishing",repository=str(repository),environment_path=str(environment_path),
        candidate_revision=request["source_revision"],candidate_source_hash=request["source_tree_hash"],
        source_environment_sha256=hashlib.sha256(original).hexdigest(),prepared_environment_sha256=hashlib.sha256(prepared).hexdigest(),
        source_release_sha256=hashlib.sha256(source_release).hexdigest(),bridge_release_sha256=hashlib.sha256(bridge).hexdigest(),
        configuration=configuration,requested_at=time.time(),published_at=None,deployed_release_sha256=None)
    validate_release_journal(saved)
    host.save_receipt(state_root/"storage-online-final.json",saved,initial=False)
    logger.info("storage_online_release_publication_started | revision=%s intent_retained=true", request["source_revision"])
    return reconcile_configuration_files(state_root,saved=saved)


def reconcile_configuration_files(state_root, *, saved):
    """Finish known atomic file writes; retain journals and never start a service."""
    state_root=Path(state_root)
    validate_release_journal(saved)
    value=saved["release"]
    if value["status"]!="publishing":
        raise RuntimeError("storage_online_release_not_publishing")
    if host.load_receipt(state_root/"storage-online-final.json")!=saved:
        raise RuntimeError("storage_online_release_journal_changed")
    files={}
    for name,key in ((SOURCE_ENVIRONMENT,"source_environment_sha256"),(PREPARED_ENVIRONMENT,"prepared_environment_sha256"),
                     (SOURCE_RELEASE,"source_release_sha256"),(BRIDGE_RELEASE,"bridge_release_sha256")):
        raw=read_private_environment(state_root/name)
        if hashlib.sha256(raw).hexdigest()!=value[key]:
            raise RuntimeError("storage_online_release_artifact_changed")
        files[name]=raw
    def check():
        if host.load_receipt(state_root/"storage-online-final.json") != saved:
            raise RuntimeError("storage_online_release_journal_changed")
        if any(read_private_environment(state_root/name) != raw for name, raw in files.items()):
            raise RuntimeError("storage_online_release_artifact_changed")
    environment=Path(value["environment_path"])
    if (read_private_environment(environment) not in (files[SOURCE_ENVIRONMENT],files[PREPARED_ENVIRONMENT])
            or read_private_environment(state_root/"release.env") not in (files[SOURCE_RELEASE],files[BRIDGE_RELEASE])):
        raise RuntimeError("storage_online_release_publication_conflict")
    check()
    _publish_file(environment,before=files[SOURCE_ENVIRONMENT],after=files[PREPARED_ENVIRONMENT])
    check()
    _publish_file(state_root/"release.env",before=files[SOURCE_RELEASE],after=files[BRIDGE_RELEASE])
    check()
    if read_private_environment(environment) != files[PREPARED_ENVIRONMENT]:
        raise RuntimeError("storage_online_release_environment_changed")
    saved=deepcopy(saved);saved["release"].update(status="published",published_at=time.time())
    host.save_receipt(state_root/"storage-online-final.json",saved,initial=False)
    logger.info("storage_online_release_configuration_published | revision=%s migration_replay_authorized=false", value["candidate_revision"])
    return dict(phase="deployment_configuration_published",ordinary_relaunch_authorized=False,
        deployment_revision=value["candidate_revision"],migration_replay_authorized=False)


def admit_deployment(state_root, *, environment_path, repository, action, revision):
    """Existing deployer guard: terminal grant is only for the exact first release.

    All original migration receipts remain. This grant never resumes migration or
    replays its SQL/Docker actions. Later releases use the existing deployment and
    recovery rules after the first complete fleet release has been recorded.
    """
    from scripts.automation import storage_online_final as final
    state_root=Path(state_root)
    saved=final._load(state_root/final.STATE)
    if "release" not in saved:
        raise RuntimeError("storage_online_release_not_published")
    validate_release_journal(saved)
    value=saved["release"]
    if value["status"] not in {"published","deployed"}:
        raise RuntimeError("storage_online_release_publication_unresolved")
    active_environment=read_private_environment(Path(environment_path))
    if (str(Path(environment_path).resolve(strict=True))!=value["environment_path"]
            or value["status"] == "published" and hashlib.sha256(active_environment).hexdigest()!=value["prepared_environment_sha256"]):
        raise RuntimeError("storage_online_release_environment_changed")
    for name,key in ((SOURCE_ENVIRONMENT,"source_environment_sha256"),(PREPARED_ENVIRONMENT,"prepared_environment_sha256"),
                     (SOURCE_RELEASE,"source_release_sha256"),(BRIDGE_RELEASE,"bridge_release_sha256")):
        if hashlib.sha256(read_private_environment(state_root/name)).hexdigest()!=value[key]:
            raise RuntimeError("storage_online_release_artifact_changed")
    request=host.load_receipt(state_root/"storage-online-request.json")
    if (host.digest(request) != value["configuration"]["request_sha256"]
            or request["source_revision"] != value["candidate_revision"] or request["source_tree_hash"] != value["candidate_source_hash"]
            or host.digest(host.load_receipt(state_root/runtime.RUNTIME_RECIPE, max_bytes=524288))
                != value["configuration"]["storage_configuration_sha256"]):
        raise RuntimeError("storage_online_release_request_changed")
    for name, key in (("storage-online-preparation.json", "preparation_sha256"), ("storage-online-worker.json", "worker_sha256")):
        if host.digest(host.load_receipt(state_root/name)) != saved["binding"][key]:
            raise RuntimeError("storage_online_release_migration_evidence_changed")
    current=read_private_environment(state_root/"release.env")
    fields=_release_values(current)
    if value["status"]=="published":
        if (action!="deploy" or revision!=value["candidate_revision"] or str(Path(repository).resolve(strict=True))!=value["repository"]
                or not (hashlib.sha256(current).hexdigest()==value["bridge_release_sha256"]
                    or (set(fields)=={"current_revision","current_source_tree_hash","previous_revision","deployed_at","storage_layout"}
                        and fields["current_revision"]==value["candidate_revision"]
                        and fields["current_source_tree_hash"]==value["candidate_source_hash"]
                        and fields["previous_revision"]=="" and fields["storage_layout"]=="ssd-hdd-v1"))):
            raise RuntimeError("storage_online_release_exact_deployment_required")
    elif (set(fields)!={"current_revision","current_source_tree_hash","previous_revision","deployed_at","storage_layout"}
            or fields["storage_layout"]!="ssd-hdd-v1"
            or not re.fullmatch(r"[0-9a-f]{40}",fields["current_revision"])
            or not re.fullmatch(r"[0-9a-f]{64}",fields["current_source_tree_hash"])):
        raise RuntimeError("storage_online_release_layout_changed")
    return saved


def record_deployment(state_root, *, environment_path, revision, source_hash):
    """Called by record_release only after its existing full-fleet checks pass."""
    from scripts.automation import storage_online_final as final
    state_root=Path(state_root)
    saved=final._load(state_root/final.STATE)
    if "release" not in saved or saved["release"]["status"]=="deployed":
        return
    value=saved["release"]
    saved=admit_deployment(state_root, environment_path=environment_path,
        repository=value["repository"], action="deploy", revision=revision)
    current=read_private_environment(state_root/"release.env")
    fields=_release_values(current)
    if (value["status"]!="published" or revision!=value["candidate_revision"] or source_hash!=value["candidate_source_hash"]
            or Path(environment_path)!=Path(value["environment_path"])
            or hashlib.sha256(read_private_environment(Path(environment_path))).hexdigest()!=value["prepared_environment_sha256"]
            or fields.get("current_revision")!=revision or fields.get("current_source_tree_hash")!=source_hash
            or fields.get("previous_revision")!="" or fields.get("storage_layout")!="ssd-hdd-v1"
            or "pending_storage_revision" in fields):
        raise RuntimeError("storage_online_release_deployment_unconfirmed")
    with (state_root/"release.env").open("rb") as stream:os.fsync(stream.fileno())
    host.sync_directory(state_root)
    if (host.load_receipt(state_root/final.STATE) != saved
            or read_private_environment(state_root/"release.env") != current):
        raise RuntimeError("storage_online_release_deployment_record_changed")
    saved=deepcopy(saved)
    saved["release"].update(status="deployed",deployed_release_sha256=hashlib.sha256(current).hexdigest())
    host.save_receipt(state_root/final.STATE,saved,initial=False)
    logger.info("storage_online_release_deployment_recorded | revision=%s receipts_retained=true", revision)


def check_deployment_render(state_root, *, environment_path, repository, revision, source_hash, model):
    """Check the deployer's actual resolved inputs immediately before activation."""
    saved = admit_deployment(state_root, environment_path=environment_path,
        repository=repository, action="deploy", revision=revision)
    value = saved["release"]
    if value["status"] == "published":
        if (source_hash != value["candidate_source_hash"] or deployment_storage_fingerprint(model,
                environment_path=environment_path, prepared_path=Path(state_root)/PREPARED_ENVIRONMENT)
                != value["configuration"]["deployment_storage_sha256"]):
            raise RuntimeError("storage_online_release_deployment_render_changed")
