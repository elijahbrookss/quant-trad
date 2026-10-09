"""Repository/native-WAL continuation of the held, committed online operation.

One fixed UID70 preparer reuses storage_recovery_prepare. It cannot access the
application spool. Original source and deployment ownership must remain live;
this boundary neither starts applications nor creates keys or a backup policy.
"""
from copy import deepcopy
import json
import math
from pathlib import Path
import re
import sys
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial
from scripts.automation import storage_online_recovery as recovery
from scripts.automation import storage_handoff_pause as preserving

RECIPE = "storage-online-repository-preparation.compose.json"
_ACTIONS = ("logins", "create", "prepare", "settings", "stop", "start", "wal_switch")
_ARCHIVE_COMMAND = "pgbackrest --config=/run/quanttrad/recovery/pgbackrest.conf --stanza=qt archive-push %p"
_PREPARE = """
import json,os,sys
from pathlib import Path
from types import SimpleNamespace
from time import monotonic
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from core.settings import get_settings
from scripts.automation.storage_recovery_prepare import prepare
from scripts.automation.storage_online_controller import OnlineController
args=json.loads(sys.argv[1])
deadline=args["deadline_monotonic"]
args["timeout_seconds"]=min(args["timeout_seconds"],int(deadline-monotonic()))
if args["timeout_seconds"] < 1: raise RuntimeError("storage_online_repository_deadline_expired")
# Reuse the admitted maintenance configuration. The preparation CLI's private
# input file is temporary; deployment must not provision a duplicate beside keys.
config=args.pop("incremental_configuration")
path=Path('/tmp/qt-incremental-config.json')
with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'w') as output:
    json.dump(config,output,sort_keys=True)
args["incremental_config"]=str(path)
for name in ('inventory','incremental_config','archiver_config','pg_controldata'):
    args[name]=Path(args[name])
engine=create_engine(get_settings().database.dsn,poolclass=NullPool,hide_parameters=True)
try:
    with engine.connect() as conn, conn.begin():
        conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        OnlineController._require_job_environment(conn,allow_connections=True)
        OnlineController._supported_builtin_catalog(conn)
finally:
    engine.dispose()
print(json.dumps(prepare(SimpleNamespace(**args)),sort_keys=True),flush=True)
"""


def validate_journal(saved):
    value = saved["repositories"]
    if (not isinstance(value, dict)
            or set(value) != {"recipe_sha256", "helper_id", "helper_contract", "started_at",
                             "deadline_monotonic", "completed", "inflight", "finished_at", "report", "helper_retirement"}
            or not isinstance(value["recipe_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", value["recipe_sha256"])
            or type(value["started_at"]) not in (int,float)
            or not saved["recovery"]["finished_at"] <= value["started_at"] <= saved["deadline"]
            or type(value["deadline_monotonic"]) not in (int,float)
            or not math.isfinite(value["deadline_monotonic"])
            or not 0 < value["deadline_monotonic"] <= saved["switch"]["deadline_monotonic"]
            or value["completed"] not in [list(_ACTIONS[:i]) for i in range(len(_ACTIONS)+1)]
            or value["inflight"] not in (None, *_ACTIONS)
            or value["inflight"] is not None and (len(value["completed"]) == len(_ACTIONS)
                or value["inflight"] != _ACTIONS[len(value["completed"])])
            or any((name in value["completed"]) != (value[field] is not None)
                   for name,field in (("create","helper_id"),("create","helper_contract"),("prepare","report"),("prepare","helper_retirement")))
            or any(value[field] is not None and (not isinstance(value[field],str)
                or not re.fullmatch(r"[0-9a-f]{64}",value[field])) for field in ("helper_id","helper_contract","helper_retirement"))):
        raise RuntimeError("storage_online_repository_journal_invalid")
    if saved["phase"] != "recovery_repository_preparing":
        if (value["completed"] != list(_ACTIONS) or value["inflight"] is not None
                or type(value["finished_at"]) not in (int,float)
                or not value["started_at"] <= value["finished_at"] <= saved["deadline"]):
            raise RuntimeError("storage_online_repository_completion_invalid")
    elif value["finished_at"] is not None:
        raise RuntimeError("storage_online_repository_completion_invalid")


def _preparer_contract(details, database):
    # Docker adopts the network-container hostname when starting this helper.
    # Both admitted values are exact identities, not arbitrary hostname drift.
    normalized = deepcopy(details)
    hostname = database["config"]["Hostname"]
    if normalized["config"].get("Hostname") not in (details["id"][:12], hostname):
        raise RuntimeError("storage_online_repository_preparer_hostname_changed")
    normalized["config"]["Hostname"] = hostname
    return host.database_contract(normalized)


def incremental_configuration(state_root):
    """Use the same strict configuration owner as the admitted maintenance service."""
    from scripts.automation.storage_online_runtime import RUNTIME_RECIPE
    from portal.backend.service.storage.maintenance_runtime import read_storage_maintenance_limits
    model = host.load_receipt(Path(state_root)/RUNTIME_RECIPE, max_bytes=524288)
    mounts = [m for m in model["services"]["storage-maintenance"]["volumes"]
              if m.get("target") == "/run/quanttrad/storage-maintenance.json"]
    if len(mounts) != 1 or mounts[0].get("type") != "bind" or mounts[0].get("read_only") is not True:
        raise RuntimeError("storage_online_repository_maintenance_configuration_required")
    path = Path(mounts[0]["source"])
    before = preserving._runtime_configuration_bytes(path)
    _, _, config = read_storage_maintenance_limits(path)
    if config is None or preserving._runtime_configuration_bytes(path) != before:
        raise RuntimeError("storage_online_repository_maintenance_configuration_changed")
    return {key: str(value) if isinstance(value, Path) else value
            for key, value in vars(config).items()}


def prepare_repositories(state_root, *, saved, worker_process, source_check,
                         max_bytes, reserve_bytes, recent_free_bytes, max_duration_seconds,
                         logins_already_open=False):
    if (type(logins_already_open) is not bool
            or logins_already_open and (source_check is None or recovery._CONTINUATION.get() is not source_check)
            or type(max_duration_seconds) is not int or not 1 <= max_duration_seconds <= 600
            or any(type(v) is not int or v < 0 for v in (max_bytes,reserve_bytes,recent_free_bytes))
            or max_bytes == 0):
        raise ValueError("storage_online_repository_budget_invalid")
    deadline = min(time.monotonic()+max_duration_seconds,saved["switch"]["deadline_monotonic"])
    worker = host.load_receipt(state_root/launch._STATE)
    preparation = initial._load(state_root)
    if (host.digest(worker) != saved["binding"]["worker_sha256"]
            or host.digest(preparation) != saved["binding"]["preparation_sha256"]):
        raise RuntimeError("storage_online_repository_binding_changed")
    database_id = saved["recovery"]["replacement_id"]
    reader_id = worker["container_id"]
    model = host.load_receipt(state_root/recovery.RECIPE)
    if host.digest(model) != saved["recovery"]["recipe_sha256"]:
        raise RuntimeError("storage_online_repository_database_recipe_changed")
    service = model["services"]["tsdb"]
    mounts = {m["target"]:m for m in service["volumes"]}
    history = mounts["/qt-history"]["source"]
    keys = Path(mounts["/run/quanttrad/recovery"]["source"])
    preserving._database_recovery_mounts(service,model["volumes"],history)
    binding = worker["binding"]
    controls = binding["mounts"]
    inventory = Path(controls["/run/qt-online/inventory.json"]["source"])
    raw = preserving._runtime_configuration_bytes(inventory)
    import hashlib
    if hashlib.sha256(raw).hexdigest() != binding["inventory_sha256"]:
        raise RuntimeError("storage_online_repository_inventory_changed")
    targets = json.loads(raw)["targets"]
    if len(targets)!=2 or {t["medium"] for t in targets}!={"ssd","hdd"}:
        raise RuntimeError("storage_online_repository_targets_changed")
    recent = next(t for t in targets if t["medium"]=="ssd")
    backup = next(t for t in targets if t["medium"]=="hdd")
    identity = str(saved["login_gate"]["database"]["cluster"])+"/"+str(saved["login_gate"]["database"]["oid"])
    project = saved["binding"]["project"]
    # Compose selects existing containers by project/service labels, even after
    # docker rename. Keep the failed preparer intact under its original project.
    preparer_project = project+("-recovery-continuation" if logins_already_open else "-recovery")
    recipe_path = state_root/RECIPE
    name = project+"-storage-repository-prepare"
    if recipe_path.exists() or recipe_path.is_symlink():
        raise RuntimeError("storage_online_repository_recipe_exists")

    def retired():
        recovery.admit_retired_reader(worker_process,worker,saved,source_check,deadline)

    with host.docker_deadline(deadline):
        retired()
        rows = host.inventory(project,operator_id=reader_id)
        initial._admit_clients(rows,preparation["clients"])
        if any(rows[n]["running"] for n in host.STOP) or rows["tsdb"]["id"]!=database_id:
            raise RuntimeError("storage_online_repository_source_changed")
        database = host.database_details(database_id)
        if host.cluster_identifier(database_id,maintenance=True)!=preparation["cluster"]:
            raise RuntimeError("storage_online_repository_cluster_changed")
        original_contract = host.database_contract(database)
        original_mounts = sorted(database["mounts"], key=lambda mount: mount["Destination"])
        original_networks = host.database_networks(database)
        collector = host.database_details(rows["market-data-collector"]["id"])
        dsn = launch._dsn(database,collector)
        from scripts.automation.storage_online_final import _GATE_OBSERVE
        original_gate = saved["login_gate"]["database"]
        def gate(opened):
            if json.loads(host.maintenance_query(database_id,_GATE_OBSERVE)) != {**original_gate,"allow_connections":opened}:
                raise RuntimeError("storage_online_repository_gate_changed")
        gate(logins_already_open)
        if host.docker("ps","-aq","--no-trunc","--filter","name=^/"+name+"$").strip():
            raise RuntimeError("storage_online_repository_unowned_preparer")
        args = dict(deadline_monotonic=deadline,inventory="/run/qt-online/inventory.json",
            incremental_configuration=incremental_configuration(state_root),
            archiver_config="/run/quanttrad/recovery/pgbackrest.conf",
            recent_target=recent["target_id"],backup_target=backup["target_id"],
            expected_database_identity=identity,pg_controldata="/usr/lib/postgresql/15/bin/pg_controldata",
            max_bytes=max_bytes,reserve_bytes=reserve_bytes,recent_free_bytes=recent_free_bytes,
            timeout_seconds=max(1,int(deadline-time.monotonic())))
        helper_mounts = deepcopy(service["volumes"])
        next(m for m in helper_mounts if m["target"]=="/run/quanttrad/recovery").pop("read_only")
        for target in ("/run/qt-online/inventory.json","/run/qt-online/udev"):
            helper_mounts.append(dict(type="bind",source=controls[target]["source"],target=target,
                                     read_only=True,bind=dict(create_host_path=False)))
        helper = dict(image=binding["image"],container_name=name,pull_policy="never",user="70:70",
            restart="no",init=True,read_only=True,cap_drop=["ALL"],security_opt=["no-new-privileges:true"],
            mem_limit=512*1024**2,memswap_limit=512*1024**2,cpus=2,pids_limit=128,
            network_mode="container:"+database_id,pid="container:"+database_id,
            entrypoint=["python"],command=["-c",_PREPARE,json.dumps(args,sort_keys=True)],
            environment=dict(PG_DSN=dsn,QT_DISABLE_DOTENV="1",QT_LOGGING_LOKI_URL="",
                QT_STORAGE_UDEV_ROOT="/run/qt-online/udev"),volumes=helper_mounts,
            tmpfs=["/tmp:rw,nosuid,nodev,size=134217728,uid=70,gid=70,mode=0700",
                   "/app/logs:rw,nosuid,nodev,size=16777216,uid=70,gid=70,mode=0700"])
        key_stat = keys.stat()
        key_binding = (key_stat.st_dev,key_stat.st_ino,key_stat.st_uid,key_stat.st_gid,key_stat.st_mode)
        socket_name = model["volumes"]["storage-recovery-socket"]["name"]
        socket_info = json.loads(host.docker("volume","inspect","--format","{{json .}}",socket_name))
        image=json.loads(host.docker("image","inspect","--format","{{json .}}",binding["image"]))
        expected_env=dict(v.split("=",1) for v in image["Config"].get("Env") or [])
        expected_env.update(helper["environment"])
        expected_mounts={m["target"]:(m["type"],model["volumes"][m["source"]]["name"] if m["type"]=="volume" else m["source"],not m.get("read_only",False)) for m in helper_mounts}
        expected_tmpfs = dict(v.split(":",1) for v in helper["tmpfs"])

        def admit_helper(identity, previous=None):
            details=host.database_details(identity);config=details["config"];h=details["host"]
            mounted=[m for m in details["mounts"] if m["Type"]!="tmpfs"]
            actual_mounts={m["Destination"]:(m["Type"],m.get("Name") if m["Type"]=="volume" else m["Source"],m["RW"]) for m in mounted}
            contract=_preparer_contract(details,database)
            if (details["image"]!=binding["image"] or image["Id"]!=binding["image"]
                    or config.get("User")!="70:70" or config.get("Entrypoint")!=["python"]
                    or config.get("Cmd")!=helper["command"]
                    or dict(v.split("=",1) for v in config.get("Env") or [])!=expected_env
                    or h.get("Privileged") or h.get("CapAdd") or h.get("CapDrop")!=["ALL"]
                    or h.get("Devices") or h.get("DeviceRequests") or h.get("GroupAdd")
                    or h.get("Sysctls") or h.get("VolumesFrom")
                    or h.get("SecurityOpt")!=helper["security_opt"] or h.get("Init") is not True
                    or h.get("RestartPolicy",{}).get("Name")!="no"
                    or h.get("Memory")!=512*1024**2 or h.get("MemorySwap")!=512*1024**2
                    or h.get("NanoCpus")!=2*10**9 or h.get("PidsLimit")!=128
                    or h.get("Tmpfs")!=expected_tmpfs
                    or not h.get("ReadonlyRootfs") or h.get("NetworkMode")!="container:"+database_id
                    or h.get("PidMode")!="container:"+database_id
                    or actual_mounts!=expected_mounts or len(mounted)!=len(expected_mounts)
                    or {m["Destination"] for m in details["mounts"] if m["Type"]=="tmpfs"}-set(expected_tmpfs)
                    or previous is not None and previous!=contract):
                raise RuntimeError("storage_online_repository_preparer_contract_changed")
            return contract

        # Private rendered recipe, never printed: it contains the original PG_DSN.
        recipe=dict(name=preparer_project,services=dict(prepare=helper),volumes=model["volumes"])
        host.save_receipt(recipe_path,recipe,initial=True)
        saved.update(phase="recovery_repository_preparing",repositories=dict(
            recipe_sha256=host.digest(recipe),helper_id=None,helper_contract=None,
            started_at=time.time(),deadline_monotonic=deadline,completed=[],inflight=None,
            finished_at=None,report=None,helper_retirement=None))
        journal=saved["repositories"];path=state_root/"storage-online-final.json"
        host.save_receipt(path,saved,initial=False)

        def check():
            retired()
            if (host.load_receipt(path)!=saved or host.digest(host.load_receipt(recipe_path))!=journal["recipe_sha256"]
                    or host.digest(host.load_receipt(state_root/recovery.RECIPE))!=saved["recovery"]["recipe_sha256"]
                    or preserving._runtime_configuration_bytes(inventory)!=raw):
                raise RuntimeError("storage_online_repository_saved_binding_changed")
            preserving._database_recovery_mounts(service,model["volumes"],history)
            info=keys.stat()
            if (info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode)!=key_binding:
                raise RuntimeError("storage_online_repository_key_directory_changed")
            if json.loads(host.docker("volume","inspect","--format","{{json .}}",socket_name))!=socket_info:
                raise RuntimeError("storage_online_repository_socket_changed")
            preserving._history_filesystem(history,preparation["history_uuid"])
            current=host.inventory(project,database_preparing=True,operator_id=reader_id)
            initial._admit_clients(current,preparation["clients"])
            if any(current[n]["running"] for n in host.STOP) or current["tsdb"]["id"]!=database_id:
                raise RuntimeError("storage_online_repository_source_restarted")
            actual=host.database_details(database_id)
            if (host.database_contract(actual)!=original_contract
                    or sorted(actual["mounts"], key=lambda mount: mount["Destination"])!=original_mounts
                    or not host.same_database_networks(actual,original_networks)):
                raise RuntimeError("storage_online_repository_database_changed")
            if journal["helper_id"] is not None:
                admit_helper(journal["helper_id"],journal["helper_contract"])
                if journal["helper_retirement"] is not None:
                    status=json.loads(host.docker("inspect","--format","{{json .State}}",journal["helper_id"]))
                    if host.digest(status)!=journal["helper_retirement"]:
                        raise RuntimeError("storage_online_repository_preparer_retirement_changed")
            return current

        def dispatch(action,arguments,*,sql=None):
            check();journal["inflight"]=action;host.save_receipt(path,saved,initial=False)
            print("event=storage_online_repository_dispatch action="+action+" intent_retained=true",file=sys.stderr,flush=True)
            host.supervised_source_action(arguments,deadline=deadline,check=check,input=sql)
        def completed(action):
            journal["completed"].append(action);journal["inflight"]=None
            host.save_receipt(path,saved,initial=False);check()
        sql_args=["exec","-i",database_id,"sh","-ec",
            'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d postgres '
            '-v ON_ERROR_STOP=1 -v target="$POSTGRES_DB" -qAtf -']
        if not logins_already_open:
            dispatch("logins",sql_args,sql="SET statement_timeout='5s';\nSELECT format('ALTER DATABASE %I ALLOW_CONNECTIONS true',datname) FROM pg_database WHERE datname=:'target'\n\\gexec\n")
        gate(True);completed("logins")
        dispatch("create",["compose","--project-name",preparer_project,"--file",str(recipe_path),
                           "create","--no-build","--pull","never","prepare"])
        ids=host.docker("ps","-aq","--no-trunc","--filter","name=^/"+name+"$").split()
        if len(ids)!=1:raise RuntimeError("storage_online_repository_preparer_missing")
        journal["helper_id"]=ids[0];journal["helper_contract"]=admit_helper(ids[0]);completed("create")
        dispatch("prepare",["start","--attach",ids[0]])
        status=json.loads(host.docker("inspect","--format","{{json .State}}",ids[0]))
        if status.get("Running") or status.get("Pid")!=0 or status.get("ExitCode")!=0 or status.get("OOMKilled"):
            raise RuntimeError("storage_online_repository_preparer_not_cleanly_retired")
        report=json.loads(host.docker("logs","--tail","1",ids[0]))
        if (report.get("schema_version")!="qt.encrypted_recovery_preparation.v1"
                or report.get("database_identity")!=identity or report.get("filesystem_uuid")!=backup["filesystem_uuid"]
                or report.get("repositories_initialized") is not True or report.get("backup_created") is not False
                or report.get("policy_enabled") is not False):
            raise RuntimeError("storage_online_repository_preparation_not_confirmed")
        if not re.fullmatch(r"[0-9a-f]{64}",report.get("archiver_config_sha256","")):
            raise RuntimeError("storage_online_repository_config_digest_invalid")
        def config_unchanged():
            observed=host.docker("exec","--user","70:70",database_id,"sha256sum",
                "/run/quanttrad/recovery/pgbackrest.conf").split()
            if observed!=[report["archiver_config_sha256"],"/run/quanttrad/recovery/pgbackrest.conf"]:
                raise RuntimeError("storage_online_repository_archiver_config_changed")
        config_unchanged()
        journal["helper_retirement"]=host.digest(status)
        journal["report"]=report;completed("prepare")
        gate(True)
        before=json.loads(host.maintenance_query(database_id,"SELECT json_build_object('mode',current_setting('archive_mode'),'command_disabled',current_setting('archive_command') IN ('','(disabled)'),'library',current_setting('archive_library'),'wal_level',current_setting('wal_level'),'archived_count',(SELECT archived_count FROM pg_stat_archiver))::text"))
        if before["mode"]!="off" or before["command_disabled"] is not True or before["library"]!="" or before["wal_level"]!="replica":
            raise RuntimeError("storage_online_repository_archive_settings_changed")
        dispatch("settings",sql_args,sql="SET statement_timeout='5s';\nALTER SYSTEM SET archive_mode='on';\nALTER SYSTEM SET archive_command='"+_ARCHIVE_COMMAND+"';\n")
        completed("settings")
        dispatch("stop",["stop","--signal","SIGTERM","--timeout","-1",database_id])
        stopped=check()["tsdb"]
        if stopped["running"] or stopped["pid"]!=0 or stopped["exit_code"]!=0:
            raise RuntimeError("storage_online_repository_database_unclean_stop")
        completed("stop")
        dispatch("start",["start",database_id]);completed("start")
        waiting=False
        while True:
            check()
            try:cluster=host.cluster_identifier(database_id,maintenance=True)
            except RuntimeError:
                if not waiting:
                    print("event=storage_online_repository_readiness_waiting intent_retained=true",file=sys.stderr,flush=True)
                    waiting=True
                time.sleep(min(.2,max(0,deadline-time.monotonic())));continue
            if cluster!=preparation["cluster"]:raise RuntimeError("storage_online_repository_cluster_changed")
            gate(True);config_unchanged();break
        dispatch("wal_switch",sql_args,sql="SET statement_timeout='5s';\nSELECT pg_switch_wal();\n")
        completed("wal_switch")
        while True:
            check();gate(True)
            wal=json.loads(host.maintenance_query(database_id,"SELECT json_build_object('mode',current_setting('archive_mode'),'command',current_setting('archive_command'),'library',current_setting('archive_library'),'archived_count',(SELECT archived_count FROM pg_stat_archiver),'pending_restart',EXISTS(SELECT 1 FROM pg_settings WHERE name IN ('archive_mode','archive_command') AND pending_restart))::text"))
            if wal["mode"]!="on" or wal["command"]!=_ARCHIVE_COMMAND or wal["library"]!="" or wal["pending_restart"]:
                raise RuntimeError("storage_online_repository_native_wal_not_configured")
            if wal["archived_count"]>before["archived_count"]:break
            time.sleep(min(.2,max(0,deadline-time.monotonic())))
        check();config_unchanged();saved["phase"]="recovery_wal_ready";journal["finished_at"]=time.time()
        host.save_receipt(path,saved,initial=False);check()
        return dict(database_id=database_id,preparer_id=ids[0],repositories_initialized=True,
                    native_wal_delivered=True,runtime_activation_authorized=False,backup_created=False)
