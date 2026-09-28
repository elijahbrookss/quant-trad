"""Fixed post-switch application handover: private spool and matching startup.

No CLI, replay engine, alternate DSN or policy owner. The final-state owner keeps
source/deployment holds. Only the isolated spool helper has source-read access;
application and maintenance startup retain their separate existing privileges.
"""
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial
from scripts.automation import storage_online_recovery as recovery
from scripts.automation import storage_handoff_pause as preserving

_ACTIONS = ("create", "copy")
_COPY = """
import hashlib,json,os,sys,time
from scripts.automation.storage_online_drain import prepare_recovery_spool
args=json.loads(sys.argv[1])
reserve=args.pop('reserve_bytes')
def check():
    if time.monotonic() >= args['deadline']:
        raise RuntimeError('storage_online_spool_deadline_expired')
    space=os.statvfs('/candidate')
    if space.f_bavail*space.f_frsize<reserve+1024**2:
        raise RuntimeError('storage_online_runtime_spool_reserve_exhausted')
report=prepare_recovery_spool('/original','/candidate',check=check,**args)
check()
raw=json.dumps(report,sort_keys=True,separators=(',',':')).encode()
if len(raw)>16*1024**2:raise RuntimeError('storage_online_spool_report_too_large')
fd=os.open('/candidate/.qt-recovery-copy.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
try:
    view=memoryview(raw)
    while view:
        check();written=os.write(fd,view[:1024**2])
        if written<=0:raise RuntimeError('storage_online_spool_report_write_failed')
        view=view[written:]
    os.fchown(fd,1000,1000);os.fsync(fd)
finally:os.close(fd)
fd=os.open('/candidate',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try:os.fsync(fd)
finally:os.close(fd)
check()
print(json.dumps(dict(manifest_sha256=hashlib.sha256(raw).hexdigest()),sort_keys=True),flush=True)
"""


def validate_spool_journal(saved):
    value = saved["runtime_spool"]
    if (not isinstance(value,dict) or set(value)!={"destination","destination_identity",
            "source","source_identity","started_at","deadline_monotonic","completed",
            "inflight","helper_id","helper_contract","helper_retirement","report","finished_at","arguments_sha256"}
            or any(not isinstance(value[k],str) or not Path(value[k]).is_absolute() for k in ("source","destination"))
            or any(not isinstance(value[k],list) or len(value[k])!=2
                   or any(type(v) is not int for v in value[k]) for k in ("source_identity","destination_identity"))
            or type(value["started_at"]) not in (int,float)
            or not saved["repositories"]["finished_at"]<=value["started_at"]<=saved["deadline"]
            or type(value["deadline_monotonic"]) not in (int,float)
            or not math.isfinite(value["deadline_monotonic"])
            or not 0<value["deadline_monotonic"]<=saved["switch"]["deadline_monotonic"]
            or value["arguments_sha256"] is None
            or value["completed"] not in ([],["create"],["create","copy"])
            or value["inflight"] not in (None,*_ACTIONS)
            or value["inflight"] is not None and (len(value["completed"])==2
                or value["inflight"]!=_ACTIONS[len(value["completed"])])
            or any((action in value["completed"])!=(value[field] is not None) for action,field in (
                ("create","helper_id"),("create","helper_contract"),("copy","helper_retirement"),("copy","report")))
            or any(value[k] is not None and (not isinstance(value[k],str)
                or not re.fullmatch(r"[0-9a-f]{64}",value[k])) for k in (
                "helper_id","helper_contract","helper_retirement","arguments_sha256"))):
        raise RuntimeError("storage_online_runtime_spool_journal_invalid")
    if saved["phase"]!="recovery_spool_preparing":
        if (value["completed"]!=list(_ACTIONS) or value["inflight"] is not None
                or type(value["finished_at"]) not in (int,float)
                or not value["started_at"]<=value["finished_at"]<=saved["deadline"]):
            raise RuntimeError("storage_online_runtime_spool_completion_invalid")
    elif value["finished_at"] is not None:
        raise RuntimeError("storage_online_runtime_spool_completion_invalid")


def _copy_report(destination, expected):
    if not isinstance(expected,str) or not re.fullmatch(r"[0-9a-f]{64}",expected):
        raise RuntimeError("storage_online_runtime_spool_manifest_digest_invalid")
    path=destination/".qt-recovery-copy.json"
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,"rb") as handle:
        before=os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_size>16*1024**2
                or (before.st_uid,before.st_gid,stat.S_IMODE(before.st_mode))!=(1000,1000,0o600)):
            raise RuntimeError("storage_online_runtime_spool_manifest_invalid")
        raw=handle.read(16*1024**2+1)
        stable=lambda info:(info.st_dev,info.st_ino,info.st_mode,info.st_uid,info.st_gid,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        if stable(os.fstat(handle.fileno()))!=stable(before) or stable(path.lstat())!=stable(before) or hashlib.sha256(raw).hexdigest()!=expected:
            raise RuntimeError("storage_online_runtime_spool_manifest_changed")
    return json.loads(raw)


def prepare_spool(state_root, *, saved, worker_process, source_check,
                  destination, max_bytes, max_entries, reserve_bytes, max_duration_seconds):
    if (type(max_duration_seconds) is not int or not 1<=max_duration_seconds<=600
            or type(max_bytes) is not int or not 0<max_bytes<=64*1024**3
            or type(max_entries) is not int or not 1<=max_entries<=4096
            or type(reserve_bytes) is not int or reserve_bytes<0):
        raise ValueError("storage_online_runtime_spool_budget_invalid")
    deadline=min(time.monotonic()+max_duration_seconds,saved["switch"]["deadline_monotonic"])
    worker=host.load_receipt(state_root/launch._STATE)
    preparation=initial._load(state_root)
    if (host.digest(worker)!=saved["binding"]["worker_sha256"]
            or host.digest(preparation)!=saved["binding"]["preparation_sha256"]):
        raise RuntimeError("storage_online_runtime_spool_binding_changed")
    roots=preparation["source_roots"]
    source=next(Path(name) for name in roots if str(Path(name)/"objects") in roots)
    destination=Path(destination)
    if (not destination.is_absolute() or destination.resolve(strict=True)!=destination
            or source==destination or source in destination.parents or destination in source.parents
            or any(c in str(source)+str(destination) for c in (",","\n","\r"))):
        raise ValueError("storage_online_runtime_spool_roots_invalid")
    src=source.stat();dst=destination.stat()
    if (not stat.S_ISDIR(dst.st_mode) or stat.S_IMODE(dst.st_mode)!=0o700
            or (dst.st_uid,dst.st_gid)!=(1000,1000) or dst.st_dev!=src.st_dev
            or any(destination.iterdir())):
        raise RuntimeError("storage_online_runtime_spool_new_private_ssd_required")
    source_identity=[src.st_dev,src.st_ino];destination_identity=[dst.st_dev,dst.st_ino]
    project=saved["binding"]["project"];name=project+"-storage-spool-prepare"
    image=worker["binding"]["image"];database_id=saved["recovery"]["replacement_id"]
    copy_args=dict(deadline=deadline,max_entries=max_entries,max_bytes=max_bytes,reserve_bytes=reserve_bytes)
    args=["create","--name",name,"--label","qt.storage-spool-operation="+saved["binding"]["controller_id"],
        "--pull","never","--network","none","--user","0:0","--cap-drop","ALL",
        "--cap-add","CHOWN","--cap-add","DAC_OVERRIDE","--cap-add","FOWNER",
        "--security-opt","no-new-privileges","--read-only","--restart","no","--init",
        "--memory","128m","--memory-swap","128m","--cpus","1","--pids-limit","32",
        "--mount","type=bind,source="+str(source)+",target=/original,readonly",
        "--mount","type=bind,source="+str(destination)+",target=/candidate",
        "--entrypoint","python",image,"-c",_COPY,json.dumps(copy_args,sort_keys=True)]
    with host.docker_deadline(deadline):
        recovery.admit_retired_reader(worker_process,worker,saved,source_check,deadline)
        rows=host.inventory(project,operator_id=worker["container_id"])
        initial._admit_clients(rows,preparation["clients"])
        if any(rows[n]["running"] for n in host.STOP) or rows["tsdb"]["id"]!=database_id:
            raise RuntimeError("storage_online_runtime_spool_source_changed")
        if host.docker("ps","-aq","--no-trunc","--filter","name=^/"+name+"$").strip():
            raise RuntimeError("storage_online_runtime_spool_helper_exists")
        database=host.database_details(database_id)
        database_contract=host.database_contract(database)
        database_mounts=sorted(database["mounts"],key=lambda m:m["Destination"])
        database_networks=host.database_networks(database)
        if host.cluster_identifier(database_id,maintenance=True)!=preparation["cluster"]:
            raise RuntimeError("storage_online_runtime_spool_cluster_changed")
        preparer=saved["repositories"]["helper_id"]
        preparer_state=saved["repositories"]["helper_retirement"]
        original=saved["repositories"]["report"]
        saved.update(phase="recovery_spool_preparing",runtime_spool=dict(
            destination=str(destination),destination_identity=destination_identity,source=str(source),
            source_identity=source_identity,started_at=time.time(),deadline_monotonic=deadline,
            completed=[],inflight=None,helper_id=None,helper_contract=None,helper_retirement=None,
            report=None,finished_at=None,arguments_sha256=host.digest(args)))
        journal=saved["runtime_spool"];path=state_root/"storage-online-final.json"
        host.save_receipt(path,saved,initial=False)
        image_info=json.loads(host.docker("image","inspect","--format","{{json .}}",image))

        def helper_contract():
            d=host.database_details(journal["helper_id"]);c=d["config"];h=d["host"]
            mounted={m["Destination"]:(m["Type"],m["Source"],m["RW"]) for m in d["mounts"]}
            if (d["image"]!=image or c.get("User")!="0:0" or c.get("Entrypoint")!=["python"]
                    or c.get("Cmd")!=["-c",_COPY,json.dumps(copy_args,sort_keys=True)]
                    or sorted(c.get("Env") or [])!=sorted(image_info["Config"].get("Env") or [])
                    or h.get("Privileged") or h.get("CapDrop")!=["ALL"]
                    or set(h.get("CapAdd") or [])!={"CAP_CHOWN","CAP_DAC_OVERRIDE","CAP_FOWNER"}
                    or h.get("Devices") or h.get("DeviceRequests") or h.get("GroupAdd")
                    or h.get("PidMode") or h.get("IpcMode") not in ("private","")
                    or h.get("NetworkMode")!="none" or h.get("VolumesFrom") or h.get("Sysctls")
                    or h.get("SecurityOpt")!=["no-new-privileges"] or not h.get("ReadonlyRootfs")
                    or h.get("Init") is not True or h.get("RestartPolicy",{}).get("Name")!="no"
                    or h.get("Memory")!=128*1024**2 or h.get("MemorySwap")!=128*1024**2
                    or h.get("NanoCpus")!=10**9 or h.get("PidsLimit")!=32 or h.get("Tmpfs")
                    or mounted!={"/original":("bind",str(source),False),"/candidate":("bind",str(destination),True)}
                    or len(d["mounts"])!=2):
                raise RuntimeError("storage_online_runtime_spool_helper_contract_changed")
            return host.database_contract(d)

        def check():
            recovery.admit_retired_reader(worker_process,worker,saved,source_check,deadline)
            if host.load_receipt(path)!=saved:
                raise RuntimeError("storage_online_runtime_spool_journal_changed")
            for root,identity in ((source,source_identity),(destination,destination_identity)):
                info=root.stat()
                if root.resolve(strict=True)!=root or [info.st_dev,info.st_ino]!=identity:
                    raise RuntimeError("storage_online_runtime_spool_root_changed")
            if (destination.stat().st_uid,destination.stat().st_gid,stat.S_IMODE(destination.stat().st_mode))!=(1000,1000,0o700):
                raise RuntimeError("storage_online_runtime_spool_destination_permissions_changed")
            space=os.statvfs(destination)
            if space.f_bavail*space.f_frsize<reserve_bytes:
                raise RuntimeError("storage_online_runtime_spool_reserve_exhausted")
            current=host.inventory(project,operator_id=worker["container_id"])
            initial._admit_clients(current,preparation["clients"])
            if any(current[n]["running"] for n in host.STOP) or current["tsdb"]["id"]!=database_id:
                raise RuntimeError("storage_online_runtime_spool_source_changed")
            d=host.database_details(database_id)
            if (host.database_contract(d)!=database_contract
                    or sorted(d["mounts"],key=lambda m:m["Destination"])!=database_mounts
                    or not host.same_database_networks(d,database_networks)):
                raise RuntimeError("storage_online_runtime_spool_database_changed")
            if host.digest(json.loads(host.docker("inspect","--format","{{json .State}}",preparer)))!=preparer_state:
                raise RuntimeError("storage_online_runtime_spool_repository_helper_changed")
            if journal["helper_id"] is not None and helper_contract()!=journal["helper_contract"]:
                raise RuntimeError("storage_online_runtime_spool_helper_changed")
            if journal["helper_retirement"] is not None:
                current_state=json.loads(host.docker("inspect","--format","{{json .State}}",journal["helper_id"]))
                if host.digest(current_state)!=journal["helper_retirement"]:
                    raise RuntimeError("storage_online_runtime_spool_helper_retirement_changed")

        def dispatch(action,arguments):
            check();journal["inflight"]=action;host.save_receipt(path,saved,initial=False)
            print("event=storage_online_runtime_spool_dispatch action="+action+" intent_retained=true",file=sys.stderr,flush=True)
            host.supervised_source_action(arguments,deadline=deadline,check=check)
        def completed(action):
            journal["completed"].append(action);journal["inflight"]=None
            host.save_receipt(path,saved,initial=False);check()
        dispatch("create",args)
        ids=host.docker("ps","-aq","--no-trunc","--filter","name=^/"+name+"$").split()
        if len(ids)!=1:raise RuntimeError("storage_online_runtime_spool_helper_missing")
        journal["helper_id"]=ids[0];journal["helper_contract"]=helper_contract();completed("create")
        dispatch("copy",["start","--attach",ids[0]])
        status=json.loads(host.docker("inspect","--format","{{json .State}}",ids[0]))
        if any(status.get(k) is not False for k in ("Running","Paused","Restarting","Dead","OOMKilled")) or status.get("Pid")!=0 or status.get("ExitCode")!=0:
            raise RuntimeError("storage_online_runtime_spool_helper_not_retired")
        reply=json.loads(host.docker("logs","--tail","1",ids[0]))
        if set(reply)!={"manifest_sha256"}:
            raise RuntimeError("storage_online_runtime_spool_copy_reply_invalid")
        report=_copy_report(destination,reply["manifest_sha256"])
        if (report.get("source_preserved") is not True or report.get("runtime_activation_authorized") is not False
                or report.get("final_switch_authorized") is not False
                or not isinstance(report.get("copied_files"),list) or len(report["copied_files"])>4096
                or type(report.get("copied_bytes")) is not int or not 0<=report["copied_bytes"]<=max_bytes
                or report["original_observation"]["pending_files"]!=len(report["copied_files"])
                or report["original_observation"]["pending_bytes"]!=report["copied_bytes"]):
            raise RuntimeError("storage_online_runtime_spool_copy_unconfirmed")
        journal["helper_retirement"]=host.digest(status)
        journal["report"]={k:v for k,v in report.items() if k!="copied_files"}
        journal["report"].update(manifest_sha256=reply["manifest_sha256"],copied_files=len(report["copied_files"]))
        completed("copy")
        if host.cluster_identifier(database_id,maintenance=True)!=preparation["cluster"]:
            raise RuntimeError("storage_online_runtime_spool_cluster_changed")
        if host.docker("exec","--user","70:70",database_id,"sha256sum","/run/quanttrad/recovery/pgbackrest.conf").split()!=[original["archiver_config_sha256"],"/run/quanttrad/recovery/pgbackrest.conf"]:
            raise RuntimeError("storage_online_runtime_spool_archive_config_changed")
        saved["phase"]="recovery_spool_ready";journal["finished_at"]=time.time()
        host.save_receipt(path,saved,initial=False);check()
        return {"working_root":str(destination),"copied_bytes":report["copied_bytes"],
                "copied_files":len(report["copied_files"]),"source_preserved":True,
                "runtime_activation_authorized":False}


RUNTIME_RECIPE = "storage-online-runtime.compose.json"
_APPLICATIONS = {
    "initialize": "portal.backend.workers.single_node_initializer",
    "backend": "portal.backend.run_backend",
    "market-data-collector": "portal.backend.workers.market_data_collector",
    "storage-maintenance": "portal.backend.workers.storage_maintenance",
}

_APPLICATION_HEALTH = {
    "backend": ["CMD","python","-c","import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/instruments/health', timeout=3).read()"],
    "market-data-collector": ["CMD","python","-m","portal.backend.workers.market_data_collector_health"],
    "storage-maintenance": ["CMD","python","-m","portal.backend.workers.market_data_collector_health","--storage-maintenance"],
}


def admit_runtime_recipe(state_root, saved, worker, preparation, rows):
    """Admit the existing split application composition before replacing clients.

    This private rendered snapshot contains only the four application services
    and the already prepared database. Infrastructure remains exactly owned by
    the source receipt; it is not redeployed by application recovery.
    """
    model = host.load_receipt(state_root/RUNTIME_RECIPE, max_bytes=524288)
    database_model = host.load_receipt(state_root/recovery.RECIPE)
    project = saved["binding"]["project"]
    if (set(model)!={"name","services","volumes","networks"} or model["name"]!=project
            or set(model["services"])!=set(_APPLICATIONS)|{"tsdb"}
            or model["networks"]!=database_model["networks"]
            or model["volumes"]!=database_model["volumes"]
            or model["services"]["tsdb"]!=database_model["services"]["tsdb"]
            or host.digest(database_model)!=saved["recovery"]["recipe_sha256"]):
        raise RuntimeError("storage_online_runtime_fixed_composition_required")
    binding=worker["binding"]
    request_raw=preserving._runtime_configuration_bytes(state_root/"storage-online-request.json")
    if hashlib.sha256(request_raw).hexdigest()!=binding["request_sha256"]:
        raise RuntimeError("storage_online_runtime_request_changed")
    request=json.loads(request_raw)
    group=launch.archive_group_override(request)
    if group!="70":
        raise RuntimeError("storage_online_runtime_shared_archive_required")
    image=json.loads(host.docker("image","inspect","--format","{{json .}}",binding["image"]))
    image_env=dict(v.split("=",1) for v in image["Config"].get("Env") or [])
    if (image["Id"]!=binding["image"]
            or image_env.get("QT_IMAGE_SOURCE_REVISION")!=request["source_revision"]
            or image_env.get("QT_IMAGE_SOURCE_TREE_HASH")!=request["source_tree_hash"]):
        raise RuntimeError("storage_online_runtime_source_changed")
    controls=binding["mounts"]
    inventory=Path(controls["/run/qt-online/inventory.json"]["source"])
    raw=preserving._runtime_configuration_bytes(inventory)
    if hashlib.sha256(raw).hexdigest()!=binding["inventory_sha256"]:
        raise RuntimeError("storage_online_runtime_inventory_changed")
    targets=json.loads(raw)["targets"]
    if len(targets)!=2 or {t["medium"] for t in targets}!={"ssd","hdd"}:
        raise RuntimeError("storage_online_runtime_targets_changed")
    ssd=next(t for t in targets if t["medium"]=="ssd")
    hdd=next(t for t in targets if t["medium"]=="hdd")
    dbmounts={m["target"]:m for m in database_model["services"]["tsdb"]["volumes"]}
    history=dbmounts["/qt-history"]["source"]
    archive=Path(history)/"archives"
    info=archive.stat()
    if archive.resolve(strict=True)!=archive or info.st_gid!=int(group) or stat.S_IMODE(info.st_mode)!=0o2770:
        raise RuntimeError("storage_online_runtime_archive_root_not_prepared")
    database=host.database_details(saved["recovery"]["replacement_id"])
    collector=host.database_details(rows["market-data-collector"]["id"])
    launch._dsn(database,collector)  # Validate original credentials/cluster, without using the worker loopback DSN.
    dsn=dict(v.split("=",1) for v in collector["config"]["Env"])["PG_DSN"]
    literal=lambda value:value.replace("$$","$") if isinstance(value,str) else value
    files={str(inventory):hashlib.sha256(raw).hexdigest()}
    def bind(source,target,readonly=False):
        return dict(type="bind",source=str(source),target=target,read_only=readonly,
                    bind=dict(create_host_path=False))
    def normalized_mount(value):
        value=deepcopy(value)
        value.setdefault("read_only",False)
        if value.get("volume")=={}:value.pop("volume")
        value["source"]=literal(value["source"])
        return value
    for name,module in _APPLICATIONS.items():
        service=model["services"][name]
        maintenance=name=="storage-maintenance"
        if (service.get("image")!=binding["image"] or service.get("pull_policy")!="never"
                or service.get("user")!=("70:70" if maintenance else "1000:1000")
                or service.get("command")!=["python","-m",module]
                or service.get("entrypoint") is not None or service.get("init") is not True
                or service.get("cap_drop")!=["ALL"] or service.get("security_opt")!=["no-new-privileges:true"]
                or (service.get("pid","") not in ("service:tsdb", "container:"+saved["recovery"]["replacement_id"])
                    if maintenance else service.get("pid","") != "")
                or service.get("restart") not in ("no","unless-stopped")
                or set(service.get("networks",{}))!={"quanttrad"}
                or any(service.get(k) for k in ("privileged","devices","device_cgroup_rules","cap_add",
                    "volumes_from","sysctls","network_mode","ipc","uts","userns_mode","tmpfs"))
                or any(k in service for k in ("build","env_file","extends","profiles","configs","secrets"))):
            raise RuntimeError("storage_online_runtime_application_contract_changed: service="+name)
        groups=[str(v) for v in service.get("group_add",[])]
        if (len(set(groups))!=len(groups) or group not in groups
                or (name!="backend" and groups!=[group])
                or (name=="backend" and (not 1<=len(groups)<=2 or any(not v.isdecimal() for v in groups)))):
            raise RuntimeError("storage_online_runtime_archive_membership_changed")
        environment=dict(image_env)
        for key,value in service.get("environment",{}).items():
            if value is None:environment.pop(key,None)
            else:environment[key]=literal(str(value))
        expected={"PG_DSN":dsn,"QT_DISABLE_DOTENV":"1","QT_STORAGE_MAINTENANCE_OWNER":"dedicated",
            "QT_ARCHIVE_SHARED_GROUP_ID":group,"MARKET_STRUCTURE_STORAGE_ROOT":"/qt-history/archives",
            "MARKET_STRUCTURE_WORKING_ROOT":"/qt-history/archives" if maintenance else "/app/logs/market-structure",
            "QT_MARKET_DATA_EXPECTED_UUID":hdd["filesystem_uuid"],
            "QT_MARKET_DATA_WORKING_EXPECTED_UUID":(hdd if maintenance else ssd)["filesystem_uuid"],
            "QT_STORAGE_INVENTORY_PATH":"/run/quanttrad/storage-inventory.json",
            "QT_STORAGE_UDEV_ROOT":"/run/qt-host-udev/data"}
        if (any(environment.get(k)!=v for k,v in expected.items())
                or "QT_STORAGE_SOURCE_FENCE_ROOT" in environment):
            raise RuntimeError("storage_online_runtime_application_environment_changed: service="+name)
        if name=="backend" and environment.get("QT_MARKET_DATA_ROOT")!=history+"/archives":
            raise RuntimeError("storage_online_runtime_bot_archive_root_changed")
        expected_mounts=[dbmounts["/qt-history"],bind(inventory,"/run/quanttrad/storage-inventory.json",True),
            bind(Path(controls["/run/qt-online/udev"]["source"]),"/run/qt-host-udev/data",True)]
        if maintenance:
            expected_mounts += [dbmounts[k] for k in ("/var/lib/postgresql/data","/run/quanttrad/recovery","/var/run/postgresql")]
            if (any(environment.get(k)!="true" for k in ("QT_MARKET_DATA_LIFECYCLE_ENABLED",
                    "QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED","QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED"))
                    or environment.get("QT_STORAGE_MAINTENANCE_LIMITS_PATH")!="/run/quanttrad/storage-maintenance.json"):
                raise RuntimeError("storage_online_runtime_single_maintenance_owner_required")
            limits=[m for m in service.get("volumes",[]) if m.get("target")=="/run/quanttrad/storage-maintenance.json"]
            if len(limits)!=1:raise RuntimeError("storage_online_runtime_limits_mount_missing")
            limit_path=Path(literal(limits[0]["source"]))
            limit_raw=preserving._runtime_configuration_bytes(limit_path)
            preserving._validate_runtime_maintenance(binding["image"],limit_path,[t["target_id"] for t in targets])
            if preserving._runtime_configuration_bytes(limit_path)!=limit_raw:
                raise RuntimeError("storage_online_runtime_limits_changed")
            files[str(limit_path)]=hashlib.sha256(limit_raw).hexdigest()
            expected_mounts.append(bind(limit_path,"/run/quanttrad/storage-maintenance.json",True))
        else:
            expected_mounts.append(bind(saved["runtime_spool"]["destination"],"/app/logs/market-structure"))
            # Existing env_file rendering does not require a secret-file mount.
            # If the original service had one, preserve that exact read-only path.
            old=host.database_details(rows[name]["id"])
            secrets=[m for m in old["mounts"] if m["Destination"]=="/app/secrets.env"]
            if secrets:
                if len(secrets)!=1 or secrets[0]["Type"]!="bind" or secrets[0]["RW"]:
                    raise RuntimeError("storage_online_runtime_original_secret_mount_invalid")
                expected_mounts.append(bind(secrets[0]["Source"],"/app/secrets.env",True))
            if name=="backend":
                expected_mounts.append(dbmounts["/var/lib/postgresql/data"])
                sockets=[m for m in old["mounts"] if m["Destination"]=="/var/run/docker.sock"]
                if sockets:
                    if len(sockets)!=1 or sockets[0]["Type"]!="bind" or sockets[0]["Source"]!="/var/run/docker.sock":
                        raise RuntimeError("storage_online_runtime_original_socket_invalid")
                    expected_mounts.append(bind("/var/run/docker.sock","/var/run/docker.sock",not sockets[0]["RW"]))
        actual=[normalized_mount(m) for m in service.get("volumes",[])]
        expected=[normalized_mount(m) for m in expected_mounts]
        if sorted(actual,key=lambda m:m["target"])!=sorted(expected,key=lambda m:m["target"]):
            raise RuntimeError("storage_online_runtime_application_mount_changed: service="+name)
        if name!="initialize" and service.get("healthcheck",{}).get("test")!=_APPLICATION_HEALTH[name]:
            raise RuntimeError("storage_online_runtime_healthcheck_required")
    if host.digest(host.load_receipt(state_root/RUNTIME_RECIPE,max_bytes=524288))!=host.digest(model):
        raise RuntimeError("storage_online_runtime_recipe_changed")
    return model,dict(recipe_sha256=host.digest(model),files=files,
        images={n:binding["image"] for n in _APPLICATIONS},archive_identity=[info.st_dev,info.st_ino,info.st_gid,stat.S_IMODE(info.st_mode)])


_RUNTIME_ACTIONS = tuple(["remove:"+n for n in _APPLICATIONS if n!="storage-maintenance"]+
    ["create:"+n for n in _APPLICATIONS]+["start:"+n for n in _APPLICATIONS])


def validate_runtime_journal(saved):
    value=saved["runtime"]
    if (not isinstance(value,dict) or set(value)!={"started_at","deadline_monotonic","completed","inflight",
            "finished_at","admission","compose_hashes","candidate_ids"}
            or type(value["started_at"]) not in (int,float)
            or not saved["runtime_spool"]["finished_at"]<=value["started_at"]<=saved["deadline"]
            or type(value["deadline_monotonic"]) not in (int,float)
            or not math.isfinite(value["deadline_monotonic"])
            or not 0<value["deadline_monotonic"]<=saved["switch"]["deadline_monotonic"]
            or not isinstance(value["completed"],list)
            or value["completed"]!=list(_RUNTIME_ACTIONS[:len(value["completed"])])
            or len(value["completed"])>len(_RUNTIME_ACTIONS)
            or value["inflight"] is not None and (len(value["completed"])==len(_RUNTIME_ACTIONS)
                or value["inflight"]!=_RUNTIME_ACTIONS[len(value["completed"])])
            or not isinstance(value["candidate_ids"],dict)
            or set(value["candidate_ids"])!={n for n in _APPLICATIONS if "create:"+n in value["completed"]}
            or any(not isinstance(v,str) or not re.fullmatch(r"[0-9a-f]{64}",v) for v in value["candidate_ids"].values())
            or len(set(value["candidate_ids"].values()))!=len(value["candidate_ids"])):
        raise RuntimeError("storage_online_runtime_journal_invalid")
    if saved["phase"]=="recovery_runtime_ready":
        if (value["completed"]!=list(_RUNTIME_ACTIONS) or value["inflight"] is not None
                or type(value["finished_at"]) not in (int,float)
                or not value["started_at"]<=value["finished_at"]<=saved["deadline"]):
            raise RuntimeError("storage_online_runtime_completion_invalid")
    elif value["finished_at"] is not None:
        raise RuntimeError("storage_online_runtime_completion_invalid")


def activate_runtime(state_root, *, saved, worker_process, source_check, max_duration_seconds):
    """One live application replacement, with intent before every daemon action.

    No retry or rollback after a possibly sent action. This starts the existing
    collector before maintenance and never waits for a full physical baseline.
    The final marker remains until the complete operation has been qualified.
    """
    if type(max_duration_seconds) is not int or not 1<=max_duration_seconds<=600:
        raise ValueError("storage_online_runtime_duration_invalid")
    deadline=min(time.monotonic()+max_duration_seconds,saved["switch"]["deadline_monotonic"])
    worker=host.load_receipt(state_root/launch._STATE);preparation=initial._load(state_root)
    if (host.digest(worker)!=saved["binding"]["worker_sha256"]
            or host.digest(preparation)!=saved["binding"]["preparation_sha256"]):
        raise RuntimeError("storage_online_runtime_binding_changed")
    project=saved["binding"]["project"];database_id=saved["recovery"]["replacement_id"]
    path=state_root/"storage-online-final.json";recipe_path=state_root/RUNTIME_RECIPE
    destination=Path(saved["runtime_spool"]["destination"])
    with host.docker_deadline(deadline):
        recovery.admit_retired_reader(worker_process,worker,saved,source_check,deadline)
        rows=host.inventory(project,operator_id=worker["container_id"])
        initial._admit_clients(rows,preparation["clients"])
        if any(rows[n]["running"] for n in host.STOP) or rows["tsdb"]["id"]!=database_id:
            raise RuntimeError("storage_online_runtime_source_changed")
        model,admission=admit_runtime_recipe(state_root,saved,worker,preparation,rows)
        database=host.database_details(database_id)
        dbcontract=host.database_contract(database);dbmounts=sorted(database["mounts"],key=lambda m:m["Destination"])
        networks=host.database_networks(database)
        # Compose resolves the service PID reference before hashing a created
        # container. Bind that resolution to the observed replacement database,
        # exactly as the existing preserving runtime does; never rewrite recipe.
        hash_model=deepcopy(model)
        hash_model["services"]["storage-maintenance"]["pid"]="container:"+database_id
        hashes={}
        for line in host.docker("compose","--project-name",project,"--file","-","config","--hash","*",
                               input=json.dumps(hash_model)).splitlines():
            name,digest=line.split()
            if name in hashes or not re.fullmatch(r"[0-9a-f]{64}",digest):
                raise RuntimeError("storage_online_runtime_compose_hash_invalid")
            hashes[name]=digest
        if set(hashes)!=set(model["services"]):raise RuntimeError("storage_online_runtime_compose_hash_missing")
        # Use the existing Docker-vs-admitted-recipe observer, not another
        # interpretation of environment, mounts, image or health configuration.
        observation=dict(admission=admission,compose_hashes=hashes,binding=dict(database_id=database_id),
            receipt=dict(project=project,database_preparation=dict(networks=networks)))
        original={n:host.database_details(row["id"]) for n,row in rows.items() if n!="tsdb"}
        original_contracts={n:host.database_contract(d) for n,d in original.items()}
        manifest_digest=saved["runtime_spool"]["report"]["manifest_sha256"]
        _copy_report(destination,manifest_digest)
        saved.update(phase="recovery_runtime_starting",runtime=dict(started_at=time.time(),
            deadline_monotonic=deadline,completed=[],inflight=None,finished_at=None,
            admission=admission,compose_hashes=hashes,candidate_ids={}))
        journal=saved["runtime"];host.save_receipt(path,saved,initial=False)

        def check():
            recovery.admit_retired_reader(worker_process,worker,saved,source_check,deadline)
            if (host.load_receipt(path)!=saved
                    or host.digest(host.load_receipt(recipe_path,max_bytes=524288))!=admission["recipe_sha256"]):
                raise RuntimeError("storage_online_runtime_saved_binding_changed")
            for filename,digest in admission["files"].items():
                if hashlib.sha256(preserving._runtime_configuration_bytes(Path(filename))).hexdigest()!=digest:
                    raise RuntimeError("storage_online_runtime_configuration_changed")
            for stage in ("runtime_spool","repositories"):
                value=saved[stage]
                if host.digest(json.loads(host.docker("inspect","--format","{{json .State}}",value["helper_id"])))!=value["helper_retirement"]:
                    raise RuntimeError("storage_online_runtime_helper_retirement_changed")
            root=destination.stat()
            if (destination.resolve(strict=True)!=destination
                    or [root.st_dev,root.st_ino]!=saved["runtime_spool"]["destination_identity"]
                    or (root.st_uid,root.st_gid,stat.S_IMODE(root.st_mode))!=(1000,1000,0o700)):
                raise RuntimeError("storage_online_runtime_working_root_changed")
            _copy_report(destination,manifest_digest)
            archive=Path(next(m["source"] for m in model["services"]["tsdb"]["volumes"] if m["target"]=="/qt-history"))/"archives"
            archive_info=archive.stat()
            if (archive.resolve(strict=True)!=archive or [archive_info.st_dev,archive_info.st_ino,
                    archive_info.st_gid,stat.S_IMODE(archive_info.st_mode)]!=admission["archive_identity"]):
                raise RuntimeError("storage_online_runtime_archive_root_changed")
            actual=host.database_details(database_id)
            if (host.database_contract(actual)!=dbcontract
                    or sorted(actual["mounts"],key=lambda m:m["Destination"])!=dbmounts
                    or not host.same_database_networks(actual,networks)):
                raise RuntimeError("storage_online_runtime_database_changed")
            inflight=journal["inflight"] or ""
            removing=inflight.removeprefix("remove:") if inflight.startswith("remove:") else None
            current=host.inventory(project,operator_id=worker["container_id"],activating=True,runtime_maintenance=True,
                removing_client_id=rows[removing]["id"] if removing else None)
            for name,row in current.items():
                if name=="tsdb":
                    if row["id"]!=database_id:raise RuntimeError("storage_online_runtime_database_changed")
                    continue
                candidate=name in journal["candidate_ids"] or inflight=="create:"+name
                if candidate:
                    if name in journal["candidate_ids"] and row["id"]!=journal["candidate_ids"][name]:
                        raise RuntimeError("storage_online_runtime_candidate_changed")
                    details=preserving._runtime_candidate_details(name,row,model,observation)
                    service=model["services"][name];h=details["host"]
                    if (h.get("CapDrop")!=["ALL"] or h.get("SecurityOpt")!=service["security_opt"]
                            or sorted(h.get("GroupAdd") or [])!=sorted(str(v) for v in service["group_add"])
                            or h.get("Init") is not True or h.get("VolumesFrom") or h.get("Sysctls")
                            or h.get("IpcMode") not in ("private","") or h.get("UsernsMode") or h.get("UTSMode")):
                        raise RuntimeError("storage_online_runtime_candidate_privileges_changed")
                    if "start:"+name in journal["completed"]:
                        status=json.loads(host.docker("inspect","--format","{{json .State}}",row["id"]))
                        if (status["OOMKilled"] or status["Paused"] or status["Restarting"]
                                or (name=="initialize" and (status["Running"] or status["ExitCode"]!=0 or status["Status"]!="exited"))
                                or (name!="initialize" and (not status["Running"] or status.get("Health",{}).get("Status")!="healthy"))):
                            raise RuntimeError("storage_online_runtime_candidate_lost_health")
                    started="start:"+name in journal["completed"] or inflight=="start:"+name
                    if not started and (row["running"] or row["pid"]!=0 or row["status"]!="created"):
                        raise RuntimeError("storage_online_runtime_candidate_started_early")
                else:
                    if name not in rows or row["id"]!=rows[name]["id"] or "remove:"+name in journal["completed"]:
                        raise RuntimeError("storage_online_runtime_unowned_client")
                    if name==removing:
                        # The exact stopped client may disappear between inventory
                        # and inspect while its already-journaled rm completes.
                        # Its contract was checked before dispatch; no later
                        # action is admitted until removal itself is confirmed.
                        if (row["running"] or row["pid"]!=0 or row["exit_code"] not in (0,143)
                                or any(row[k] for k in ("paused","restarting","oom"))):
                            raise RuntimeError("storage_online_runtime_removing_client_changed")
                        continue
                    d=host.database_details(row["id"])
                    if (host.database_contract(d)!=original_contracts[name]
                            or sorted(d["mounts"],key=lambda m:m["Destination"])!=sorted(original[name]["mounts"],key=lambda m:m["Destination"])):
                        raise RuntimeError("storage_online_runtime_original_client_changed")
                    if name in host.STOP and row["running"]:
                        raise RuntimeError("storage_online_runtime_source_restarted")
            required={"tsdb"}|(set(rows)-set(_APPLICATIONS))|set(journal["candidate_ids"])
            required|={n for n in _APPLICATIONS if n in rows and "remove:"+n not in journal["completed"] and n!=removing}
            if not required<=set(current):raise RuntimeError("storage_online_runtime_client_missing")
            return current

        check()
        for action in _RUNTIME_ACTIONS:
            kind,name=action.split(":")
            check();journal["inflight"]=action;host.save_receipt(path,saved,initial=False)
            if kind=="remove":arguments=["rm",rows[name]["id"]]
            elif kind=="create":arguments=["compose","--project-name",project,"--file",str(recipe_path),"up","--no-start","--no-deps","--no-recreate","--no-build","--pull","never",name]
            else:arguments=["start",*(["--attach"] if name=="initialize" else []),journal["candidate_ids"][name]]
            print("event=storage_online_runtime_dispatch action="+action+" intent_retained=true",file=sys.stderr,flush=True)
            host.supervised_source_action(arguments,deadline=deadline,check=check)
            current=check()
            if kind=="remove" and name in current:raise RuntimeError("storage_online_runtime_original_still_present")
            if kind=="create":
                if name not in current:raise RuntimeError("storage_online_runtime_candidate_missing")
                journal["candidate_ids"][name]=current[name]["id"]
            if kind=="start":
                while True:
                    row=check()[name]
                    status=json.loads(host.docker("inspect","--format","{{json .State}}",row["id"]))
                    if status["OOMKilled"] or status["Paused"] or status["Restarting"]:
                        raise RuntimeError(f"storage_online_runtime_candidate_unstable: service={name} "
                            f"oom={status['OOMKilled']} paused={status['Paused']} restarting={status['Restarting']}")
                    if name=="initialize":
                        if status["Running"] or status["ExitCode"]!=0 or status["Status"]!="exited":
                            raise RuntimeError("storage_online_runtime_initializer_failed")
                        break
                    if not status["Running"]:raise RuntimeError("storage_online_runtime_candidate_exited")
                    if status.get("Health",{}).get("Status")=="healthy":break
                    if status.get("Health",{}).get("Status")!="starting":
                        raise RuntimeError("storage_online_runtime_candidate_unhealthy")
                    time.sleep(min(.2,max(0,deadline-time.monotonic())))
            journal["completed"].append(action);journal["inflight"]=None;host.save_receipt(path,saved,initial=False);check()
        if host.cluster_identifier(database_id,maintenance=True)!=preparation["cluster"]:
            raise RuntimeError("storage_online_runtime_cluster_changed")
        saved["phase"]="recovery_runtime_ready";journal["finished_at"]=time.time()
        host.save_receipt(path,saved,initial=False);check()
        return dict(application_containers=dict(journal["candidate_ids"]),collector_process_healthy=True,
                    complete_backup_confirmed=False,ordinary_relaunch_authorized=False)
