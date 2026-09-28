"""Fixed post-switch application handover; preserving spool preparation first.

No CLI, replay engine, alternate DSN or policy owner. The final-state owner keeps
source/deployment holds; this helper has no database, keys, network or peer PID.
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
    if saved["phase"]=="recovery_spool_ready":
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
