"""Native explicit-mount startup/refusal rehearsal; no production inputs.

Proves the real Docker contract and the worker's startup path reaches a deliberately
absent private database with stdout still reserved for protocol. It does NOT
qualify successful prepared-controller operation or host pause/restart.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
from uuid import uuid4

from scripts.automation import storage_online_launch as launch


def rehearse(image):
    project = "qt-online-launch-"+uuid4().hex[:12]
    root = Path(tempfile.mkdtemp(prefix=project+"-")).resolve()
    owned = []
    report = {"project": project, "production_inputs": False,
              "successful_prepared_controller_qualified": False}
    def run(args, *, timeout=60, check=True, env=None):
        result = subprocess.run(["docker", *args], capture_output=True, text=True,
                                timeout=timeout, env=env)
        if check and result.returncode:
            raise RuntimeError("online_launch_rehearsal_failed: "+result.stderr[-1500:])
        return result
    try:
        image_id = run(["image","inspect",image,"--format","{{.Id}}"]).stdout.strip()
        image_env = dict(v.split("=",1) for v in json.loads(run(
            ["image","inspect",image_id,"--format","{{json .Config.Env}}"]).stdout))
        for name in ("pg","history","working","udev","state","private"):
            (root/name).mkdir()
        (root/"working"/"objects").mkdir()
        (root/"private"/"key").write_text("DISPOSABLE SECRET MUST NOT BE MOUNTED")
        # No filesystem feature changes. This initialization owns this tiny tree.
        prepare=project+"-prepare";owned.append(prepare)
        code = """
from pathlib import Path
import os
root=Path('/fixture')
for name in ('pg','history'):
 os.chown(root/name,70,70);os.chmod(root/name,0o700)
for name in ('working','working/objects'):
 os.chown(root/name,1000,1000);os.chmod(root/name,0o750)
p=root/'working/objects/private';p.write_bytes(b'preserved fixture');p.chmod(0o600)
"""
        run(["run","--name",prepare,"--user","0:0","--network","none","--memory","64m","--cpus","0.25",
             "--label","qt.disposable="+project,"--mount","type=bind,source="+str(root)+",target=/fixture",
             "--entrypoint","python",image_id,"-c",code])
        info=(root/"working"/"objects").stat()
        request={"schema_version":"qt.storage_online_worker.v1",
            "source_revision":image_env["QT_IMAGE_SOURCE_REVISION"],
            "source_tree_hash":image_env["QT_IMAGE_SOURCE_TREE_HASH"],
            "database_identity":"1/1","source_device":info.st_dev,"source_inode":info.st_ino,
            "expected_started_at":"2026-09-26T00:00:00+00:00",
            "policy":{"recent":["ssd"],"history":["hdd"],"archives":["hdd"],"backups":["hdd"],
                      "movement_enabled":True,"backup_enabled":True},
            "resource_limits":{"wal_bytes":1024**2,"temporary_bytes":{"ssd":1024**2,"hdd":1024**2},
                "growth_bytes_per_second":{"ssd":0,"hdd":0},"maintenance_bytes":{"ssd":1024**2,"hdd":1024**2},
                "movement_timeout_seconds":30,"cancellation_grace_seconds":5},
            "max_page_bytes":1024**2,"max_objects":128,"max_bytes":1024**2,"page_rows":2,"command_seconds":5}
        data=(json.dumps(request,sort_keys=True,separators=(",",":"))+"\n").encode()
        request_path=root/"state"/"request.json";request_path.write_bytes(data);request_path.chmod(0o600)
        inventory=root/"state"/"inventory.json"
        inventory.write_text(json.dumps({"schema_version":"qt.storage_inventory.v1","targets":[
            {"target_id":"ssd","label":"Fixture SSD","filesystem_uuid":"fixture-ssd",
             "root":"/var/lib/postgresql/data","medium":"ssd"},
            {"target_id":"hdd","label":"Fixture HDD","filesystem_uuid":"fixture-hdd",
             "root":"/qt-history","medium":"hdd"}]}))
        peer=project+"-peer";owned.append(peer)
        run(["run","-d","--name",peer,"--network","none","--read-only","--memory","64m",
             "--cpus","0.25","--cap-drop","ALL","--label","qt.disposable="+project,
             "--mount","type=bind,source="+str(root/"pg")+",target=/var/lib/postgresql/data",
             "--mount","type=bind,source="+str(root/"history")+",target=/qt-history",
             "--mount","type=bind,source="+str(root/"private")+",target=/run/recovery-secrets,readonly",
             "--entrypoint","python",image_id,"-c","import time; time.sleep(180)"])
        peer_id=run(["inspect",peer,"--format","{{.Id}}"]).stdout.strip()
        database=launch.held._database_details(peer_id)
        collector={"mounts":[{"Type":"bind","Source":str(root/"working"),
                             "Destination":"/app/logs/market-structure","RW":True}]}
        try:
            launch._explicit_mounts(database,collector,inventory,request_path,root/"udev")
        except RuntimeError as exc:
            assert str(exc) == "storage_online_peer_mount_exposure_refused"
            report["peer_secret_mount_refused"]=True
        else:
            raise AssertionError("shared PID namespace must refuse peer secret mounts")
        run(["rm","-f",peer]);owned.remove(peer)
        run(["run","-d","--name",peer,"--network","none","--read-only","--memory","64m",
             "--cpus","0.25","--cap-drop","ALL","--label","qt.disposable="+project,
             "--mount","type=bind,source="+str(root/"pg")+",target=/var/lib/postgresql/data",
             "--mount","type=bind,source="+str(root/"history")+",target=/qt-history",
             "--entrypoint","python",image_id,"-c","import time; time.sleep(180)"])
        owned.append(peer)
        peer_id=run(["inspect",peer,"--format","{{.Id}}"]).stdout.strip()
        database=launch.held._database_details(peer_id)
        mounts,_=launch._explicit_mounts(database,collector,inventory,request_path,root/"udev")
        digest=hashlib.sha256(data).hexdigest()
        overrides={"PG_DSN":"postgresql+psycopg2://fixture:disposable@127.0.0.1:1/fixture",
                   "QT_DISABLE_DOTENV":"1","QT_LOGGING_LOKI_URL":"",
                   "QT_ONLINE_REQUEST_SHA256":digest,"QT_STORAGE_UDEV_ROOT":"/run/qt-online/udev"}
        binding={"image":image_id,"database_id":peer_id,
                 "database_hostname":database["config"]["Hostname"],"request_sha256":digest,
                 "mounts":mounts,"descriptor_limit":1024,"memory_bytes":512*1024**2,
                 "environment_sha256":launch.held._digest(sorted(
                     k+"="+v for k,v in {**image_env,**overrides}.items()))}
        worker=project+"-worker";owned.append(worker)
        import os
        identity=run(launch._arguments(worker,image_id,peer_id,mounts,overrides,1024,
                       512*1024**2,digest),env={**os.environ,"PG_DSN":overrides["PG_DSN"]}).stdout.strip()
        try:
            contract=launch._admit(identity,binding)
        except RuntimeError:
            details=launch.held._database_details(identity)
            details['config']['Env']=['<redacted>']
            report['admission_diagnostic']=details
            raise
        result=run(["start","--attach","--interactive",identity],check=False)
        launch._admit(identity,binding,contract)
        assert result.returncode == 1 and not result.stdout, "protocol stdout must remain empty"
        assert "event=storage_online_worker_failed error_type=OperationalError" in result.stderr, result.stderr[-1000:]
        assert not any(secret in result.stdout+result.stderr for secret in
                       ("DISPOSABLE SECRET","postgresql+psycopg2://","disposable@"))
        assert (root/"working").stat().st_uid == 1000
        details=launch.held._database_details(identity)
        assert not any("recovery-secrets" in m["Destination"] for m in details["mounts"])
        report.update(passed=True,image=image_id,container_contract_sha256=contract,
                      original_source_owner_preserved=True,database_unavailable_refused=True,
                      protocol_stdout_empty=True)
    finally:
        failures=[]
        for name in reversed(owned):
            if run(["rm","-f",name],check=False).returncode: failures.append(name)
        cleanup=project+"-cleanup"
        # Exact newly created owned root only; never a computed production path.
        assert root.name.startswith(project+"-") and root.parent == Path(tempfile.gettempdir()).resolve()
        result=run(["run","--rm","--name",cleanup,"--user","0:0","--network","none","--memory","64m",
            "--cpus","0.25","--mount","type=bind,source="+str(root)+",target=/fixture",
            "--entrypoint","python",image,"-c",
            "import shutil; from pathlib import Path; "
            "[(shutil.rmtree(p) if p.is_dir() else p.unlink()) for p in Path('/fixture').iterdir()]"],
            check=False)
        if result.returncode: failures.append(cleanup)
        else: root.rmdir()
        report["cleanup_failures"]=failures
        print("QT_ONLINE_LAUNCH="+json.dumps(report,sort_keys=True),flush=True)
        if failures: raise RuntimeError("online_launch_rehearsal_cleanup_failed")
    return report


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image",required=True)
    rehearse(parser.parse_args().image)
