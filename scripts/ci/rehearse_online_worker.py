"""Disposable Linux read-identity rehearsal; no host data, DSN or keys accepted."""
from __future__ import annotations

import argparse
import json
import subprocess
from uuid import uuid4


def rehearse(image):
    project = "qt-online-identity-"+uuid4().hex[:12]
    volumes = [project+"-source", project+"-target"]
    containers = []
    def run(args, timeout=90, check=True):
        result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError("online_identity_rehearsal_failed: "+result.stdout[-12000:]+result.stderr[-2000:])
        return result
    def container(name, args, code):
        containers.append(name)
        return run(["run", "--name", name, "--network", "none", "--memory", "512m",
            "--cpus", "1", "--pids-limit", "64", "--label", "qt.disposable="+project,
            *args, "--entrypoint", "python", image, "-c", code])
    created = []
    report = {"project": project, "image": image, "production_data_mounted": False}
    try:
        for volume in volumes:
            run(["volume", "create", "--label", "qt.disposable="+project, volume])
            created.append(volume)
        initialize = """
from pathlib import Path
import os
source=Path('/source');target=Path('/target')
os.chown(source,1000,1000);os.chmod(source,0o750)
for name,uid in [('root-private',0),('collector-private',1000)]:
 p=source/name;p.write_bytes((name+' immutable probe').encode()*1024)
 os.chown(p,uid,uid);os.chmod(p,0o600)
os.chown(target,70,70);os.chmod(target,0o700)
objects=target/'objects';objects.mkdir();os.chown(objects,70,70);os.chmod(objects,0o700)
foreign=target/'foreign-private';foreign.write_bytes(b'not writable by UID70');os.chmod(foreign,0o600)
"""
        container(project+"-prepare", ["--user", "0:0", "-v", volumes[0]+":/source",
                  "-v", volumes[1]+":/target"], initialize)
        # The operator must reject an unprepared destination before pausing.
        # Exercise its actual UID70/read-only probe, without source or SQL access.
        container(project+"-archive-prepare", ["--user","70:70","--cap-drop","ALL",
                  "--read-only","-v",volumes[1]+":/target"],
                  "from pathlib import Path;p=Path('/target/archives/objects');p.mkdir(parents=True,mode=0o700)")
        archive_args=["--user","70:70","--read-only","--cap-drop","ALL",
            "--security-opt","no-new-privileges","-v",volumes[1]+":/qt-history:ro",
            "-e","QT_DISABLE_DOTENV=1","-e","QT_ARCHIVE_SHARED_GROUP_ID=70",
            "-e","QT_LOGGING_LOKI_URL="]
        container(project+"-archive-refuse",archive_args,"""
from pathlib import Path
from scripts.automation.storage_online_operation import _ARCHIVE_DESTINATION_PROBE
root=Path('/qt-history/archives/objects');before=root.stat()
try: exec(_ARCHIVE_DESTINATION_PROBE)
except PermissionError as exc: assert 'shared_directory_invalid' in str(exc)
else: raise AssertionError('private archive destination admitted')
after=root.stat();assert (before.st_ino,before.st_mode,before.st_uid,before.st_gid)==(after.st_ino,after.st_mode,after.st_uid,after.st_gid)
assert not any(root.iterdir())
""")
        container(project+"-archive-ready",["--user","70:70","--cap-drop","ALL",
                  "--read-only","-v",volumes[1]+":/target"],
                  "from pathlib import Path;Path('/target/archives/objects').chmod(0o2770)")
        result=container(project+"-archive-admit",archive_args,
                  "from scripts.automation.storage_online_operation import _ARCHIVE_DESTINATION_PROBE;exec(_ARCHIVE_DESTINATION_PROBE)")
        report["archive_preflight"]=json.loads(result.stdout)
        assert report["archive_preflight"]["mode"]==0o2770
        args = ["--user", "0:0", "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=64m",
                "--security-opt", "no-new-privileges", "--cap-drop", "ALL",
                "--cap-add", "DAC_READ_SEARCH", "--cap-add", "SETUID", "--cap-add", "SETGID",
                "-v", volumes[0]+":/qt-online-source:ro",
                "-v", volumes[1]+":/qt-online-target:rw",
                "-e", "QT_TEST_ONLINE_READ_IDENTITY=1", "-e", "QT_DISABLE_DOTENV=1",
                "-e", "QT_LOGGING_LOKI_URL=", "-w", "/app"]
        # An independent old-identity writer continues through its RW mount;
        # the migration process sees this same owned fixture through RO only.
        writer = project+"-writer"
        containers.append(writer)
        writer_code = """
from pathlib import Path
import os,time
root=Path('/source')
for i in range(300):
 p=root/('live-'+str(i))
 p.write_bytes(('immutable live publication '+str(i)).encode()*64);p.chmod(0o600)
 marker=root/'current.tmp';marker.write_text(str(i));os.replace(marker,root/'current')
 time.sleep(0.05)
"""
        run(["run", "-d", "--name", writer, "--network", "none", "--memory", "64m",
             "--cpus", "0.25", "--pids-limit", "16", "--label", "qt.disposable="+project,
             "--read-only", "--user", "1000:1000", "--cap-drop", "ALL",
             "--security-opt", "no-new-privileges", "-v", volumes[0]+":/source:rw",
             "--entrypoint", "python", image, "-c", writer_code])
        # Reject extra authority before any UID transition, in a separate process.
        refusal = """
from pathlib import Path
from scripts.automation.storage_online_worker import enter_source_read_identity
root=Path('/qt-online-source');info=root.stat()
try: enter_source_read_identity(root,expected_device=info.st_dev,expected_inode=info.st_ino)
except RuntimeError as exc: assert str(exc)=='storage_online_initial_capabilities_invalid'
else: raise AssertionError('unexpected capabilities accepted')
"""
        container(project+"-extra-cap", [*args, "--cap-add", "CHOWN"], refusal)
        result = container(project+"-test", args,
            "import pytest; raise SystemExit(pytest.main(['-q','tests/test_storage_online_worker.py',"
            "'-s','-o','cache_dir=/tmp/qt-online-pytest']))")
        print(result.stdout, flush=True)
        report["passed"] = True
    finally:
        failures = []
        for name in reversed(containers):
            result = run(["rm", "-f", name], check=False)
            if result.returncode:
                failures.append(name)
        for volume in reversed(created):
            result = run(["volume", "rm", volume], check=False)
            if result.returncode:
                failures.append(volume)
        report["cleanup_failures"] = failures
        print("QT_ONLINE_IDENTITY="+json.dumps(report, sort_keys=True), flush=True)
        if failures:
            raise RuntimeError("online_identity_rehearsal_cleanup_failed")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    rehearse(parser.parse_args().image)
