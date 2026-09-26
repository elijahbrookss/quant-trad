#!/usr/bin/env python3
"""Qualify fixed root permissions on one owned disposable tmpfs container.

Only hardware audit/findmnt observations are synthetic. Filesystem ownership,
process UIDs, permissions, interruption and preservation checks are real.
"""
from pathlib import Path
import os
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]
PROBE = r"""
from pathlib import Path
import json,os,stat,subprocess,sys
sys.path.insert(0,"/proof")
import storage_host_prepare as helper
mount = Path("/srv/quanttrad/storage/hdd-fixture")
plan = dict(device="/dev/disk/by-id/fixture",expected_serial="fixture-serial",
    expected_size_bytes=1024,filesystem_uuid="df88b80e-1b89-49dc-87a9-ac5b48801b79",
    mountpoint=str(mount),owner="nobody")
helper.audit = lambda *args: {"topology":{"blockdevices":[dict(type="disk",
    serial=plan["expected_serial"],size=1024,fstype="ext4",uuid=plan["filesystem_uuid"],
    mountpoints=[str(mount)])]},"signatures":{"signatures":[{"type":"ext4"}]}}
def mounted(command):
    assert command[0] == "findmnt", "format/mount operation attempted"
    return plan["filesystem_uuid"]
helper.run = mounted
expected_refusal = os.environ.get("DIRECTORY_FIXTURE_REFUSAL")
if expected_refusal:
    try:
        helper.prepare_runtime_directories(plan)
        raise AssertionError("invalid filesystem admitted")
    except ValueError as exc:
        assert expected_refusal in str(exc), str(exc)
    print("PASS: filesystem refusal before directory mutation: "+expected_refusal)
    sys.exit(0)
original = helper.os.fchmod
interrupted = [False]
def lose_reply(fd,mode):
    if not interrupted[0]:
        interrupted[0] = True
        raise RuntimeError("injected_after_chown")
    return original(fd,mode)
helper.os.fchmod = lose_reply
try:
    try:
        helper.prepare_runtime_directories(plan)
        raise AssertionError("preparation was not interrupted")
    except RuntimeError as exc:
        assert str(exc) == "injected_after_chown"
finally:
    helper.os.fchmod = original
result = helper.prepare_runtime_directories(plan)
data,archives = mount/"data",mount/"data"/"archives"
for path in (data,archives):
    info = path.stat()
    assert (info.st_uid,info.st_gid,stat.S_IMODE(info.st_mode)) == (70,65534,0o770)
inodes = [p.stat().st_ino for p in (data,archives)]
def as_user(uid,gid,code,path):
    def demote():
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)
    subprocess.run([sys.executable,"-c",code,str(path)],preexec_fn=demote,check=True)
as_user(65534,65534,"from pathlib import Path; import sys; (Path(sys.argv[1])/'operator-proof').write_text('operator')",data)
as_user(70,70,"import os,sys; from pathlib import Path; p=Path(sys.argv[1])/'private-proof'; fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); os.write(fd,b'preserved'); os.close(fd)",archives)
proof = archives/"private-proof"
before = proof.stat()
assert helper.prepare_runtime_directories(plan) == result
after = proof.stat()
assert [p.stat().st_ino for p in (data,archives)] == inodes
assert (after.st_uid,after.st_gid,after.st_mode,after.st_ino) == (before.st_uid,before.st_gid,before.st_mode,before.st_ino)
assert proof.read_bytes() == b"preserved"
archives.chmod(0o700)
try:
    try:
        helper.prepare_runtime_directories(plan)
        raise AssertionError("nonempty directory permissions silently changed")
    except ValueError as exc:
        assert "requires_review" in str(exc)
    assert stat.S_IMODE(archives.stat().st_mode) == 0o700
finally:
    archives.chmod(0o770)
saved = data/"archives-retained"
archives.rename(saved)
archives.symlink_to("/etc",target_is_directory=True)
try:
    try:
        helper.prepare_runtime_directories(plan)
        raise AssertionError("symlink accepted")
    except OSError:
        pass
finally:
    archives.unlink()
    saved.rename(archives)
assert proof.read_bytes() == b"preserved"
# The legacy collector runs as root and publishes private 0600 files. Exercise
# that actual ownership boundary, including a reply lost after the first chown.
legacy=mount/"legacy";legacy.mkdir(mode=0o700);os.chown(legacy,1000,1000)
objects=legacy/"objects";objects.mkdir(mode=0o700)
private=objects/"retained";private.write_bytes(b"frozen archive bytes");private.chmod(0o600)
spool=legacy/"pending-spool";spool.write_bytes(b"uncommitted collection");spool.chmod(0o600)
def binding():
    info=legacy.stat()
    return dict(expected_device=info.st_dev,expected_inode=info.st_ino,max_duration_seconds=30)
def denied(uid,code):
    pid=os.fork()
    if pid==0:
        os.setgroups([]);os.setgid(uid);os.setuid(uid)
        try:exec(code)
        except PermissionError:os._exit(0)
        os._exit(1)
    assert os.waitpid(pid,0)[1]==0

denied(70,"private.read_bytes()")
before={path:(path.stat(),path.read_bytes() if path.is_file() else None)
        for path in (legacy,objects,private,spool)}
# Every unsupported entry must be refused during preflight, before any chown.
for kind in ("symlink","hardlink","foreign-owner"):
    bad=legacy/"unsupported"
    if kind=="symlink":bad.symlink_to("/etc")
    elif kind=="hardlink":os.link(private,bad)
    else:bad.write_bytes(b"foreign");os.chown(bad,65534,65534)
    try:
        try:helper.prepare_legacy_working_ownership(legacy,**binding())
        except ValueError as exc:assert "unsupported_entry" in str(exc)
        else:raise AssertionError("foreign legacy entry accepted")
        assert all(path.stat().st_uid==old.st_uid for path,(old,_) in before.items())
    finally:bad.unlink()
before={path:(path.stat(),path.read_bytes() if path.is_file() else None) for path in (legacy,objects,private,spool)}
original_chown=helper.os.fchown
lost=[False]
def interrupted_owner(fd,uid,gid):
    original_chown(fd,uid,gid)
    if not lost[0]:
        lost[0]=True
        raise RuntimeError("injected_after_legacy_chown")
helper.os.fchown=interrupted_owner
try:
    try:helper.prepare_legacy_working_ownership(legacy,**binding())
    except RuntimeError as exc:assert str(exc)=="injected_after_legacy_chown"
    else:raise AssertionError("ownership interruption missing")
finally:helper.os.fchown=original_chown
helper.prepare_legacy_working_ownership(legacy,**binding())
assert helper.prepare_legacy_working_ownership(legacy,**binding())["changed"]==0
for path,(old,contents) in before.items():
    now=path.stat()
    assert now.st_uid==70
    assert (now.st_dev,now.st_ino,now.st_mode,now.st_gid,now.st_size,now.st_mtime_ns)==(old.st_dev,old.st_ino,old.st_mode,old.st_gid,old.st_size,old.st_mtime_ns)
    if contents is not None:assert path.read_bytes()==contents
as_user(70,70,"from pathlib import Path;import sys;p=Path(sys.argv[1]);assert (p/'objects/retained').read_bytes()==b'frozen archive bytes';assert (p/'pending-spool').read_bytes()==b'uncommitted collection'",legacy)
print("PASS: interrupted legacy ownership transfer preserves archive/spool bytes, inodes, modes and groups; UID70 gains access; symlinks, hardlinks and foreign owners are refused before mutation")
# Qualify the packaged entry point's privilege boundary in a separate process.
import types
fixed=Path("/app/logs/market-structure")
fixed_file=fixed/"private";fixed_file.write_bytes(b"legacy bytes");fixed_file.chmod(0o600)
request=Path("/run/qt-handoff/request.json");request.write_text("{}\n");request.chmod(0o444)
os.environ.update(QT_HANDOFF_WORKING_DEVICE=str(fixed.stat().st_dev),
    QT_HANDOFF_WORKING_INODE=str(fixed.stat().st_ino),QT_HANDOFF_WORKING_SECONDS="30")
def db_boundary():
    assert os.getresuid()==(70,70,70) and os.getresgid()==(70,70,70) and not os.getgroups()
    assert next(line for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('CapEff:')).split()[1]=='0000000000000000'
    assert fixed_file.read_bytes()==b"legacy bytes" and sys.stdin.read()=="{}\n"
    return 0
module=types.ModuleType("scripts.db.fact_header_v2_handoff");module.database_operator_main=db_boundary
sys.modules[module.__name__]=module
for wrong in (True,False):
    pid=os.fork()
    if pid==0:
        if wrong:os.environ['QT_HANDOFF_WORKING_INODE']=str(fixed.stat().st_ino+1)
        os._exit(helper.legacy_working_operator_main())
    assert os.waitpid(pid,0)[1]==(256 if wrong else 0)
    assert fixed_file.stat().st_uid==(0 if wrong else 70)
print("PASS: exact source identity required before mutation; database entry point runs only after permanent UID/GID70 transition with no effective capabilities")
print(json.dumps(dict(interrupted_preparation_recovered=True,operator_and_runtime_roots_writable=True,
    private_files_and_directory_identities_preserved=True,foreign_metadata_and_symlinks_refused=True,
    physical_device_identity_tested=False,host_data_changed=False)))
"""


def main():
    token = uuid.uuid4().hex
    name = "qt-storage-directories-"+token[:12]
    env = {key:value for key,value in os.environ.items()
           if not key.startswith(("QT_","PG_","POSTGRES_","COMPOSE_"))}
    command = ["docker","run","--rm","--name",name,"--network","none","--read-only",
        "--memory","128m","--cpus","0.5","--pids-limit","32",
        "--label","qt.storage-directory-proof="+token,
        "--tmpfs","/srv/quanttrad/storage/hdd-fixture:rw,size=16m,mode=0755",
        "--tmpfs","/app/logs/market-structure:rw,size=1m,mode=0700",
        "--tmpfs","/run/qt-handoff:rw,size=64k,mode=0755",
        "--env","PYTHONDONTWRITEBYTECODE=1"]
    for filename in ("storage_host_prepare.py","storage_device_audit.py"):
        command += ["--mount","type=bind,source="+str(ROOT/"scripts/automation"/filename)+",target=/proof/"+filename+",readonly"]
    command += ["--pull","never","python:3.12.3-slim","python","-c",PROBE]
    readonly = [name+"-readonly" if value == name else value for value in command]
    readonly[readonly.index("/srv/quanttrad/storage/hdd-fixture:rw,size=16m,mode=0755")] = "/srv/quanttrad/storage/hdd-fixture:ro,size=16m,mode=0755"
    readonly[readonly.index("--pull"):readonly.index("--pull")] = ["--env","DIRECTORY_FIXTURE_REFUSAL=read_only"]
    foreign = [name+"-foreign" if value == name else value for value in command]
    foreign[foreign.index("--pull"):foreign.index("--pull")] = [
        "--env","DIRECTORY_FIXTURE_REFUSAL=wrong_filesystem",
        "--tmpfs","/srv/quanttrad/storage/hdd-fixture/data:rw,size=1m,mode=0700"]
    for candidate in (command,readonly,foreign):
        try:
            subprocess.run(candidate,cwd=ROOT,env=env,check=True,timeout=180)
        except subprocess.TimeoutExpired:
            subprocess.run(["docker","rm","--force",candidate[candidate.index("--name")+1]],
                           env=env,check=True,timeout=30)
            raise
    left = subprocess.check_output(["docker","ps","-aq","--filter",
        "label=qt.storage-directory-proof="+token],env=env,text=True).strip()
    if left:
        raise RuntimeError("disposable_directory_container_not_removed")
    print("PASS: owned runtime-directory fixture removed; no host/device writes")


if __name__ == "__main__":
    main()
