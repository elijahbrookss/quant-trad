"""Linux capability boundary exercised only in its isolated worker container."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(os.getenv("QT_TEST_ONLINE_READ_IDENTITY") != "1",
    reason="requires disposable read-only source and exact initial Linux capabilities")


def _run(body):
    result = subprocess.run([sys.executable, "-c", """
import errno, hashlib, json, os, subprocess, sys
from pathlib import Path
from time import monotonic
from scripts.automation.storage_online_worker import enter_source_read_identity, _status
source=Path('/qt-online-source')
target=Path('/qt-online-target')
info=source.stat()
""" + body], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.splitlines()[-1])


def test_read_only_legacy_source_copies_exact_bytes_without_ownership_changes():
    result = _run("""
files=[source/'root-private',source/'collector-private']
before=[(p.stat().st_uid,p.stat().st_gid,p.stat().st_mode,p.stat().st_ino) for p in files]
receipt=enter_source_read_identity(source,expected_device=info.st_dev,expected_inode=info.st_ino)
assert os.getuid()==70 and _status()['CapEff']==4 and _status()['NoNewPrivs']==1
from market_data.archive import FilesystemRawArchiveObjectStore
from scripts.db.archive_file_v2_proof import ArchiveFileProof
store=FilesystemRawArchiveObjectStore(target/'objects')
with ArchiveFileProof(target/'objects',max_files=16,max_bytes=1024**2,deadline=monotonic()+20) as proof:
 for p in files:
  data=p.read_bytes(); digest=hashlib.sha256(data).hexdigest()
  ack=store.put_verified(object_key=p.name,source_path=p,expected_sha256=digest)
  assert ack.byte_count==len(data)
  proof.observe(key=p.name,sha256=digest,byte_count=len(data))
  assert (target/'objects'/p.name).read_bytes()==data
 proof.verify_all()
assert before==[(p.stat().st_uid,p.stat().st_gid,p.stat().st_mode,p.stat().st_ino) for p in files]
assert not receipt['source_ownership_changed']
print(json.dumps(receipt))
""")
    assert result["source_readonly"] and result["uid"] == 70


def test_read_identity_cannot_write_bypass_regain_root_or_pass_capability_to_exec():
    result = _run("""
enter_source_read_identity(source,expected_device=info.st_dev,expected_inode=info.st_ino)
for action in (lambda:(source/'root-private').write_bytes(b'forbidden'),
               lambda:os.chmod(source/'root-private',0o666),
               lambda:(target/'foreign-private').write_bytes(b'forbidden'),
               lambda:os.chown(target/'foreign-private',70,70),
               lambda:os.setresuid(0,0,0)):
 try: action()
 except OSError as exc: assert exc.errno in (errno.EPERM,errno.EACCES,errno.EROFS)
 else: raise AssertionError('write or root escalation admitted')
(target/'worker-owned').write_bytes(b'ordinary UID70 writes remain possible')
child=subprocess.run([sys.executable,'-c',
 "from pathlib import Path; Path('/qt-online-source/root-private').read_bytes()"],
 capture_output=True,text=True,timeout=10)
assert child.returncode!=0 and 'PermissionError' in child.stderr
assert _status()['CapEff']==4 and _status()['CapAmb']==0 and _status()['CapInh']==0
print(json.dumps({'write_bypass':False,'child_capability':False,'root_regained':False}))
""")
    assert result == {"write_bypass": False, "child_capability": False, "root_regained": False}


def test_identity_or_writable_mount_refuses_before_changing_identity():
    result = _run("""
for root,device,inode,expected in (
 (source,info.st_dev,info.st_ino+1,'identity_changed'),
 (target,target.stat().st_dev,target.stat().st_ino,'readonly_mount_required')):
 try: enter_source_read_identity(root,expected_device=device,expected_inode=inode)
 except RuntimeError as exc: assert expected in str(exc)
 else: raise AssertionError('unbound source admitted')
 assert os.getuid()==0 and _status()['CapEff']==196
print(json.dumps({'source_drift_refused':True,'writable_source_refused':True}))
""")
    assert result["source_drift_refused"] and result["writable_source_refused"]

def test_multithreaded_entry_refuses_before_identity_change():
    result = _run("""
import threading
release=threading.Event()
worker=threading.Thread(target=lambda: release.wait(5))
worker.start()
try:
 try: enter_source_read_identity(source,expected_device=info.st_dev,expected_inode=info.st_ino)
 except RuntimeError as exc: assert str(exc)=='storage_online_initial_capabilities_invalid'
 else: raise AssertionError('multithreaded credential transition admitted')
 assert os.getuid()==0
finally:
 release.set();worker.join()
print(json.dumps({'multithreaded_entry_refused':True}))
""")
    assert result["multithreaded_entry_refused"]


def test_live_old_identity_publication_remains_readable_through_readonly_worker_mount():
    result = _run("""
enter_source_read_identity(source,expected_device=info.st_dev,expected_inode=info.st_ino)
from market_data.archive import FilesystemRawArchiveObjectStore
from scripts.db.archive_file_v2_proof import ArchiveFileProof
import time
store=FilesystemRawArchiveObjectStore(target/'objects')
observed=set();deadline=monotonic()+5
with ArchiveFileProof(target/'objects',max_files=16,max_bytes=1024**2,deadline=deadline) as proof:
 while len(observed)<5 and monotonic()<deadline:
  identity=int((source/'current').read_text())
  p=source/('live-'+str(identity))
  if identity not in observed:
   info=p.stat();assert info.st_uid==1000 and info.st_mode&0o777==0o600
   digest=hashlib.sha256(p.read_bytes()).hexdigest()
   store.put_verified(object_key=p.name,source_path=p,expected_sha256=digest)
   proof.observe(key=p.name,sha256=digest,byte_count=info.st_size)
   observed.add(identity)
  time.sleep(0.02)
 proof.verify_all()
assert len(observed)==5
assert source.stat().st_uid==1000 and source.stat().st_mode&0o777==0o750
print(json.dumps({'concurrent_immutable_publications':len(observed),'source_owner':1000}))
""")
    assert result["concurrent_immutable_publications"] == 5
