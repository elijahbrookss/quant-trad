"""Private deployment handoff: preserve secrets/intent and refuse ambiguous inputs."""
import copy
import json
from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts.automation import storage_online_release as release
from scripts.automation import storage_online_operation as operation


@pytest.fixture
def completed(tmp_path):
    def mount(source,target):return dict(source=source,target=target)
    model=dict(services={
        "tsdb":dict(image="sha256:"+"a"*64,volumes=[mount("/history","/qt-history"),mount("/private/keys","/run/quanttrad/recovery")]),
        "backend":dict(group_add=["70","65534"],environment=dict(QT_ARCHIVE_SHARED_GROUP_ID="70",
            QT_MARKET_DATA_EXPECTED_UUID="hdd-uuid",QT_MARKET_DATA_WORKING_EXPECTED_UUID="ssd-uuid"),
            volumes=[mount("/private/inventory.json","/run/quanttrad/storage-inventory.json")]),
        "storage-maintenance":dict(volumes=[mount("/private/limits.json","/run/quanttrad/storage-maintenance.json")])},
        volumes={"postgres-data":dict(name="original-pg"),"storage-recovery-socket":dict(name="original-socket")},
        networks={"quanttrad":dict(name="original-network")})
    saved=dict(phase="recovery_runtime_ready",binding=dict(project="qt-original"),
        runtime=dict(admission=dict(recipe_sha256=release.host.digest(model))),
        runtime_spool=dict(destination="/ssd/new-private-spool"))
    release.host.save_receipt(tmp_path/release.runtime.RUNTIME_RECIPE,model,initial=True)
    release.host.save_receipt(tmp_path/"storage-online-final.json",saved,initial=True)
    source=tmp_path/"active.env"
    source.write_bytes(b"# retained credentials\r\nPOSTGRES_PASSWORD='private$with#chars'\r\nMULTILINE='first\nsecond'\nexport QT_STORAGE_SOURCE_FENCE_ROOT=/old/private\nQT_MARKET_DATA_ROOT=/old/private\n")
    source.chmod(0o600)
    return tmp_path,source,saved,model


def test_environment_preparation_preserves_original_secrets_and_all_journals(completed):
    root,source,saved,_=completed
    original=source.read_bytes();journal=(root/"storage-online-final.json").read_bytes()
    before=release.prepare_deployment_environment(root,environment_path=source,saved=saved)
    assert not before["environment_prepared"] and not (root/release.PREPARED_ENVIRONMENT).exists()
    result=release.prepare_deployment_environment(root,environment_path=source,saved=saved,execute=True)
    assert result["environment_prepared"] and not result["ordinary_relaunch_authorized"]
    assert not result["active_environment_changed"]
    prepared=root/release.PREPARED_ENVIRONMENT
    values=dotenv_values(prepared,interpolate=False)
    assert prepared.read_bytes().startswith(original[:original.index(b"export QT_STORAGE_SOURCE")])
    assert values["POSTGRES_PASSWORD"] == "private$with#chars" and values["MULTILINE"]=="first\nsecond"
    assert "QT_STORAGE_SOURCE_FENCE_ROOT" not in values
    assert values["QT_MARKET_DATA_ROOT"]=="/history/archives"
    assert values["QT_MARKET_DATA_WORKING_ROOT"]=="/ssd/new-private-spool"
    assert values["QT_STORAGE_POSTGRES_VOLUME"]=="original-pg"
    assert values["QT_STORAGE_RECOVERY_SOCKET_VOLUME"]=="original-socket"
    assert values["QT_STORAGE_NETWORK"]=="original-network"
    assert values["QT_STORAGE_DATABASE_IMAGE"]=="sha256:"+"a"*64
    assert b"QT_STORAGE_DATABASE_IMAGE=sha256:" in prepared.read_bytes()
    assert prepared.stat().st_mode & 0o777 == 0o600
    assert (root/release.SOURCE_ENVIRONMENT).read_bytes()==source.read_bytes()==original
    assert (root/"storage-online-final.json").read_bytes()==journal
    assert release.prepare_deployment_environment(root,environment_path=source,saved=saved,execute=True)==result


@pytest.mark.parametrize("fault",["duplicate","malformed","nonprivate","symlink","recipe","journal","partial","existing_other","mutable_db","socket","alias"])
def test_ambiguous_or_changed_handoff_refuses_without_repair(completed,fault):
    root,source,saved,model=completed
    if fault=="duplicate":source.write_text("A=one\nA=two\n")
    elif fault=="malformed":source.write_text("KEY='unterminated")
    elif fault=="nonprivate":source.chmod(0o640)
    elif fault=="symlink":
        target=source.with_name("secret-target");source.rename(target);source.symlink_to(target)
    elif fault=="recipe":model["services"]["tsdb"]["image"]="sha256:"+"b"*64
    elif fault=="journal":saved["binding"]["project"]="changed"
    elif fault in ("partial","existing_other"):
        p=root/release.PREPARED_ENVIRONMENT;p.write_bytes(b"partial" if fault=="partial" else b"different complete data\n");p.chmod(0o600)
    elif fault=="mutable_db":model["services"]["tsdb"]["image"]="mutable:tag"
    elif fault=="socket":model["services"]["backend"]["group_add"]=["70"]
    elif fault=="alias":source=root/release.SOURCE_ENVIRONMENT;source.write_bytes(b"private");source.chmod(0o600)
    if fault in ("recipe","mutable_db","socket"):
        release.host.save_receipt(root/release.runtime.RUNTIME_RECIPE,model,initial=False)
        if fault!="recipe":
            saved["runtime"]["admission"]["recipe_sha256"]=release.host.digest(model)
            release.host.save_receipt(root/"storage-online-final.json",saved,initial=False)
    prior={p:p.read_bytes() for p in root.iterdir() if p.is_file()}
    with pytest.raises((RuntimeError,OSError)):
        release.prepare_deployment_environment(root,environment_path=source,saved=saved,execute=True)
    for path,data in prior.items():assert path.read_bytes()==data


@pytest.mark.parametrize("ready,execute",[(False,True),(True,False),(True,True)])
def test_public_operation_requires_fresh_completion_before_staging(monkeypatch,completed,ready,execute):
    root,source,saved,_=completed
    request={"policy":{}}
    saved["binding"].update(source_revision="original")
    saved["commit"]={"source_image":"source-image"}
    release.host.save_receipt(root/"storage-online-final.json",saved,initial=False)
    release.host.save_receipt(root/operation.launch._STATE,{"binding":{"image":"candidate"}},initial=True)
    release.host.save_receipt(root/"storage-online-request.json",request,initial=True)
    plan=dict(state_root=str(root),limits=vars(operation.OperationLimits(1,1,1,1,1,1,0,1,0,0)),
        project="qt-original",source_revision="original",source_image="source-image",image="candidate",
        request=request,spool_destination=saved["runtime_spool"]["destination"],deployment_environment=str(source))
    monkeypatch.setattr(operation,"load_operation_plan",lambda _:copy.deepcopy(plan))
    monkeypatch.setattr(operation.final,"_load",lambda _:copy.deepcopy(saved))
    events=[]
    def observe(*a,**kw):events.append("fresh-completion");return dict(ready=ready,ordinary_relaunch_authorized=False)
    monkeypatch.setattr(operation.final,"inspect_runtime_completion_locked",observe)
    result=operation.run_operation_plan(root/"plan",execute=execute)
    assert events==["fresh-completion"]
    assert (root/release.PREPARED_ENVIRONMENT).exists() is (ready and execute)
    assert not result["ordinary_relaunch_authorized"]
    assert source.read_bytes().startswith(b"# retained credentials")


@pytest.mark.parametrize("failure",[None,"mutable","missing","rebuild"])
def test_storage_application_build_cannot_fetch_or_rebuild_database(tmp_path,failure):
    import os
    import subprocess
    script=Path("scripts/automation/server_deploy.sh").read_text().split('action="${1:-}"')[0]
    image="mutable:tag" if failure=="mutable" else "sha256:"+"a"*64
    probe=tmp_path/"probe.sh"
    probe.write_text(script+"""
recorded_storage_layout() { printf 'ssd-hdd-v1'; }
env_value() { :; }
profile_enabled() { return 1; }
compose() {
  printf '%s\\n' "$*" >>"$FIXTURE_CALLS"
  if test "$1 $2" = 'config --services'; then
    printf '%s\\n' tsdb backend frontend frontend-v2 loki storage-maintenance
  fi
}
docker() { test "$FIXTURE_MISSING" != 1; }
build_release_images
""")
    env={**os.environ,"QT_STORAGE_DATABASE_IMAGE":image,"QT_REBUILD_DATABASE_IMAGE":"1" if failure=="rebuild" else "0",
         "FIXTURE_MISSING":"1" if failure=="missing" else "0","FIXTURE_CALLS":str(tmp_path/"calls")}
    result=subprocess.run(["bash",str(probe)],env=env,text=True,capture_output=True)
    if failure:
        assert result.returncode and not (tmp_path/"calls").exists()
    else:
        assert result.returncode==0,result.stderr
        calls=(tmp_path/"calls").read_text().splitlines()
        assert calls==["config --services","pull --ignore-buildable backend frontend frontend-v2 loki",
                       "build --pull backend frontend frontend-v2"]


def test_input_change_during_staging_keeps_original_and_refuses_reentry(completed,monkeypatch):
    root,source,saved,_=completed
    original=source.read_bytes()
    preserve=release._preserve_file
    def changed(path,raw):
        preserve(path,raw)
        if path.name==release.SOURCE_ENVIRONMENT:source.write_bytes(original+b"NEW=concurrent\n")
    monkeypatch.setattr(release,"_preserve_file",changed)
    with pytest.raises(RuntimeError,match="input_changed"):
        release.prepare_deployment_environment(root,environment_path=source,saved=saved,execute=True)
    assert (root/release.SOURCE_ENVIRONMENT).read_bytes()==original
    monkeypatch.setattr(release,"_preserve_file",preserve)
    with pytest.raises(RuntimeError,match="artifact_changed"):
        release.prepare_deployment_environment(root,environment_path=source,saved=saved,execute=True)
    assert source.read_bytes()==original+b"NEW=concurrent\n"


@pytest.mark.parametrize("value",["/path with spaces","/path$VARIABLE","/path\nOTHER=1","/path'quote"])
def test_proposed_host_bindings_cannot_change_shell_or_dotenv_meaning(value):
    with pytest.raises(RuntimeError,match="binding_invalid"):
        release._environment_bytes(b"PASSWORD='unchanged'\n",{"QT_MARKET_DATA_ROOT":value})
