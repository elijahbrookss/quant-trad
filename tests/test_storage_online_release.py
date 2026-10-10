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


def test_explicit_runtime_budgets_survive_private_handoff(completed):
    root, source, saved, model = completed
    selected = {
        "backend": {"QT_RESEARCH_EVIDENCE_BYTES":"1073741824", "QT_HISTORY_READ_CACHE_BYTES":"0",
            "QT_HISTORY_READ_CACHE_MIN_FREE_BYTES":"496426550887"},
        "storage-maintenance": {"QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS":"60",
            "QT_MARKET_DATA_LIFECYCLE_CANONICAL_MAX_STEPS_PER_RUN":"4",
            "QT_MARKET_DATA_LIFECYCLE_CANONICAL_MAX_RUN_SECONDS":"60"},
    }
    for owner, values in selected.items():
        model["services"][owner].setdefault("environment", {}).update(values)
    model["services"]["backend"]["environment"]["UNRELATED_SETTING"] = "not-a-binding"
    release.host.save_receipt(root/release.runtime.RUNTIME_RECIPE, model, initial=False)
    saved["runtime"]["admission"]["recipe_sha256"] = release.host.digest(model)
    release.host.save_receipt(root/"storage-online-final.json", saved, initial=False)
    original = source.read_bytes()
    source.write_bytes(original+b"QT_RESEARCH_EVIDENCE_BYTES=268435456\n")
    original = source.read_bytes()
    release.prepare_deployment_environment(root, environment_path=source, saved=saved, execute=True)
    values = dotenv_values(root/release.PREPARED_ENVIRONMENT, interpolate=False)
    for settings in selected.values():
        for key, expected in settings.items(): assert values[key] == expected
    assert "UNRELATED_SETTING" not in values
    assert source.read_bytes() == (root/release.SOURCE_ENVIRONMENT).read_bytes() == original
    assert values["POSTGRES_PASSWORD"] == "private$with#chars"


@pytest.mark.parametrize("key,owner", [
    ("QT_RESEARCH_EVIDENCE_BYTES", "backend"),
    ("QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS", "storage-maintenance"),
])
@pytest.mark.parametrize("value", ["0", "-1", "1.5", "NaN", " 60", "01", "9223372036854775808", True, None])
def test_runtime_control_refuses_invalid_value_before_writing(completed, key, owner, value):
    root, source, saved, model = completed
    model["services"][owner].setdefault("environment", {})[key] = value
    before = source.read_bytes()
    with pytest.raises(RuntimeError, match="runtime_setting_invalid"):
        release.deployment_bindings(root, saved, model)
    assert source.read_bytes() == before and not (root/release.PREPARED_ENVIRONMENT).exists()


def test_runtime_control_refuses_conflicting_service_settings(completed):
    root, _, saved, model = completed
    key = "QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS"
    model["services"]["storage-maintenance"]["environment"] = {key:"60"}
    model["services"]["backend"]["environment"][key] = "3600"
    with pytest.raises(RuntimeError, match="runtime_setting_conflict"):
        release.deployment_bindings(root, saved, model)


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


@pytest.mark.parametrize("execute", [False, True])
def test_repair_operation_stages_only_after_stopped_runtime_proof(monkeypatch, completed, execute):
    root, source, saved, _ = completed
    request = {"policy":{}, "source_revision":"c"*40, "source_tree_hash":"d"*64}
    saved["binding"].update(source_revision="original")
    saved["commit"] = {"source_image":"source-image"}
    release.host.save_receipt(root/"storage-online-final.json", saved, initial=False)
    release.host.save_receipt(root/operation.launch._STATE, {"binding":{"image":"candidate"}}, initial=True)
    release.host.save_receipt(root/"storage-online-request.json", request, initial=True)
    plan = dict(state_root=str(root), limits=vars(operation.OperationLimits(1,1,1,1,1,1,0,1,0,0)),
        project="qt-original", source_revision="original", source_image="source-image", image="candidate",
        request=request, spool_destination=saved["runtime_spool"]["destination"],
        deployment_environment=str(source), deployment_repository=str(root))
    repair = repair_request(saved)
    package = root/"repair.json"
    release.host.save_receipt(package, repair, initial=True)
    monkeypatch.setattr(operation, "load_operation_plan", lambda _:copy.deepcopy(plan))
    monkeypatch.setattr(operation.final, "_load", lambda _:copy.deepcopy(saved))
    events = []
    def observe(*args, **kwargs):
        assert kwargs == {"repair_release":True}
        events.append("observe")
        return dict(ready=False, repair_release_admissible=True, complete_backup_confirmed=False)
    def configure(*args, **kwargs):
        assert kwargs["repair"] == repair
        events.append("configure")
        return {}
    def publish(*args, **kwargs):
        assert kwargs["repair"] == repair
        events.append("publish")
        return dict(complete_backup_confirmed=False)
    monkeypatch.setattr(operation.final, "inspect_runtime_completion_locked", observe)
    monkeypatch.setattr(release, "inspect_deployment_configuration", configure)
    monkeypatch.setattr(operation.final, "publish_deployment_configuration_locked", publish)
    result = operation.run_operation_plan(root/"operation.json", repair_release_file=package, execute=execute)
    assert not result["complete_backup_confirmed"]
    assert events == (["observe", "configure", "publish"] if execute else ["observe"])
    assert (root/release.PREPARED_ENVIRONMENT).exists() is execute
    if execute:
        assert source.read_bytes() == (root/release.SOURCE_ENVIRONMENT).read_bytes()
    else:
        assert source.read_bytes().startswith(b"# retained credentials")


@pytest.mark.parametrize("other", ["prepare_forward_only", "extend_attempt_seconds", "capacity_file",
    "replacement_package_file", "cancel_attempt_file", "forward_package_file", "prepare_forward_keys_file",
    "place_forward_lookups_file", "reschedule_forward_file", "recover_repositories_file"])
def test_repair_release_refuses_migration_actions_before_reading_inputs(other):
    with pytest.raises(ValueError, match="repair_release_must_be_separate"):
        operation.run_operation_plan("/does-not-exist", repair_release_file="/does-not-exist", **{other:True})


def test_repair_cli_is_explicit_and_does_not_open_http_or_dispatch_deployment(monkeypatch):
    from cli import main
    monkeypatch.setattr(main, "_client", lambda _:pytest.fail("HTTP client opened"))
    calls = []
    monkeypatch.setattr(operation, "run_operation_plan", lambda path, **kwargs:calls.append((path,kwargs)) or {})
    for execute in (False, True):
        args = main.build_parser().parse_args(["storage", "migrate", "--operation-file", "/operation.json",
            "--repair-release-file", "/repair.json", *(["--execute"] if execute else [])])
        assert args.func(args) == 0
    assert calls == [("/operation.json", dict(execute=value, repair_release_file="/repair.json")) for value in (False, True)]


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


@pytest.fixture
def configuration_pair():
    bindings={"QT_STORAGE_NETWORK":"qt_quanttrad", "QT_MARKET_DATA_WORKING_EXPECTED_UUID":"ssd-uuid",
              "QT_MARKET_DATA_EXPECTED_UUID":"hdd-uuid"}
    request={"source_revision":"c"*40, "source_tree_hash":"e"*64}
    services={}
    for name in ("tsdb", *release.runtime._APPLICATIONS):
        image="sha256:"+("d" if name=="tsdb" else "a")*64
        services[name]=dict(image=image,pull_policy="never",environment={"PG_DSN":"private-fixture",
            "PROVIDER_MODE":"preserved", "QT_MARKET_DATA_WORKING_EXPECTED_UUID":"hdd-uuid" if name=="storage-maintenance" else "ssd-uuid"},
            volumes=[dict(type="bind",source="/run/udev/data",target="/run/qt-host-udev/data",read_only=True,bind=dict(create_host_path=False))],
            networks={"quanttrad":{}},command=["unchanged"],user="70:70" if name=="tsdb" else "1000:1000",
            cap_drop=["ALL"],mem_limit=1024,healthcheck={"test":["CMD","unchanged"]})
    services["backend"]["volumes"].append(dict(type="bind",source="/active.env",target="/app/secrets.env",read_only=True))
    services["backend"]["environment"].update(SOURCE_REVISION="b"*40,
        SOURCE_TREE_HASH="f"*64, QT_BOT_RUNTIME_IMAGE="quanttrad-backend:"+"b"*40)
    admitted=dict(name="qt",services=services,volumes={"postgres-data":dict(name="pg",external=True),
        "storage-recovery-socket":dict(name="socket",external=True)},networks={"quanttrad":dict(name="qt_quanttrad",external=True)})
    proposed=copy.deepcopy(admitted)
    proposed["networks"]["quanttrad"]["ipam"]={}
    for name,service in proposed["services"].items():
        if name!="tsdb":service["image"]="quanttrad-backend:"+request["source_revision"]
        if name in ("backend","initialize","market-data-collector"):service["build"]={"context":"exact-clean-checkout"}
        service["environment"].update(bindings)
        if name=="storage-maintenance":service["environment"]["QT_MARKET_DATA_WORKING_EXPECTED_UUID"]="hdd-uuid"
        service["volumes"][0].update(source="/run/udev",target="/run/qt-host-udev")
    proposed["services"]["backend"]["volumes"][1]["source"]="/prepared.env"
    proposed["services"]["backend"]["environment"].update(SOURCE_REVISION=request["source_revision"],
        SOURCE_TREE_HASH=request["source_tree_hash"], QT_BOT_RUNTIME_IMAGE="quanttrad-backend:"+request["source_revision"])
    images={v["image"]:{"PATH":"/fixture","IMAGE_DEFAULT":"unchanged"} for v in services.values()}
    return proposed,dict(admitted=admitted,bindings=bindings,request=request,
        source_environment=Path("/active.env"),prepared_environment=Path("/prepared.env"),image_environments=images)


def test_canonical_comparison_accepts_only_explicit_proposal_differences(configuration_pair):
    proposed,args=configuration_pair
    before=copy.deepcopy((proposed,args))
    result=release.compare_deployment_configuration(proposed,**args)
    assert result==dict(storage_configuration_sha256=release.host.digest(args["admitted"]),
                        canonical_configuration_sha256=release.host.digest(proposed))
    assert (proposed,args)==before


@pytest.mark.parametrize("key", ["SOURCE_REVISION", "SOURCE_TREE_HASH", "QT_BOT_RUNTIME_IMAGE"])
@pytest.mark.parametrize("fault", ["old", "missing", "unqualified"])
def test_release_identity_requires_exact_candidate_values(configuration_pair, key, fault):
    proposed, args = configuration_pair
    backend = proposed["services"]["backend"]["environment"]
    if fault == "old": backend[key] = args["admitted"]["services"]["backend"]["environment"][key]
    elif fault == "missing": backend.pop(key)
    else: backend[key] = "unqualified"
    with pytest.raises(RuntimeError, match="service_changed: service=backend fields=environment"):
        release.compare_deployment_configuration(proposed, **args)


def test_canonical_comparison_preserves_selected_runtime_controls(configuration_pair):
    proposed, args = configuration_pair
    controls = {"QT_RESEARCH_EVIDENCE_BYTES":"1073741824",
        "QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS":"60"}
    args["admitted"]["services"]["backend"]["environment"]["QT_RESEARCH_EVIDENCE_BYTES"] = controls["QT_RESEARCH_EVIDENCE_BYTES"]
    args["admitted"]["services"]["storage-maintenance"]["environment"]["QT_MARKET_DATA_LIFECYCLE_INTERVAL_SECONDS"] = "60"
    args["bindings"].update(controls)
    for service in proposed["services"].values(): service["environment"].update(controls)
    release.compare_deployment_configuration(proposed, **args)
    proposed["services"]["backend"]["environment"]["QT_RESEARCH_EVIDENCE_BYTES"] = "2147483648"
    with pytest.raises(RuntimeError, match="service_changed"):
        release.compare_deployment_configuration(proposed, **args)


@pytest.mark.parametrize("fault",["project","volume","network","db_image","db_build","app_image","pull",
    "command","credential","provider","capability","uid","limit","health","pid","mount",
    "udev_other_parent","udev_writable","secret_other_path","duplicate_mount","maintenance_uuid","extra_environment",
    "maintenance_build","service_missing"])
def test_canonical_comparison_refuses_drift_without_exposing_private_values(configuration_pair,fault):
    proposed,args=configuration_pair
    backend=proposed["services"]["backend"]
    if fault=="project":proposed["name"]="other"
    elif fault=="volume":proposed["volumes"]["postgres-data"]["name"]="other"
    elif fault=="network":proposed["networks"]["quanttrad"]["name"]="other"
    elif fault=="db_image":proposed["services"]["tsdb"]["image"]="other"
    elif fault=="db_build":proposed["services"]["tsdb"]["build"]={}
    elif fault=="app_image":backend["image"]="unqualified"
    elif fault=="pull":backend["pull_policy"]="always"
    elif fault=="command":backend["command"]=["changed"]
    elif fault=="credential":backend["environment"]["PG_DSN"]="SECRET-DO-NOT-EXPOSE"
    elif fault=="provider":backend["environment"]["PROVIDER_MODE"]="changed"
    elif fault=="capability":backend["cap_add"]=["DAC_READ_SEARCH"]
    elif fault=="uid":backend["user"]="0:0"
    elif fault=="limit":backend["mem_limit"]=2048
    elif fault=="health":backend["healthcheck"]={"test":["CMD","true"]}
    elif fault=="pid":backend["pid"]="service:tsdb"
    elif fault=="mount":backend["volumes"].append(dict(type="bind",source="/private",target="/new"))
    elif fault=="udev_other_parent":backend["volumes"][0]["source"]="/private"
    elif fault=="udev_writable":backend["volumes"][0]["read_only"]=False
    elif fault=="secret_other_path":backend["volumes"][1]["source"]="/other.env"
    elif fault=="duplicate_mount":backend["volumes"].append(copy.deepcopy(backend["volumes"][0]))
    elif fault=="maintenance_uuid":proposed["services"]["storage-maintenance"]["environment"]["QT_MARKET_DATA_WORKING_EXPECTED_UUID"]="ssd-uuid"
    elif fault=="extra_environment":backend["environment"]["UNKNOWN_OVERRIDE"]="changed"
    elif fault=="maintenance_build":proposed["services"]["storage-maintenance"]["build"]={}
    elif fault=="service_missing":proposed["services"].pop("initialize")
    with pytest.raises(RuntimeError,match="storage_online_deployment_") as exc:
        release.compare_deployment_configuration(proposed,**args)
    assert "SECRET-DO-NOT-EXPOSE" not in str(exc.value) and "private-fixture" not in str(exc.value)


@pytest.fixture(params=["legacy-four-fields", "empty-storage-layout"])
def publication(completed, request):
    root, source, saved, model = completed
    saved["binding"]["source_revision"] = "a"*40
    saved["runtime"]["finished_at"] = 1
    hold=dict(phase="database_prepared",project=saved["binding"]["project"],source_revision="a"*40)
    release.host.save_receipt(root/release.host.HOLD,hold,initial=True)
    for name, key in (("storage-online-preparation.json", "preparation_sha256"), ("storage-online-worker.json", "worker_sha256")):
        evidence={"fixture":name}
        if name=="storage-online-preparation.json":
            evidence.update(phase="serving",project=saved["binding"]["project"],
                source_revision="a"*40,hold_sha256=release.host.digest(hold))
        release.host.save_receipt(root/name,evidence,initial=True)
        saved["binding"][key]=release.host.digest(evidence)
    release.host.save_receipt(root/"storage-online-final.json", saved, initial=False)
    release.host.save_receipt(root/"storage-online-request.json", dict(source_revision="c"*40, source_tree_hash="d"*64), initial=True)
    metadata=root/"release.env"
    metadata.write_text("current_revision="+"a"*40+"\ncurrent_source_tree_hash="+"b"*64+
        "\nprevious_revision=\ndeployed_at=original\n"+
        ("storage_layout=\n" if request.param == "empty-storage-layout" else ""))
    metadata.chmod(0o600)
    release.prepare_deployment_environment(root, environment_path=source, saved=saved, execute=True)
    configuration=dict(storage_configuration_sha256=release.host.digest(model), canonical_configuration_sha256="e"*64,
        deployment_storage_sha256="f"*64, request_sha256=release.host.digest(release.host.load_receipt(root/"storage-online-request.json")), repository=str(root), files={n:"1"*64 for n in release._COMPOSE_FILES},
        ordinary_relaunch_authorized=False)
    return root, source, saved, configuration


def publish(publication, repair=None):
    root, source, saved, configuration = publication
    return release.publish_configuration(root, repository=root, environment_path=source, saved=saved, configuration=configuration, repair=repair)


@pytest.mark.parametrize("entry", ["inspect", "publish"])
@pytest.mark.parametrize("fault", ["extra", "missing", "other-revision", "claimed-layout", "duplicate", "malformed"])
def test_source_metadata_refuses_before_publication_or_render(publication, monkeypatch, entry, fault):
    root, source, saved, _ = publication
    path = root/"release.env"
    raw = path.read_bytes()
    if fault == "extra": raw += b"unknown=value\n"
    elif fault == "missing": raw = raw.replace(b"deployed_at=original\n", b"")
    elif fault == "other-revision": raw = raw.replace(b"a"*40, b"f"*40)
    elif fault == "claimed-layout": raw = raw.replace(b"storage_layout=\n", b"")+b"storage_layout=ssd-hdd-v1\n"
    elif fault == "duplicate": raw += b"current_revision="+b"a"*40+b"\n"
    elif fault == "malformed": raw += b"not-a-field\n"
    path.write_bytes(raw)
    before = {p:p.read_bytes() for p in root.iterdir() if p.is_file()}
    def unexpected(*args, **kwargs):
        pytest.fail("invalid source metadata must refuse before external inspection")
    monkeypatch.setattr(release.subprocess, "run", unexpected)
    monkeypatch.setattr(release.host, "docker", unexpected)
    with pytest.raises(RuntimeError, match="source_metadata_changed|metadata_invalid"):
        if entry == "inspect":
            release.inspect_deployment_configuration(root, repository=root, environment_path=source, saved=saved)
        else:
            publish(publication)
    assert {p:p.read_bytes() for p in root.iterdir() if p.is_file()} == before


def repair_request(saved):
    return dict(schema_version="qt.storage_repair_release.v1", final_sha256=release.host.digest(saved),
        source_revision="9"*40, source_tree_hash="8"*64)


@pytest.mark.parametrize("failure", ["before_environment", "after_environment", "after_release", None])
@pytest.mark.parametrize("repair", [False, True])
def test_terminal_publication_reconciles_only_exact_files_and_preserves_originals(publication, monkeypatch, failure, repair):
    root, source, saved, _ = publication
    request = repair_request(saved) if repair else None
    original_request = (root/"storage-online-request.json").read_bytes()
    original=source.read_bytes(); old_release=(root/"release.env").read_bytes()
    writer=release._publish_file
    def interrupted(path, **kw):
        if failure=="before_environment" and path==source:raise OSError("fixture publication interrupted")
        writer(path, **kw)
        if (failure=="after_environment" and path==source or failure=="after_release" and path.name=="release.env"):
            raise OSError("fixture publication interrupted")
    monkeypatch.setattr(release,"_publish_file",interrupted)
    if failure:
        with pytest.raises(OSError,match="fixture publication"):publish(publication, request)
        intent=release.host.load_receipt(root/"storage-online-final.json")
        assert intent["release"]["status"]=="publishing"
        assert intent["release"]["published_at"] is None
        assert source.read_bytes()==(original if failure=="before_environment" else (root/release.PREPARED_ENVIRONMENT).read_bytes())
        monkeypatch.setattr(release,"_publish_file",writer)
        result=release.reconcile_configuration_files(root,saved=intent)
    else:result=publish(publication, request)
    assert result["phase"]=="deployment_configuration_published"
    assert not result["ordinary_relaunch_authorized"] and not result["migration_replay_authorized"]
    assert (root/release.SOURCE_ENVIRONMENT).read_bytes()==original
    assert (root/release.SOURCE_RELEASE).read_bytes()==old_release
    assert source.read_bytes()==(root/release.PREPARED_ENVIRONMENT).read_bytes()
    assert source.stat().st_mode & 0o777==0o600
    values=release._release_values((root/"release.env").read_bytes())
    assert values["current_revision"]==values["previous_revision"]==""
    assert values["pending_storage_revision"] == ("9" if repair else "c")*40
    after = release.host.load_receipt(root/"storage-online-final.json")
    assert after["release"]["status"]=="published"
    assert {k:v for k,v in after.items() if k != "release"} == saved
    assert (root/"storage-online-request.json").read_bytes() == original_request
    if repair: assert after["release"]["repair"] == request


@pytest.mark.parametrize("fault", ["journal", "phase", "revision", "hash", "schema", "extra", "same-revision"])
def test_repair_request_cannot_change_original_evidence_or_reuse_broken_revision(publication, fault):
    root, source, saved, _ = publication
    request = repair_request(saved)
    before = {p:p.read_bytes() for p in root.iterdir() if p.is_file()}
    if fault == "journal": request["final_sha256"] = "0"*64
    elif fault == "phase": saved["phase"] = "recovery_runtime_starting"
    elif fault == "revision": request["source_revision"] = "branch-name"
    elif fault == "hash": request["source_tree_hash"] = None
    elif fault == "schema": request["schema_version"] = "unknown"
    elif fault == "extra": request["skip_checks"] = True
    elif fault == "same-revision": request["source_revision"] = "c"*40
    with pytest.raises(RuntimeError, match="repair_release_"):
        publish(publication, request)
    assert {p:p.read_bytes() for p in before} == before


@pytest.mark.parametrize("fault", [None, "candidate", "original-request", "original-journal", "repair-request"])
def test_repair_deployment_uses_new_candidate_without_rewriting_migration_request(publication, monkeypatch, fault):
    from scripts.automation import storage_online_final as final
    root, source, saved, _ = publication
    request = repair_request(saved)
    publish(publication, request)
    monkeypatch.setattr(final, "_load", lambda path: release.host.load_receipt(path))
    revision = request["source_revision"]
    if fault == "candidate": revision = "c"*40
    if fault == "original-request":
        original = release.host.load_receipt(root/"storage-online-request.json")
        original["source_revision"] = revision
        release.host.save_receipt(root/"storage-online-request.json", original, initial=False)
    if fault in ("original-journal", "repair-request"):
        changed = release.host.load_receipt(root/"storage-online-final.json")
        if fault == "original-journal": changed["runtime"]["finished_at"] = 2
        else: changed["release"]["repair"]["source_revision"] = "7"*40
        release.host.save_receipt(root/"storage-online-final.json", changed, initial=False)
    def admit():
        return release.admit_deployment(root, environment_path=source, repository=root, action="deploy", revision=revision)
    if fault:
        with pytest.raises(RuntimeError): admit()
    else:
        assert admit()["release"]["status"] == "published"
        (root/"release.env").write_text("current_revision="+revision+"\ncurrent_source_tree_hash="+request["source_tree_hash"]+
            "\nprevious_revision=\ndeployed_at=now\nstorage_layout=ssd-hdd-v1\n")
        release.record_deployment(root, environment_path=source, revision=revision, source_hash=request["source_tree_hash"])
        assert admit()["release"]["status"] == "deployed"
        assert admit()["release"]["repair"] == request


@pytest.mark.parametrize("admitted", [False, True])
def test_repair_publication_requires_fresh_stopped_runtime_admission(publication, monkeypatch, admitted):
    from scripts.automation import storage_online_final as final
    root, source, saved, configuration = publication
    request = repair_request(saved)
    monkeypatch.setattr(final, "_load", lambda path: release.host.load_receipt(path))
    events = []
    def observe(*args, **kwargs):
        assert kwargs == {"repair_release":True}
        events.append("fresh-stopped-runtime")
        return dict(ready=False, repair_release_admissible=admitted, complete_backup_confirmed=False)
    def inspect(*args, **kwargs):
        assert kwargs["repair"] == request
        events.append("exact-configuration")
        return configuration
    monkeypatch.setattr(final, "inspect_runtime_completion_locked", observe)
    monkeypatch.setattr(release, "inspect_deployment_configuration", inspect)
    if admitted:
        result = final.publish_deployment_configuration_locked(root, repository=root, environment_path=source, repair=request)
        assert result["deployment_revision"] == request["source_revision"]
        assert events == ["fresh-stopped-runtime", "exact-configuration"]
    else:
        with pytest.raises(RuntimeError, match="complete_recovery_required"):
            final.publish_deployment_configuration_locked(root, repository=root, environment_path=source, repair=request)
        assert events == ["fresh-stopped-runtime"]
        assert release.host.load_receipt(root/"storage-online-final.json") == saved


@pytest.mark.parametrize("fault", [None, "dirty", "wrong-revision", "wrong-hash", "storage-drift", "metadata-during-render"])
def test_repair_configuration_binds_clean_new_source_and_original_request(publication, configuration_pair, monkeypatch, fault):
    import subprocess
    from scripts.provenance.source_tree_hash import working_tree_hash
    root, source, saved, _ = publication
    proposed, args = configuration_pair
    repository = root/"checkout"
    repository.mkdir()
    for name in release._COMPOSE_FILES:
        path = repository/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# exact fixture configuration\n")
    (repository/"requirements.txt").write_text("# source material\n")
    def git(*argv):
        return subprocess.run(["git", "-C", str(repository), *argv], capture_output=True, text=True, check=True).stdout.strip()
    git("init", "-q")
    git("add", ".")
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgSign=false",
        "commit", "-qm", "fixture: source")
    revision = git("rev-parse", "HEAD")
    admitted = args["admitted"]
    admitted["services"]["backend"]["volumes"][1]["source"] = str(source)
    proposed["services"]["backend"]["volumes"][1]["source"] = str(root/release.PREPARED_ENVIRONMENT)
    for name in release.runtime._APPLICATIONS:
        proposed["services"][name]["image"] = "quanttrad-backend:"+revision
    saved["runtime"]["admission"]["recipe_sha256"] = release.host.digest(admitted)
    release.host.save_receipt(root/release.runtime.RUNTIME_RECIPE, admitted, initial=False)
    release.host.save_receipt(root/"storage-online-final.json", saved, initial=False)
    bindings = args["bindings"]
    (root/release.PREPARED_ENVIRONMENT).write_bytes(release._environment_bytes(source.read_bytes(), bindings))
    monkeypatch.setattr(release, "deployment_bindings", lambda *a:bindings)
    original_request = (root/"storage-online-request.json").read_bytes()
    repair = repair_request(saved)
    repair.update(source_revision=revision, source_tree_hash=working_tree_hash(repository))
    proposed["services"]["backend"]["environment"].update(SOURCE_REVISION=revision,
        SOURCE_TREE_HASH=repair["source_tree_hash"], QT_BOT_RUNTIME_IMAGE="quanttrad-backend:"+revision)
    if fault == "dirty": (repository/"requirements.txt").write_text("changed")
    if fault == "wrong-revision": repair["source_revision"] = "0"*40
    if fault == "wrong-hash": repair["source_tree_hash"] = "0"*64
    if fault == "storage-drift": proposed["services"]["backend"]["cap_add"] = ["SYS_ADMIN"]
    def docker(*argv, **kwargs):
        if argv[0] == "image":
            image = argv[-1]
            return json.dumps(dict(Id=image, Config=dict(Env=[k+"="+v for k,v in args["image_environments"][image].items()])))
        assert argv[0] == "compose"
        assert kwargs["env"]["QT_RELEASE_REVISION"] == revision
        if fault == "metadata-during-render":
            metadata = root/"release.env"
            metadata.write_bytes(metadata.read_bytes().replace(b"deployed_at=original", b"deployed_at=changed"))
        return json.dumps(proposed)
    monkeypatch.setattr(release.host, "docker", docker)
    def inspect():
        return release.inspect_deployment_configuration(root, repository=repository,
            environment_path=source, saved=saved, repair=repair)
    if fault:
        with pytest.raises(RuntimeError, match="exact_clean_checkout_required|source_hash_changed|service_changed|inspection_changed"):
            inspect()
    else:
        result = inspect()
        assert result["request_sha256"] == release.host.digest(json.loads(original_request))
        assert result["storage_configuration_sha256"] == release.host.digest(admitted)
    assert (root/"storage-online-request.json").read_bytes() == original_request
    assert release.host.load_receipt(root/"storage-online-final.json") == saved


@pytest.mark.parametrize("fault",["active_environment","active_release","artifact","journal"])
def test_interrupted_publication_refuses_conflict_without_repair(publication,monkeypatch,fault):
    root,source,_,_=publication
    writer=release._publish_file
    monkeypatch.setattr(release,"_publish_file",lambda *a,**k: (_ for _ in ()).throw(OSError("fixture")))
    with pytest.raises(OSError):publish(publication)
    intent=release.host.load_receipt(root/"storage-online-final.json")
    path={"active_environment":source,"active_release":root/"release.env","artifact":root/release.PREPARED_ENVIRONMENT,
          "journal":root/"storage-online-final.json"}[fault]
    path.write_bytes(b"conflicting input")
    before={p:p.read_bytes() for p in root.iterdir() if p.is_file()}
    monkeypatch.setattr(release,"_publish_file",writer)
    with pytest.raises((RuntimeError,ValueError)):
        release.reconcile_configuration_files(root,saved=intent)
    assert {p:p.read_bytes() for p in before}==before


@pytest.mark.parametrize("fault",[None,"other_revision","other_action","environment","request","unresolved","configuration","worker","preparation"])
def test_existing_deployer_admits_only_exact_published_release(publication,monkeypatch,fault):
    from scripts.automation import storage_online_final as final
    root,source,_,_=publication
    publish(publication)
    # This component test isolates the new release grant; final._load's original
    # complete migration journal validation remains independently required.
    monkeypatch.setattr(final,"_load",lambda path:release.host.load_receipt(path))
    revision="c"*40;action="deploy"
    if fault=="other_revision":revision="9"*40
    elif fault=="other_action":action="rollback"
    elif fault=="environment":source.write_bytes(b"different")
    elif fault=="request":release.host.save_receipt(root/"storage-online-request.json",dict(source_revision="9"*40,source_tree_hash="d"*64),initial=False)
    elif fault in ("worker","preparation"):
        release.host.save_receipt(root/("storage-online-"+fault+".json"),dict(changed=True),initial=False)
    elif fault in ("unresolved","configuration"):
        saved=release.host.load_receipt(root/"storage-online-final.json")
        if fault=="unresolved":saved["release"]["status"]="publishing";saved["release"]["published_at"]=None
        else:saved["release"]["configuration"]["ordinary_relaunch_authorized"]=True
        release.host.save_receipt(root/"storage-online-final.json",saved,initial=False)
    def admit():return release.admit_deployment(root,environment_path=source,repository=root,action=action,revision=revision)
    if fault:
        with pytest.raises(RuntimeError):admit()
    else:
        assert admit()["release"]["status"]=="published"


def test_recording_gap_allows_only_same_normal_deploy_then_full_release_record(publication,monkeypatch):
    from scripts.automation import storage_online_final as final
    root,source,_,_=publication
    publish(publication)
    monkeypatch.setattr(final,"_load",lambda path:release.host.load_receipt(path))
    (root/"release.env").write_text("current_revision="+"c"*40+"\ncurrent_source_tree_hash="+"d"*64+
        "\nprevious_revision=\ndeployed_at=now\nstorage_layout=ssd-hdd-v1\n")
    saved=release.admit_deployment(root,environment_path=source,repository=root,action="deploy",revision="c"*40)
    assert saved["release"]["status"]=="published" # file alone never records full fleet success
    release.record_deployment(root,environment_path=source,revision="c"*40,source_hash="d"*64)
    after=release.host.load_receipt(root/"storage-online-final.json")
    assert after["release"]["status"]=="deployed"
    # Completed migration must not freeze future credential/configuration changes.
    # The normal deployer's existing private-config/storage validation owns them.
    source.write_bytes(source.read_bytes()+b"FUTURE_SETTING=reviewed\n")
    assert release.admit_deployment(root,environment_path=source,repository=root,action="deploy",revision="9"*40)["release"]["status"]=="deployed"
    assert (root/release.SOURCE_RELEASE).exists() and (root/release.SOURCE_ENVIRONMENT).exists()


def test_actual_deployer_render_rejects_changed_storage_but_allows_unrelated_profile(configuration_pair,publication,monkeypatch):
    proposed,args=configuration_pair
    root,source,_,_=publication
    publish(publication)
    saved=release.host.load_receipt(root/"storage-online-final.json")
    saved["release"]["configuration"]["deployment_storage_sha256"]=release.deployment_storage_fingerprint(proposed,
        environment_path=source,prepared_path=root/release.PREPARED_ENVIRONMENT)
    monkeypatch.setattr(release,"admit_deployment",lambda *a,**k:saved)
    proposed["services"]["grafana"]=dict(image="unrelated-profile")
    def check():release.check_deployment_render(root,environment_path=source,repository=root,revision="c"*40,source_hash="d"*64,model=proposed)
    check()
    proposed["services"]["backend"]["environment"]["PROVIDER_MODE"]="changed"
    with pytest.raises(RuntimeError,match="render_changed"):check()


@pytest.mark.parametrize("ready,changed",[(False,False),(True,True),(True,False)])
def test_publication_owner_freshly_checks_runtime_and_rechecks_configuration_on_partial_write(publication,monkeypatch,ready,changed):
    from scripts.automation import storage_online_final as final
    root,source,_,configuration=publication
    monkeypatch.setattr(release,"_publish_file",lambda *a,**k: (_ for _ in ()).throw(OSError("interrupted")))
    with pytest.raises(OSError):publish(publication)
    monkeypatch.setattr(final,"_load",lambda path:release.host.load_receipt(path))
    events=[]
    def completion(*a,**k):events.append("runtime");return dict(ready=ready)
    def config(*a,**k):
        events.append("configuration")
        if changed:raise RuntimeError("fixture configuration changed")
        return configuration
    monkeypatch.setattr(final,"inspect_runtime_completion_locked",completion)
    monkeypatch.setattr(release,"inspect_deployment_configuration",config)
    monkeypatch.setattr(release,"reconcile_configuration_files",lambda *a,**k:events.append("files"))
    if not ready or changed:
        with pytest.raises(RuntimeError):final.publish_deployment_configuration_locked(root,repository=root,environment_path=source)
    else:final.publish_deployment_configuration_locked(root,repository=root,environment_path=source)
    assert events==(["runtime"] if not ready else ["runtime","configuration"] if changed else ["runtime","configuration","files"])


def test_deployer_checks_configuration_before_start_and_records_after_full_fleet():
    script=Path("scripts/automation/server_deploy.sh").read_text()
    body=script.split("deploy_release() {",1)[1].split("# All server mutations",1)[0]
    assert body.index("check_deployment_render") < body.index("compose up --detach")
    assert body.index("verify_release_image storage-maintenance") < body.index("record_release") < body.index("record_deployment")
    assert "require_no_storage_handoff \"${1:-}\"" in script


def test_already_published_file_is_synced_before_terminal_receipt(publication,monkeypatch):
    root,source,_,_=publication
    prepared=(root/release.PREPARED_ENVIRONMENT).read_bytes()
    source.write_bytes(prepared)
    synced=[]
    monkeypatch.setattr(release.host,"sync_directory",lambda path:synced.append(path))
    release._publish_file(source,before=b"original",after=prepared)
    assert synced==[source.parent]
    assert source.read_bytes()==prepared


@pytest.mark.parametrize("after_publication", [False, True])
@pytest.mark.parametrize("fault", ["missing", "changed", "unrelated", "preparation"])
def test_terminal_grant_requires_its_exact_retained_initial_hold(publication, monkeypatch, after_publication, fault):
    from scripts.automation import storage_online_final as final
    root, source, _, _ = publication
    if after_publication:
        publish(publication)
    marker=root/release.host.HOLD
    if fault=="missing":marker.unlink()
    elif fault in {"changed", "unrelated"}:
        hold=release.host.load_receipt(marker)
        hold["phase" if fault=="changed" else "project"]="different"
        release.host.save_receipt(marker,hold,initial=False)
    else:
        path=root/"storage-online-preparation.json"
        value=release.host.load_receipt(path);value["hold_sha256"]="0"*64
        release.host.save_receipt(path,value,initial=False)
    before={path:path.read_bytes() for path in root.iterdir() if path.is_file()}
    monkeypatch.setattr(final,"_load",lambda path:release.host.load_receipt(path))
    with pytest.raises((RuntimeError,OSError)):
        if after_publication:
            release.admit_deployment(root,environment_path=source,repository=root,action="deploy",revision="c"*40)
        else:publish(publication)
    assert before=={path:path.read_bytes() for path in root.iterdir() if path.is_file()}
