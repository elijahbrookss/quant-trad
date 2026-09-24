#!/usr/bin/env python3
"""Rehearse the fixed host hold with owned PostgreSQL and synthetic clients.

No production configuration, credentials, provider egress, or physical disks.
The history bind uses a disposable directory and synthetic UUID evidence.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.automation import storage_handoff_pause as pause


def run(args, *, env, timeout=300, ok=True):
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)
    if ok and result.returncode:
        # All inputs in this fixture are synthetic; no environment is printed.
        raise RuntimeError(f"disposable_database_hold_command_failed: {args[0]} exit={result.returncode}\n{result.stderr[-4000:]}")
    return result


def main():
    project = "qt-database-hold-"+uuid.uuid4().hex[:12]
    network = project+"_quanttrad"
    volume = project+"-postgres"
    env = {key: value for key, value in os.environ.items() if not key.startswith(("QT_", "PG_", "POSTGRES_", "COMPOSE_"))}
    print("Owned disposable project: "+project, flush=True)
    pg_image = run(["docker", "image", "inspect", "quanttrad-postgres:2.14.2-pg15", "--format", "{{.Id}}"], env=env).stdout.strip()
    client_image = run(["docker", "image", "inspect", "python:3.12.3-slim", "--format", "{{.Id}}"], env=env).stdout.strip()
    created_network = created_volume = False
    with tempfile.TemporaryDirectory(prefix="qt-database-hold-") as directory:
        root = Path(directory)
        state = root/"state"; state.mkdir()
        history = root/"history"; history.mkdir(); history.chmod(0o777)
        (history/"preparation-proof").write_text("retained-history-path")
        udev = root/"udev"; udev.mkdir()
        device = history.stat().st_dev
        (udev/f"b{os.major(device)}:{os.minor(device)}").write_text("E:ID_FS_UUID=fixture-history\n")
        env["QT_STORAGE_UDEV_ROOT"] = str(udev)
        revision = "a"*40
        (state/"release.env").write_text("current_revision="+revision+"\n")
        service = {
            "image": pg_image, "pull_policy": "never", "hostname": "tsdb.quanttrad",
            "command": ["postgres", "-c", "shared_buffers=64MB", "-c", "max_connections=20"],
            "environment": {"POSTGRES_DB": "quanttrad", "POSTGRES_USER": "quanttrad",
                            "POSTGRES_PASSWORD": uuid.uuid4().hex, "PGDATA": "/var/lib/postgresql/data/pgdata"},
            "init": True, "restart": "no", "shm_size": 1073741824,
            "healthcheck": {"test": pause._TCP_PROBE, "interval": "1s", "timeout": "2s", "retries": 120, "start_period": "10s"},
            "volumes": [{"type": "volume", "source": "postgres-data", "target": "/var/lib/postgresql/data"}],
            "networks": {"quanttrad": {"aliases": ["tsdb.quanttrad"]}},
        }
        model = {"name": project, "services": {"tsdb": service},
                 "volumes": {"postgres-data": {"name": volume, "external": True}},
                 "networks": {"quanttrad": {"name": network, "external": True}}}
        source = root/"source.compose.json"; source.write_text(json.dumps(model)); source.chmod(0o600)
        recipe = json.loads(json.dumps(model))
        recipe["services"]["tsdb"]["volumes"].append({"type": "bind", "source": str(history),
            "target": "/qt-history", "bind": {"create_host_path": False}})
        path = state/pause.DATABASE_RECIPE; path.write_text(json.dumps(recipe)); path.chmod(0o600)
        compose = ["docker", "compose", "--project-name", project, "--file", str(source)]
        def cid():
            return run(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=com.docker.compose.project="+project,
                        "--filter", "label=com.docker.compose.service=tsdb"], env=env).stdout.strip()
        def query(sql):
            return run(["docker", "exec", cid(), "sh", "-ec",
                'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -Atc "$1"',
                "fixture", sql], env=env).stdout.strip()
        old_udev = os.environ.get("QT_STORAGE_UDEV_ROOT")
        try:
            run(["docker", "network", "create", "--internal", network], env=env); created_network = True
            run(["docker", "volume", "create", volume], env=env); created_volume = True
            run(compose+["up", "--detach", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "180"], env=env)
            original = cid()
            original_details = pause._database_details(original)
            cluster = query("SELECT system_identifier FROM pg_control_system()")
            query("CREATE TABLE public.qt_host_hold_proof(value text PRIMARY KEY); INSERT INTO public.qt_host_hold_proof VALUES ('retained')")
            for name in pause.STOP+tuple(value for value in pause.PASSIVE if value != "tsdb"):
                run(["docker", "run", "--detach", "--pull", "never", "--network", network,
                    "--name", project+"-"+name, "--read-only", "--user", "65534:65534", "--init",
                    "--memory", "32m", "--cpus", "0.1", "--pids-limit", "32", "--restart", "unless-stopped",
                    "--label", "com.docker.compose.project="+project, "--label", "com.docker.compose.service="+name,
                    "--label", "com.docker.compose.oneoff=False", client_image, "sh", "-c",
                    "trap 'exit 0' TERM; while :; do sleep 1 & wait $!; done"], env=env)
            child_code = "\n".join([
                "import os,signal,sys",
                "from pathlib import Path",
                "from scripts.automation import storage_handoff_pause as pause",
                "actual=pause._docker",
                "def die_after_create(*args,**kwargs):",
                "    result=actual(*args,**kwargs)",
                "    if args[0]=='compose' and 'create' in args:",
                "        print('CREATED_BEFORE_PROCESS_DEATH',flush=True)",
                "        os.kill(os.getpid(),signal.SIGKILL)",
                "    return result",
                "pause._docker=die_after_create",
                "with pause.paused_storage_clients(Path(sys.argv[1]),project=sys.argv[2],source_revision='a'*40,prepare_database=True,history_uuid='fixture-history'):",
                "    raise AssertionError('interruption was not injected')",
            ])
            print("Interrupting the owning host controller after database creation", flush=True)
            child = run([sys.executable, "-c", child_code, str(state), project], env=env, timeout=500, ok=False)
            if child.returncode != -9 or "CREATED_BEFORE_PROCESS_DEATH" not in child.stdout:
                raise RuntimeError("disposable_host_interruption_failed: "+child.stderr[-6000:])
            held = json.loads((state/pause.HOLD).read_text())
            assert held["phase"] == "preparing_database" and held["database_preparation"]["source_stopped"]
            replacement = cid(); assert replacement != original
            assert run(["docker", "inspect", "--format", "{{.State.Status}}", replacement], env=env).stdout.strip() == "created"
            os.environ["QT_STORAGE_UDEV_ROOT"] = str(udev)
            with pause.paused_storage_clients(state, project=project, source_revision=revision,
                                               prepare_database=True, history_uuid="fixture-history") as receipt:
                assert receipt["phase"] == "database_prepared" and cid() == replacement
                assert receipt["database_preparation"]["cluster_identifier"] == cluster
                assert query("SELECT value FROM public.qt_host_hold_proof") == "retained"
                rows = pause._inventory(project)
                assert not any(rows[name]["running"] for name in pause.STOP)
                assert all(rows[name]["running"] for name in pause.PASSIVE)
                assert run(["docker", "exec", replacement, "cat", "/qt-history/preparation-proof"], env=env).stdout.strip() == "retained-history-path"
            with pause.paused_storage_clients(state, project=project, source_revision=revision,
                                               prepare_database=True, history_uuid="fixture-history"):
                assert cid() == replacement
            for action in ("deploy", "recover", "rollback"):
                result = run(["bash", "scripts/automation/server_deploy.sh", action],
                    env={**env, "QT_SINGLE_NODE_STATE_ROOT": str(state), "QT_SINGLE_NODE_ENV_FILE": str(root/"absent.env")}, ok=False)
                assert result.returncode and "storage handoff hold" in result.stderr
            assert (state/pause.HOLD).exists()
            print("PASS: durable database replacement intent survives owning-process death; exact replacement resumes with original cluster/data and history bind; clients stay paused and ordinary deployment stays blocked", flush=True)
        except BaseException:
            target = cid()
            if target:
                if "original_details" in locals():
                    current = pause._database_details(target)
                    # Diagnostic keys only: never print the resolved environment.
                    changes = {group: [key for key in set(original_details[group]) | set(current[group])
                               if original_details[group].get(key) != current[group].get(key)]
                               for group in ("config", "host")}
                    print("Fixture changed Docker fields: "+json.dumps(changes), flush=True)
                    print("Fixture networks: "+json.dumps({"source": pause._database_networks(original_details),
                                                           "replacement": pause._database_networks(current)}), flush=True)
                    print("Fixture mounts: "+json.dumps({"source": original_details["mounts"], "replacement": current["mounts"]}), flush=True)
                print(run(["docker", "logs", "--tail", "35", target], env=env, ok=False).stdout[-7000:], flush=True)
            raise
        finally:
            if old_udev is None:
                os.environ.pop("QT_STORAGE_UDEV_ROOT", None)
            else:
                os.environ["QT_STORAGE_UDEV_ROOT"] = old_udev
            ids = run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project="+project], env=env).stdout.split()
            if ids:
                run(["docker", "rm", "--force", *ids], env=env)
            if created_volume:
                run(["docker", "volume", "rm", volume], env=env)
            if created_network:
                run(["docker", "network", "rm", network], env=env)
            print("Owned disposable host-hold resources removed.", flush=True)




_SEED = r"""
import contextlib,hashlib,json,os
from dataclasses import replace,asdict,is_dataclass
from datetime import timedelta
from pathlib import Path
os.environ['MARKET_STRUCTURE_STORAGE_ROOT']='/app/logs/market-structure'
os.environ['QT_MARKET_DATA_EXPECTED_UUID']='fixture-ssd'
import pytest
from sqlalchemy import text
from market_data.contracts import DatasetSeriesRequest
from tests.test_market_data import test_fact_storage_tiers_db as tiers
from tests.test_market_data.tiered_v1_fixture import restore_tiered_v1_fixture
root=Path('/app/logs/market-structure')
with pytest.MonkeyPatch.context() as mp:
    mp.setattr(tiers,'fresh_migration_database',lambda label:contextlib.nullcontext(os.environ['PG_DSN']))
    fixture=tiers.storage.__wrapped__(mp)
    storage=next(fixture)
    current=storage.today
    storage.today=current-timedelta(days=31)
    tiers._placement(mp,storage.today)
    facts=[replace(storage.fact,observation_key='held-'+str(i),observation_time=tiers.BASE+timedelta(seconds=i)) for i in range(3)]
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=facts)
    request=DatasetSeriesRequest(storage.series_id,tiers.BASE-timedelta(hours=1),tiers.BASE+timedelta(hours=1))
    frozen=storage.repo.freeze_dataset([request])
    expected=storage.repo.read_dataset_fact_revisions(dataset_id=frozen.dataset_id,series_id=storage.series_id)
    with storage.database.session() as session:
        reclaimed_bytes=session.scalar(text("SELECT pg_total_relation_size(to_regclass(:name))"),
            {'name':'market.'+tiers.fact_partition_name(storage.today)})
    tiers._verified_cold_fixture(storage,root,mp)
    # The reader-only helper drops the hot partition. This full runtime seed
    # must also represent its completed reclamation for the retention planner.
    with storage.database.session() as session:
        session.execute(text("UPDATE market.fact_retention_partitions SET state='reclaimed', reclaimed_at=now(), reclaimed_bytes=:bytes WHERE storage_day=:day"), {'day':storage.today,'bytes':reclaimed_bytes})
    storage.open_day=current
    tiers._placement(mp,current)
    recent=replace(storage.fact,observation_key='held-recent',observation_time=tiers.BASE+timedelta(days=2))
    storage.repo.ingest_facts(series_id=storage.series_id,source_id=storage.source_id,facts=[recent])
    restore_tiered_v1_fixture(storage)
    proof=dict(dataset_id=frozen.dataset_id,series_id=storage.series_id,
        frozen_sha256=hashlib.sha256(json.dumps(expected,sort_keys=True,default=lambda v:asdict(v) if is_dataclass(v) else str(v)).encode()).hexdigest(),
        history_before=(current-timedelta(days=30)).isoformat(),old_day=storage.today.isoformat(),recent_day=current.isoformat())
    (root/'held-proof.json').write_text(json.dumps(proof))
    fixture.close()
print('owned_v1_data_seeded')
"""

_VERIFY = r"""
import hashlib,json,os
from dataclasses import asdict,is_dataclass
from pathlib import Path
from sqlalchemy import text
from portal.backend.db.session import Database
from portal.backend.service.storage.repos import market_data
proof=json.loads(Path('/app/logs/market-structure/held-proof.json').read_text())
database=Database(os.environ['PG_DSN'])
assert database.ensure_schema(),str(database.last_error)
market_data.db=database
repo=market_data.PostgresMarketDataRepository()
rows=repo.read_dataset_fact_revisions(dataset_id=proof['dataset_id'],series_id=proof['series_id'])
assert hashlib.sha256(json.dumps(rows,sort_keys=True,default=lambda v:asdict(v) if is_dataclass(v) else str(v)).encode()).hexdigest()==proof['frozen_sha256']
with database.session() as session:
    def disk(relation):
        return session.scalar(text('SELECT COALESCE(NULLIF(reltablespace,0),1663) FROM pg_class WHERE oid=to_regclass(:name)'),{'name':relation})
    assert disk('market.fact_identities')!=1663
    assert disk('market.fact_versions_'+proof['old_day'].replace('-',''))!=1663
    assert disk('market.fact_versions_'+proof['recent_day'].replace('-',''))==1663
    assert session.scalar(text('SELECT count(*) FROM market.fact_versions'))==4
    assert session.scalar(text("SELECT count(*) FROM market.fact_versions WHERE observation_key='held-recent'"))==1
print('frozen_history_and_recent_placement_preserved')
"""


def operator_rehearsal_inner(project, image, source_root, history_root, control_root, *runtime_images):
    """Run the real host code in the daemon's owned-volume filesystem namespace."""
    from datetime import date
    from urllib.parse import quote
    source_root,history_root,control_root=map(Path,(source_root,history_root,control_root))
    assert history_root==Path('/dev/shm')/(project+'-history')
    env={key:value for key,value in os.environ.items() if not key.startswith(('QT_','PG_','POSTGRES_','COMPOSE_'))}
    state=control_root/'state';state.mkdir(mode=0o700)
    # PGDATA is a child only to share this disposable SSD with the legacy
    # working bind. Match the real runtime-owned PostgreSQL volume root.
    os.chown(source_root,70,70)
    working=source_root/'working';working.mkdir(mode=0o700);os.chown(working,70,70)
    (history_root/'archives'/'objects').mkdir(parents=True)
    for path in (history_root,history_root/'archives',history_root/'archives'/'objects'):
        os.chown(path,70,70);path.chmod(0o700)
    udev=control_root/'udev';udev.mkdir()
    if runtime_images:
        assert len(runtime_images)==2
        udev=udev/'data';udev.mkdir()
    for path,uuid_value in ((source_root,'fixture-ssd'),(history_root,'fixture-hdd')):
        device=path.stat().st_dev
        (udev/f'b{os.major(device)}:{os.minor(device)}').write_text('E:ID_FS_UUID='+uuid_value+'\n')
    assert source_root.stat().st_dev!=history_root.stat().st_dev
    os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
    inventory=control_root/'inventory.json'
    inventory.write_text(json.dumps(dict(schema_version='qt.storage_inventory.v1',targets=[
        dict(target_id='ssd',label='Recent',filesystem_uuid='fixture-ssd',root='/var/lib/postgresql/data',medium='ssd'),
        dict(target_id='hdd',label='History',filesystem_uuid='fixture-hdd',root='/qt-history',medium='hdd')])))
    pg_image=run(['docker','image','inspect','quanttrad-postgres:2.14.2-pg15','--format','{{.Id}}'],env=env).stdout.strip()
    client_image=run(['docker','image','inspect','python:3.12.3-slim','--format','{{.Id}}'],env=env).stdout.strip()
    pg_database='qt_migration_held_'+uuid.uuid4().hex[:12]
    secret=uuid.uuid4().hex
    revision='a'*40
    (state/'release.env').write_text('current_revision='+revision+'\n')
    network=project+'_quanttrad'
    service=dict(image=pg_image,pull_policy='never',hostname='tsdb.quanttrad',init=True,restart='no',shm_size=1073741824,
        command=['postgres','-c','shared_buffers=64MB','-c','max_connections=30'],
        environment=dict(POSTGRES_DB=pg_database,POSTGRES_USER='quanttrad',POSTGRES_PASSWORD=secret,PGDATA='/var/lib/postgresql/data/pgdata'),
        healthcheck=dict(test=pause._TCP_PROBE,interval='1s',timeout='2s',retries=120,start_period='10s'),
        volumes=[dict(type='volume',source='postgres-data',target='/var/lib/postgresql/data')],
        networks={'quanttrad':dict(aliases=['tsdb.quanttrad'])})
    model=dict(name=project,services={'tsdb':service},volumes={'postgres-data':dict(name=project+'-source',external=True)},
        networks={'quanttrad':dict(name=network,external=True)})
    source=control_root/'source.json';source.write_text(json.dumps(model));source.chmod(0o600)
    recipe=json.loads(json.dumps(model));recipe['services']['tsdb']['volumes'].append(
        dict(type='bind',source=str(history_root),target='/qt-history',bind=dict(create_host_path=False)))
    if runtime_images:
        recovery_secrets=control_root/'recovery-secrets'
        recovery_secrets.mkdir(mode=0o700);os.chown(recovery_secrets,70,70)
        socket_name=project+'-recovery-socket'
        recipe['volumes']['storage-recovery-socket']=dict(name=socket_name,external=True)
        recipe['services']['tsdb']['volumes'] += [
            dict(type='bind',source=str(recovery_secrets),target='/run/quanttrad/recovery',
                 read_only=True,bind=dict(create_host_path=False)),
            dict(type='volume',source='storage-recovery-socket',target='/var/run/postgresql')]
    recipe_path=state/pause.DATABASE_RECIPE;recipe_path.write_text(json.dumps(recipe));recipe_path.chmod(0o600)
    run(['docker','network','create','--internal',network],env=env)
    compose=['docker','compose','--project-name',project,'--file',str(source)]
    run(compose+['up','--detach','--no-build','--pull','never','--wait','--wait-timeout','180'],env=env)
    dbid=run(compose+['ps','--quiet','tsdb'],env=env).stdout.strip()
    dsn='postgresql+psycopg2://quanttrad:'+secret+'@127.0.0.1:5432/'+pg_database
    def application(code, container, *, user="70:70"):
        return run(['docker','run','--rm','--pull','never','--name',project+'-fixture-app','--user',user,
            '--network','container:'+container,'--pid','container:'+container,'--volumes-from',container,
            '--mount','type=bind,source='+str(working)+',target=/app/logs/market-structure',
            '--mount','type=bind,source='+str(udev)+',target=/run/qt-handoff/udev,readonly',
            '--env','PG_DSN','--env','QT_DISABLE_DOTENV=1','--env','QT_DB_TEST_ISOLATED=1',
            '--env','MARKET_STRUCTURE_STORAGE_ROOT=/qt-history/archives',
            '--env','QT_MARKET_DATA_EXPECTED_UUID=fixture-hdd','--env','QT_STORAGE_UDEV_ROOT=/run/qt-handoff/udev',
            '--entrypoint','python',image,'-c',code],env={**env,'PG_DSN':dsn},timeout=600)
    print('Seeding real collected/frozen recent and archived records in owned database',flush=True)
    print(application(_SEED,dbid,user="0:0").stdout[-300:],flush=True)
    import hashlib,stat
    legacy_files={str(path.relative_to(working)):(path.stat(),hashlib.sha256(path.read_bytes()).hexdigest())
                  for path in working.rglob('*') if path.is_file()}
    assert any(info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 for info,_ in legacy_files.values())
    private=control_root/'candidate.env'
    if runtime_images:
        setup_env={**env,'QT_SINGLE_NODE_ENV_FILE':str(private),'QT_SINGLE_NODE_STATE_ROOT':str(state)}
        run(['bash','scripts/automation/server_deploy.sh','init-env'],env=setup_env)
        values=dict(line.split('=',1) for line in private.read_text().splitlines())
        values['PG_DSN']=dsn.replace('@127.0.0.1:','@tsdb:')
        values['POSTGRES_DB']=pg_database
        values['POSTGRES_PASSWORD']=secret
        for flag in ('BOOTSTRAP_MARKET_DATA','ENABLE_SCHEDULED_FACTS','ENABLE_STRUCTURED_FACTS','ENABLE_TRADE_STREAMS','ENABLE_L2_STREAMS'):
            values['QT_SINGLE_NODE_'+flag]='false'
        private.write_text(''.join(key+'='+value+'\n' for key,value in values.items()))
    # Start old clients through Compose: manually labelled docker-run clients
    # survive alongside replacements instead of following Compose ownership.
    client_names=pause.STOP+tuple(v for v in pause.PASSIVE if v!='tsdb')
    clients=json.loads(json.dumps(model))
    for name in client_names:
        client=dict(image=client_image,pull_policy='never',read_only=True,user='65534:65534',
            init=True,restart='unless-stopped',entrypoint=['sh'],
            command=['-c',"trap 'exit 0' TERM; while :; do sleep 1 & wait $!; done"],
            networks={'quanttrad':{}})
        mounts=[]
        if name=='market-data-collector':
            mounts.append(dict(type='bind',source=str(working),target='/app/logs/market-structure'))
            client['environment']={'PG_DSN':dsn.replace('@127.0.0.1:','@tsdb:')}
        if runtime_images and name in pause._RUNTIME_WRITERS:
            mounts.append(dict(type='bind',source=str(private),target='/app/secrets.env',read_only=True))
        if mounts:client['volumes']=mounts
        clients['services'][name]=client
    client_recipe=control_root/'source-clients.json'
    client_recipe.write_text(json.dumps(clients));client_recipe.chmod(0o600)
    run(['docker','compose','--project-name',project,'--file',str(client_recipe),
        'up','--detach','--no-deps','--no-build','--pull','never',*client_names],env=env)
    image_env=dict(v.split('=',1) for v in json.loads(run(['docker','image','inspect',image,'--format','{{json .Config.Env}}'],env=env).stdout))
    proof=json.loads((working/'held-proof.json').read_text())
    identity=pause._database_query(dbid,"SELECT c.system_identifier::text||'/'||d.oid::text FROM pg_control_system() c CROSS JOIN pg_database d WHERE d.datname=current_database()")
    request=dict(schema_version='qt.storage_database_operator.v1',source_revision=image_env['QT_IMAGE_SOURCE_REVISION'],
        source_tree_hash=image_env['QT_IMAGE_SOURCE_TREE_HASH'],database_identity=identity,inventory_path='/run/quanttrad/storage-inventory.json',
        policy=dict(recent=['ssd'],history=['hdd'],archives=['hdd'],backups=['hdd'],movement_enabled=True,backup_enabled=True),
        resource_limits=dict(wal_bytes=16*1024**2,temporary_bytes={'ssd':1024**2,'hdd':0},growth_bytes_per_second={'ssd':0,'hdd':0},
            maintenance_bytes={'ssd':1024**2,'hdd':1024**2},movement_timeout_seconds=60,cancellation_grace_seconds=5),
        history_before=proof['history_before'],source_root='/app/logs/market-structure/objects',destination_root='/qt-history/archives/objects',
        max_page_bytes=32*1024**2,max_objects=100,max_bytes=32*1024**2,page_rows=2,max_duration_seconds=1800 if runtime_images else 600)
    options=dict(project=project,source_revision=revision,history_uuid='fixture-hdd',image=image,request=request,inventory_path=str(inventory))
    input_file=control_root/'invocation.json';input_file.write_text(json.dumps(options));input_file.chmod(0o600)
    child_code="\n".join([
        'import json,os,signal,sys','from pathlib import Path','from scripts.automation import storage_handoff_pause as pause',
        'actual=pause._docker','def interrupted(*args,**kwargs):','    result=actual(*args,**kwargs)',
        "    if args[0]=='create':",'        os.kill(os.getpid(),signal.SIGKILL)','    return result',
        'pause._docker=interrupted','pause.run_held_database_handoff(Path(sys.argv[1]),**json.loads(Path(sys.argv[2]).read_text()))'])
    print('Interrupting host controller after creating its actual migration container',flush=True)
    child=run([sys.executable,'-c',child_code,str(state),str(input_file)],env={**env,'QT_STORAGE_UDEV_ROOT':str(udev)},timeout=800,ok=False)
    if child.returncode!=-9:
        held=pause._load(state/pause.HOLD)
        current=run(['docker','ps','-aq','--no-trunc','--filter','label=com.docker.compose.project='+project,
                     '--filter','label=com.docker.compose.service=tsdb'],env=env).stdout.strip()
        if current:
            observed=pause._database_details(current)
            print('Prepared database mount/network evidence: '+json.dumps(dict(
                mounts=observed['mounts'],networks=pause._database_networks(observed),
                expected_networks=held.get('database_preparation',{}).get('networks'),
                expected_history=held.get('database_preparation',{}).get('history_root'))),flush=True)
    assert child.returncode==-9,child.stderr[-4000:]
    operator_id=run(['docker','ps','-aq','--no-trunc','--filter','name=^/'+project+'-storage-handoff$'],env=env).stdout.strip()
    try:
        result=pause.run_held_database_handoff(state,**options)
    except Exception:
        # This owned fixture uses only synthetic connection settings; the
        # packaged worker emits bounded, sanitized guard outcomes.
        diagnostic=run(['docker','logs','--tail','12',operator_id],env=env,ok=False)
        print('Owned migration worker outcome: '+diagnostic.stdout[-4000:]+diagnostic.stderr[-4000:],flush=True)
        raise
    assert result['database_sequence_complete'] and not result['collection_resume_authorized']
    assert {str(path.relative_to(working)) for path in working.rglob('*') if path.is_file()}==set(legacy_files)
    for key,(old,digest) in legacy_files.items():
        path=working/key;now=path.stat()
        assert now.st_uid==70 and hashlib.sha256(path.read_bytes()).hexdigest()==digest
        assert (now.st_dev,now.st_ino,now.st_mode,now.st_gid,now.st_mtime_ns)==(old.st_dev,old.st_ino,old.st_mode,old.st_gid,old.st_mtime_ns)
    print('Legacy root-owned private files now readable by UID70 with bytes and file identity preserved',flush=True)
    assert run(['docker','ps','-aq','--no-trunc','--filter','name=^/'+project+'-storage-handoff$'],env=env).stdout.strip()==operator_id
    print(application(_VERIFY,pause._load(state/pause.HOLD)['containers']['tsdb']['id']).stdout[-400:],flush=True)
    assert (state/pause.HOLD).exists()
    assert not any(pause._inventory(project,operator_id=operator_id)[name]['running'] for name in pause.STOP)
    assert pause.run_held_database_handoff(state,**options)==result
    print('PASS: held host procedure recovered the same actual operator, preserved frozen archived reads and recent records, placed old headers/identities on HDD, and kept application clients paused',flush=True)
    if runtime_images:
        _activate_fixture_runtime(state=state,project=project,image=image,runtime_images=runtime_images,
            options=options,recipe=recipe,working=working,history_root=history_root,
            private=private,udev=udev,inventory=inventory,control_root=control_root,
            env=env,client_image=client_image,application=application)



def _activate_fixture_runtime(*, state, project, image, runtime_images, options, recipe,
        working, history_root, private, udev, inventory, control_root, env, client_image, application):
    """Real API, initializer, collector and portals; auxiliary clients stay synthetic."""
    limits=control_root/'maintenance.json'
    limits.write_text(json.dumps(dict(schema_version='qt.storage_maintenance_limits.v1',
        history=options['request']['resource_limits'],
        recovery=dict(max_bytes=32*1024**2,timeout_seconds=120,
            headroom_bytes={'ssd':1024**2,'hdd':1024**2},max_objects=1000))))
    recovery_secrets=control_root/'recovery-secrets'
    recovery_secrets.mkdir(mode=0o700,exist_ok=True)
    configured={**env,'QT_SERVER_ENV_FILE':str(private),'QT_COMPOSE_PROJECT_NAME':project,
        'QT_RELEASE_REVISION':options['request']['source_revision'],
        'QT_SOURCE_TREE_HASH':options['request']['source_tree_hash'],
        'QT_STORAGE_HDD_ROOT':str(history_root),'QT_MARKET_DATA_ROOT':str(history_root/'archives'),
        'QT_MARKET_DATA_EXPECTED_UUID':'fixture-hdd','QT_MARKET_DATA_WORKING_ROOT':str(working),
        'QT_MARKET_DATA_WORKING_EXPECTED_UUID':'fixture-ssd','QT_STORAGE_INVENTORY_HOST_PATH':str(inventory),
        'QT_STORAGE_MAINTENANCE_LIMITS_HOST_PATH':str(limits),
        'QT_STORAGE_RECOVERY_SECRETS_ROOT':str(recovery_secrets),
        'QT_DOCKER_SOCKET_GID':str(Path('/var/run/docker.sock').stat().st_gid)}
    for flag in ('BOOTSTRAP_MARKET_DATA','ENABLE_SCHEDULED_FACTS','ENABLE_STRUCTURED_FACTS','ENABLE_TRADE_STREAMS','ENABLE_L2_STREAMS'):
        configured['QT_SINGLE_NODE_'+flag]='false'
    rendered=json.loads(run(['docker','compose','--env-file',str(private),
        '--file','docker/docker-compose.server.yml','--file','docker/docker-compose.storage-server.yml',
        'config','--format','json'],env=configured).stdout)
    model=json.loads(json.dumps(recipe))
    for name in ('backend','initialize','market-data-collector','frontend','frontend-v2'):
        service=rendered['services'][name]
        service.pop('build',None);service.pop('ports',None)
        service['image']=runtime_images[0] if name=='frontend' else runtime_images[1] if name=='frontend-v2' else image
        service['pull_policy']='never';service['restart']='no'
        for mount in service.get('volumes',[]):
            if mount['target']=='/run/qt-host-udev':mount['source']=str(udev.parent)
        model['services'][name]=service
    for name in (*pause.STOP,*pause.PASSIVE):
        if name in model['services']:continue
        model['services'][name]=dict(image=image if name=='docker-stats' else client_image,
            pull_policy='never',restart='no',user='65534:65534',
            command=['python','-c','import time; time.sleep(3600)'],
            healthcheck=dict(test=['CMD','true'],interval='1s',timeout='2s',retries=30),
            networks={'quanttrad':{}})
    path=state/pause.RUNTIME_RECIPE
    path.write_text(json.dumps(model));path.chmod(0o600)
    normalized=json.loads(run(['docker','compose','--project-name',project,'--file',str(path),
        'config','--format','json'],env=env).stdout)
    path.write_text(json.dumps(normalized))
    invocation=control_root/'runtime-invocation.json'
    invocation.write_text(json.dumps(options));invocation.chmod(0o600)
    # Kill the actual host controller after Compose changed the real services,
    # before it can record readiness or remove the durable hold.
    code="\n".join([
        'import json,os,signal,sys','from pathlib import Path','from scripts.automation import storage_handoff_pause as pause',
        'actual=pause._docker',
        'actual_run=pause.subprocess.run',
        'def diagnosed_run(command,*args,**kwargs):',
        '    result=actual_run(command,*args,**kwargs)',
        "    if command[:2]==['docker','compose'] and 'up' in command and result.returncode:",
        "        print('Owned synthetic Compose startup: '+result.stderr[-4000:],file=sys.stderr,flush=True)",
        '    return result',
        'pause.subprocess.run=diagnosed_run',
        'def interrupted(*args,**kwargs):','    result=actual(*args,**kwargs)',
        "    if args[0]=='compose' and 'up' in args:",
        '        os.kill(os.getpid(),signal.SIGKILL)','    return result','pause._docker=interrupted',
        'pause.run_held_runtime_handoff(Path(sys.argv[1]),activation_timeout_seconds=600,**json.loads(Path(sys.argv[2]).read_text()))'])
    print('Starting real candidate services and interrupting the owning activation controller',flush=True)
    child=run([sys.executable,'-c',code,str(state),str(invocation)],
        env={**env,'QT_STORAGE_UDEV_ROOT':str(udev)},timeout=800,ok=False)
    try:
        assert child.returncode==-9,child.stderr[-4000:]
        saved=pause._load(state/pause._RUNTIME_STATE)
        before=pause._identities(pause._inventory(project,operator_id=saved['operator_id'],activating=True))
        assert (state/pause.HOLD).exists() and 'storage_layout=' not in (state/'release.env').read_text()
        outcome=pause.run_held_runtime_handoff(state,activation_timeout_seconds=600,**options)
        assert outcome['ready'] and not (state/pause.HOLD).exists()
        after=pause._identities(pause._inventory(project,operator_id=saved['operator_id'],activating=True))
        assert after==before,'recovery must retain the same actual candidate containers'
        assert 'storage_layout=ssd-hdd-v1\n' in (state/'release.env').read_text()
        assert 'previous_revision=\n' in (state/'release.env').read_text()
        assert pause.run_held_runtime_handoff(state,activation_timeout_seconds=600,**options)==outcome
        print(application(_VERIFY,saved['binding']['database_id']).stdout[-400:],flush=True)
        print('PASS: real API, initializer, collector and both portals healthy; current-layout recovery copy verified; interrupted activation resumed same candidate; hold retired only after verified release; database and frozen/recent reads preserved',flush=True)
    except Exception:
        for service in ('backend','initialize','market-data-collector','frontend','frontend-v2'):
            ids=run(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project,
                '--filter','label=com.docker.compose.service='+service],env=env).stdout.split()
            if ids:
                status=run(['docker','inspect','--format',
                    '{{json .State.Status}} {{json .State.Error}} {{if .State.Health}}{{json .State.Health.Status}}{{end}}',
                    ids[0]],env=env,ok=False)
                print('Owned candidate state '+service+': '+status.stdout[-1000:],flush=True)
                detail=run(['docker','logs','--tail','20',ids[0]],env=env,ok=False)
                print('Owned candidate '+service+': '+detail.stdout[-3000:]+detail.stderr[-3000:],flush=True)
        raise


def operator_rehearsal_outer(image, *runtime_images):
    """Only owned disposable volumes are visible as host storage to this fixture."""
    env={key:value for key,value in os.environ.items() if not key.startswith(('QT_','PG_','POSTGRES_','COMPOSE_'))}
    project='qt-held-operator-'+uuid.uuid4().hex[:12]
    print('Owned disposable project: '+project,flush=True)
    image=run(['docker','image','inspect',image,'--format','{{.Id}}'],env=env).stdout.strip()
    runtime_images=tuple(run(['docker','image','inspect',value,'--format','{{.Id}}'],env=env).stdout.strip() for value in runtime_images)
    plugins=json.loads(run(['docker','info','--format','{{json .ClientInfo.Plugins}}'],env=env).stdout)
    plugin=next(value['Path'] for value in plugins if value['Name']=='compose')
    # Desktop's raw socket preserves daemon-volume paths; native CI uses its
    # ordinary local socket. No remote daemon or host discovery is supported.
    daemon_socket=Path('/var/run/docker.sock.raw')
    if not daemon_socket.exists():
        daemon_socket=Path('/var/run/docker.sock')
    if not daemon_socket.is_socket():
        raise RuntimeError('owned_operator_local_docker_socket_required')
    created=[]
    controller=project+'-controller'
    history='/dev/shm/'+project+'-history'
    history_created=False
    try:
        roots=[]
        for name in ('source','control'):
            volume=project+'-'+name
            args=['docker','volume','create']
            run(args+[volume],env=env);created.append(volume)
            roots.append(run(['docker','volume','inspect',volume,'--format','{{.Mountpoint}}'],env=env).stdout.strip())
        # The daemon's existing shared-memory filesystem is outside its data
        # root, so ordinary private propagation is preserved. Only this unique
        # owned directory is touched; no host mount operation is performed.
        prepare_code="import os,sys;from pathlib import Path;p=Path(sys.argv[1]);p.mkdir(mode=0o700);(p/'.qt-rehearsal-owner').write_text(sys.argv[2]);os.chown(p,70,70)"
        run(['docker','run','--rm','--pull','never','--network','none','--ipc','host',
             '--entrypoint','python',image,'-c',prepare_code,history,project],env=env)
        history_created=True
        args=['docker','run','--rm','--pull','never','--name',controller,'--network','none','--ipc','host','--user','0:0',
            '--mount','type=bind,source='+str(ROOT)+',target=/qt-host,readonly',
            # Docker Desktop's raw socket preserves the daemon-volume paths below.
            '--mount','type=bind,source='+str(daemon_socket)+',target=/var/run/docker.sock',
            '--mount','type=bind,source='+plugin+',target=/usr/local/lib/docker/cli-plugins/docker-compose,readonly',
            '--entrypoint','python','--workdir','/qt-host']
        for volume,root in zip(created,roots):
            args+=['--mount','type=volume,source='+volume+',target='+root]
        if runtime_images:
            socket_name=project+'-recovery-socket'
            run(['docker','volume','create',socket_name],env=env);created.append(socket_name)
        result=run(args+[image,'scripts/ci/test_storage_database_preparation.py','--operator-inner',project,image,roots[0],history,roots[1],*runtime_images],env=env,timeout=2400,ok=False)
        print(result.stdout[-14000:],flush=True)
        if result.returncode:
            print(result.stderr[-10000:],flush=True)
            raise RuntimeError('owned_held_operator_rehearsal_failed')
    finally:
        for name in (controller,project+'-storage-handoff',project+'-fixture-app'):
            ids=run(['docker','ps','-aq','--filter','name=^/'+name+'$'],env=env).stdout.split()
            if ids:run(['docker','rm','--force',*ids],env=env)
        ids=run(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project],env=env).stdout.split()
        if ids:run(['docker','rm','--force',*ids],env=env)
        networks=run(['docker','network','ls','-q','--filter','name=^'+project+'_quanttrad$'],env=env).stdout.split()
        if networks:run(['docker','network','rm',*networks],env=env)
        if history_created:
            cleanup_code="import shutil,sys;from pathlib import Path;p=Path(sys.argv[1]);assert p.parent==Path('/dev/shm') and p.resolve(strict=True)==p and not p.is_symlink();assert (p/'.qt-rehearsal-owner').read_text()==sys.argv[2];shutil.rmtree(p)"
            run(['docker','run','--rm','--pull','never','--network','none','--ipc','host',
                 '--entrypoint','python',image,'-c',cleanup_code,history,project],env=env)
        for volume in reversed(created):run(['docker','volume','rm',volume],env=env)
        print('Owned held-operator fixture resources removed',flush=True)


if __name__ == "__main__":
    if len(sys.argv)==3 and sys.argv[1]=='--operator-image':
        operator_rehearsal_outer(sys.argv[2])
    elif len(sys.argv)==5 and sys.argv[1]=='--runtime-images':
        operator_rehearsal_outer(*sys.argv[2:])
    elif len(sys.argv) in (7,9) and sys.argv[1]=='--operator-inner':
        operator_rehearsal_inner(*sys.argv[2:])
    elif len(sys.argv)==1:
        main()
    else:
        raise SystemExit('Use --operator-image IMAGE or --runtime-images BACKEND FRONTEND FRONTEND_V2')
