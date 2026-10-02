"""Disposable runtime configuration shared by online host rehearsals."""
import json
from pathlib import Path
from scripts.automation import storage_host_boundary as host_boundary


def write_runtime_recipe(*, state, runtime_model, inventory, udev, image, password,
                         dbname, history, project, candidate_working, owned):
    from scripts.automation import storage_online_runtime as runtime_host
    from scripts.automation import storage_online_recovery as recovery_host
    mounts={m['target']:m for m in runtime_model['services']['tsdb']['volumes']}
    targets=json.loads(inventory.read_text())['targets']
    ssd=next(t for t in targets if t['medium']=='ssd');hdd=next(t for t in targets if t['medium']=='hdd')
    limits=write_maintenance_limits(state, targets)
    def fixture_mount(source,target,readonly=False):
     return dict(type='bind',source=str(source),target=target,read_only=readonly,bind=dict(create_host_path=False))
    for service_name,module in runtime_host._APPLICATIONS.items():
     maintenance=service_name=='storage-maintenance'
     environment=dict(PG_DSN='postgresql+psycopg2://fixture:'+password+'@tsdb:5432/'+dbname,
       QT_DISABLE_DOTENV='1',QT_LOGGING_LOKI_URL='',QT_STORAGE_MAINTENANCE_OWNER='dedicated',QT_ARCHIVE_SHARED_GROUP_ID='70',
       MARKET_STRUCTURE_STORAGE_ROOT='/qt-history/archives',MARKET_STRUCTURE_WORKING_ROOT='/qt-history/archives' if maintenance else '/app/logs/market-structure',
       QT_MARKET_DATA_EXPECTED_UUID=hdd['filesystem_uuid'],QT_MARKET_DATA_WORKING_EXPECTED_UUID=(hdd if maintenance else ssd)['filesystem_uuid'],
       QT_STORAGE_INVENTORY_PATH='/run/quanttrad/storage-inventory.json',QT_STORAGE_UDEV_ROOT='/run/qt-host-udev/data',
       QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA='false',QT_SINGLE_NODE_ENABLE_SCHEDULED_FACTS='false',QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS='false',
       QT_SINGLE_NODE_ENABLE_TRADE_STREAMS='false',QT_SINGLE_NODE_ENABLE_L2_STREAMS='false')
     service_mounts=[mounts['/qt-history'],fixture_mount(inventory,'/run/quanttrad/storage-inventory.json',True),fixture_mount(udev,'/run/qt-host-udev/data',True)]
     service=dict(image=image,pull_policy='never',user='70:70' if maintenance else '1000:1000',group_add=['70'],init=True,
       restart='no',cap_drop=['ALL'],security_opt=['no-new-privileges:true'],command=['python','-m',module],
       networks={'quanttrad':{}},environment=environment,volumes=service_mounts,mem_limit=(2 if service_name=='backend' else 1)*1024**3,memswap_limit=(2 if service_name=='backend' else 1)*1024**3,cpus=2,pids_limit=256,
       labels={'qt.disposable':project})
     if maintenance:
      service['pid']='service:tsdb'
      service_mounts.extend(mounts[k] for k in ('/var/lib/postgresql/data','/run/quanttrad/recovery','/var/run/postgresql'))
      service_mounts.append(fixture_mount(limits,'/run/quanttrad/storage-maintenance.json',True))
      environment.update(QT_MARKET_DATA_LIFECYCLE_ENABLED='true',QT_MARKET_DATA_LIFECYCLE_EXECUTION_ENABLED='true',
        QT_MARKET_DATA_LIFECYCLE_CANONICAL_EXECUTION_ENABLED='true',QT_STORAGE_MAINTENANCE_LIMITS_PATH='/run/quanttrad/storage-maintenance.json')
     else:
      service_mounts.append(fixture_mount(candidate_working,'/app/logs/market-structure'))
      if service_name=='backend':
       service_mounts.append(mounts['/var/lib/postgresql/data']);environment['QT_MARKET_DATA_ROOT']=str(history/'archives')
     if service_name!='initialize':
      probe=runtime_host._APPLICATION_HEALTH[service_name]
      service['healthcheck']=dict(test=probe,interval='1s',timeout='3s',retries=15,start_period='2s')
     runtime_model['services'][service_name]=service
     owned.append(project+'-'+service_name+'-1')
    host_boundary.save_receipt(state/runtime_host.RUNTIME_RECIPE,runtime_model,initial=True)


def write_maintenance_limits(state, targets):
    limits=state/'runtime-maintenance.json'
    limits.write_text(json.dumps(dict(schema_version='qt.storage_maintenance_limits.v2',
      history=dict(wal_bytes=16*1024**2,temporary_bytes={t['target_id']:1024**2 for t in targets},
        growth_bytes_per_second={t['target_id']:0 for t in targets},maintenance_bytes={t['target_id']:1024**2 for t in targets},
        movement_timeout_seconds=60,cancellation_grace_seconds=5),
      recovery=dict(max_bytes=256*1024**2,timeout_seconds=60,headroom_bytes={t['target_id']:1024**2 for t in targets},max_objects=128,
        incremental=dict(pgbackrest='/usr/local/bin/pgbackrest',restic='/usr/local/bin/restic',pg_path='/var/lib/postgresql/data',
          pg_socket_path='/var/run/postgresql',database_key_path='/run/quanttrad/recovery/database.key',
          archive_key_path='/run/quanttrad/recovery/archive.key',max_chain_backups=4)))))
    limits.chmod(0o644)
    return limits


def filesystem_uuid(root):
    """Observe the real read-only host identity for a canonical disposable mount."""
    import os
    device = Path(root).stat().st_dev
    metadata = Path('/run/udev/data') / f'b{os.major(device)}:{os.minor(device)}'
    values = [line.split('=', 1)[1] for line in metadata.read_text().splitlines()
              if line.startswith('E:ID_FS_UUID=')]
    if len(values) != 1 or not values[0]:
        raise RuntimeError('canonical_fixture_filesystem_identity_missing')
    return values[0]


def prepare_canonical_configuration(*, repository, state, project, image, database_image,
        revision, source_hash, password, dbname, history, candidate_working,
        volume, network, recovery_keys, recovery_socket):
    """Render the actual public recipe before starting the owned source database.

    Credentials are disposable and stay in the private fixture directory. The
    candidate runtime changes only immutable image/build and the admitted udev
    mount spelling. No service setting or admission guard is relaxed.
    """
    import os
    import socket
    import base64
    import subprocess
    from copy import deepcopy
    repository = Path(repository).resolve(strict=True)
    assert subprocess.check_output(['git', '-C', str(repository), 'rev-parse', 'HEAD'], text=True).strip() == revision
    assert not subprocess.check_output(['git', '-C', str(repository), 'status', '--porcelain'], text=True)
    ssd_uuid, hdd_uuid = filesystem_uuid(state), filesystem_uuid(history)
    limits = write_maintenance_limits(state, [{'target_id': 'ssd'}, {'target_id': 'hdd'}])
    values = dict(POSTGRES_USER='fixture', POSTGRES_PASSWORD=password, POSTGRES_DB=dbname,
        PGDATA='/var/lib/postgresql/data', QT_COMPOSE_PROJECT_NAME=project,
        QT_SINGLE_NODE_STATE_ROOT=str(state), QT_STORAGE_HDD_ROOT=str(history),
        QT_MARKET_DATA_ROOT=str(history/'archives'), QT_MARKET_DATA_WORKING_ROOT=str(candidate_working),
        QT_MARKET_DATA_EXPECTED_UUID=hdd_uuid, QT_MARKET_DATA_WORKING_EXPECTED_UUID=ssd_uuid,
        QT_STORAGE_INVENTORY_HOST_PATH=str(state/'inventory.json'),
        QT_STORAGE_RECOVERY_SECRETS_ROOT=str(recovery_keys),
        QT_STORAGE_MAINTENANCE_LIMITS_HOST_PATH=str(limits), QT_ARCHIVE_SHARED_GROUP_ID='70',
        QT_DOCKER_SOCKET_GID=str(Path('/var/run/docker.sock').stat().st_gid),
        QT_STORAGE_DATABASE_IMAGE=database_image, QT_STORAGE_POSTGRES_VOLUME=volume,
        QT_STORAGE_RECOVERY_SOCKET_VOLUME=recovery_socket, QT_STORAGE_NETWORK=network,
        QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA='false', QT_SINGLE_NODE_ENABLE_SCHEDULED_FACTS='false',
        QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS='false', QT_SINGLE_NODE_ENABLE_TRADE_STREAMS='false',
        QT_SINGLE_NODE_ENABLE_L2_STREAMS='false', QT_ALERTS_ENABLED='false',
        PGADMIN_DEFAULT_EMAIL='storage-fixture@quanttrad.dev', PGADMIN_DEFAULT_PASSWORD=password,
        GF_SECURITY_ADMIN_USER='fixture', GF_SECURITY_ADMIN_PASSWORD=password,
        QT_SECURITY_PROVIDER_CREDENTIAL_KEY=base64.urlsafe_b64encode(os.urandom(32)).decode())
    # Reserve distinct loopback ports during rendering; the owned recipe never
    # uses production listeners. Binding conflicts still fail Docker startup.
    sockets = []
    try:
        # The backend also consumes QT_BACKEND_PORT as its internal listener.
        # Preserve the canonical 8000 health/peer contract and isolate its host
        # listener on a distinct loopback address, rather than changing that port.
        backend_address='127.253.'+str(os.urandom(1)[0])+'.'+str(1+os.urandom(1)[0]%254)
        listener=socket.socket(); listener.bind((backend_address,8000)); sockets.append(listener)
        values['QT_BACKEND_BIND_ADDRESS']=backend_address
        values['QT_BACKEND_PORT']='8000'
        for key in ('QT_FRONTEND_PORT','QT_FRONTEND_V2_PORT',
                    'QT_PGADMIN_PORT','QT_GRAFANA_PORT','QT_ALLOY_PORT'):
            listener = socket.socket(); listener.bind(('127.0.0.1', 0)); sockets.append(listener)
            values[key] = str(listener.getsockname()[1])
        environment = state/'fixture.env'
        environment.write_text(''.join(key+'='+value+'\n' for key,value in sorted(values.items())))
        environment.chmod(0o600)
        env = {**os.environ, 'QT_SERVER_ENV_FILE': str(environment),
               'QT_RELEASE_REVISION': revision, 'QT_SOURCE_TREE_HASH': source_hash}
        rendered = subprocess.run(['docker','compose','--env-file',str(environment),
            '-f',str(repository/'docker/docker-compose.server.yml'),
            '-f',str(repository/'docker/docker-compose.storage-server.yml'),
            'config','--format','json'], env=env, text=True, capture_output=True, timeout=60)
        if rendered.returncode:
            raise RuntimeError('canonical_fixture_render_failed')
        public = json.loads(rendered.stdout)
    finally:
        for listener in sockets: listener.close()
    from scripts.automation import storage_online_runtime as runtime
    model = dict(name=project,
        services={name:deepcopy(public['services'][name]) for name in ('tsdb', *runtime._APPLICATIONS)},
        volumes={name:public['volumes'][name] for name in ('postgres-data','storage-recovery-socket')},
        networks={'quanttrad': public['networks']['quanttrad']})
    for name, service in model['services'].items():
        if name != 'tsdb':
            service['image'] = image
            service.pop('build', None)
            for mount in service['volumes']:
                if mount['target'] == '/run/qt-host-udev':
                    assert mount['source']=='/run/udev' and mount['read_only'] is True
                    mount.update(source='/run/udev/data', target='/run/qt-host-udev/data')
    database = deepcopy(model)
    database['services'] = {'tsdb': database['services']['tsdb']}
    database['volumes'].pop('storage-recovery-socket')
    database['services']['tsdb']['volumes'] = [mount for mount in database['services']['tsdb']['volumes']
        if mount['target']=='/var/lib/postgresql/data']
    return dict(environment=environment, runtime=model, database=database,
                history_uuid=hdd_uuid, recent_uuid=ssd_uuid)


def rehearse_package_amendment(*, state, kwargs, replacement_image, history_uuid,
                              control, source, owned):
    """Real Docker/SQL replacement before the first header, with synthetic peers.

    Production runtime configuration admission is deliberately not claimed by
    this fixture. Actual launch contracts, SQL observations, durable publication,
    stopped-worker preservation and resumed capture remain real.
    """
    from copy import deepcopy
    import time
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_prepare as initial
    from scripts.automation import storage_online_deadline as amendment
    from scripts.automation import storage_online_operation as operation

    with launch.launched_online_worker(state, **kwargs) as (worker, receipt):
        channel = host_boundary.OnlineWorkerChannel(worker,
            deadline=time.monotonic()+receipt["deadline"]-time.time())
        (control/"publish").write_text("publish")
        channel.exchange("prepare_step", step="catalog_history",
            relation="qt_fact_storage_cutover_v1.fact_versions", max_duration_seconds=30)
        for _ in range(64):
            page = channel.exchange("sql_copy")["result"]
            result = page["outcome"]
            if result == "raw_relocation_required":
                channel.exchange("prepare_step", step="raw_history", relation=None, max_duration_seconds=30)
            elif result == "identity_relocation_required":
                channel.exchange("prepare_step", step="identity_history", relation=None, max_duration_seconds=30)
                break
            elif page["phase"] not in {"raw_baseline", "identity_baseline"}:
                raise AssertionError("package fixture advanced beyond the unstarted header")
        else:
            raise AssertionError("tiny package baseline did not converge")
        channel.exchange("close")
    assert worker.returncode == 0
    old_worker = host_boundary.load_receipt(state/launch._STATE)
    old_id = old_worker["container_id"]; owned.append(old_id)
    request = deepcopy(kwargs["request"]); capture_plan = request.pop("capture_preparation")
    keys = state/"package-fixture-keys"; keys.mkdir(mode=0o700)
    spool = state/"package-fixture-spool"; spool.mkdir(mode=0o700)
    limits = operation.OperationLimits(preparation_seconds=30, final_seconds=30,
        recovery_seconds=30, runtime_seconds=30, spool_max_bytes=1024**2,
        spool_max_entries=128, spool_reserve_bytes=1024**2,
        repository_max_bytes=256*1024**2, repository_reserve_bytes=1024**2, recent_free_bytes=1024**2)
    plan = dict(schema_version="qt.storage_online_operation.v1", state_root=str(state),
        **kwargs, source_image=kwargs["image"], history_uuid=history_uuid,
        history_before=capture_plan["history_before"], attempt_seconds=old_worker["capture"]["seconds"],
        limits=vars(limits), keys_root=str(keys), socket_volume=kwargs["project"]+"-unused-socket",
        spool_destination=str(spool))
    plan["request"] = request; plan["inventory_path"] = str(kwargs["inventory_path"])
    path = state/"package-operation.json"; host_boundary.save_receipt(path, plan, initial=True)
    candidate = host_boundary.docker("image","inspect",replacement_image,"--format","{{.Id}}").strip()
    config = json.loads(host_boundary.docker("image","inspect",candidate,"--format","{{json .Config.Env}}"))
    environment = dict(value.split("=",1) for value in config)
    manifest = dict(schema_version="qt.storage_online_package.v1",
        plan_sha256=amendment._sha(path.read_bytes()), image=candidate,
        source_revision=environment["QT_IMAGE_SOURCE_REVISION"],
        source_tree_hash=environment["QT_IMAGE_SOURCE_TREE_HASH"])
    manifest_path = state/"package.json"; host_boundary.save_receipt(manifest_path, manifest, initial=True)
    # Synthetic runtime peers: retain the image-only publication contract here;
    # the dedicated runtime-inspector tests exercise complete recipe admission.
    from scripts.automation import storage_online_runtime as runtime
    recipe = dict(name=kwargs["project"], services={name:dict(image=kwargs["image"])
        for name in runtime._APPLICATIONS})
    recipe["services"]["tsdb"] = dict(image="unchanged-disposable-database")
    host_boundary.save_receipt(state/runtime.RUNTIME_RECIPE, recipe, initial=True)
    real_preflight, real_replace = operation.inspect_prepared_operation, amendment._replace
    def fixture_preflight(state_root, **arguments):
        launch.inspect_candidate_image(arguments["image"], arguments["request"])
        return initial.admit_serving_source(state_root, project=kwargs["project"],
            source_revision=kwargs["source_revision"], operator_id=arguments["operator_id"])
    def publish_then_lose_reply(target, before, after):
        real_replace(target, before, after)
        if target.name == amendment.REQUEST:
            raise TimeoutError("owned package publication reply loss")
    operation.inspect_prepared_operation = fixture_preflight
    try:
        inspected = operation.run_operation_plan(path, replacement_package_file=manifest_path)
        assert inspected["storage_mutations_performed"] is False
        amendment._replace = publish_then_lose_reply
        try:
            operation.run_operation_plan(path, replacement_package_file=manifest_path, execute=True)
            raise AssertionError("package publication interruption missing")
        except TimeoutError as exc:
            assert str(exc) == "owned package publication reply loss"
        finally:
            amendment._replace = real_replace
        before = host_boundary.load_receipt(state/amendment.PACKAGE_STATE,max_bytes=amendment.PACKAGE_JOURNAL_BYTES)
        try:
            amendment.require_settled(state)
            raise AssertionError("partial package did not block launch")
        except RuntimeError as exc:
            assert str(exc) == "storage_online_package_amendment_requires_reconciliation"
        completed = operation.run_operation_plan(path, replacement_package_file=manifest_path, execute=True)
        after = host_boundary.load_receipt(state/amendment.PACKAGE_STATE,max_bytes=amendment.PACKAGE_JOURNAL_BYTES)
        assert completed["migration_started"] is False and after["phase"] == "complete"
        for key in ("intent_sha256", "wall_deadline", "monotonic_deadline"):
            assert after[key] == before[key]
    finally:
        operation.inspect_prepared_operation = real_preflight
        amendment._replace = real_replace
    kwargs["image"] = candidate
    kwargs["request"] = host_boundary.load_receipt(state/amendment.REQUEST)
    with launch.launched_online_worker(state, **kwargs) as (resumed, receipt):
        owned.append(receipt["container_id"])
        assert receipt["container_id"] != old_id and receipt["deadline"] == old_worker["deadline"]
        channel = host_boundary.OnlineWorkerChannel(resumed,
            deadline=time.monotonic()+receipt["deadline"]-time.time())
        assert channel.greeting["state"] == "background"
        assert host_boundary.load_receipt(state/launch._STATE)["capture"] == old_worker["capture"]
        assert host_boundary.identities(host_boundary.inventory(kwargs["project"],
            operator_id=receipt["container_id"])) == source
        channel.exchange("prepare_step", step="identity_order", relation=None, max_duration_seconds=30)
        channel.exchange("close")
    assert resumed.returncode == 0
    for identity in (old_id, receipt["container_id"]):
        observed = json.loads(host_boundary.docker("inspect","--format","{{json .State}}",identity))
        assert not observed["Running"] and observed["Pid"] == 0 and not observed["OOMKilled"]
    return dict(original_capture_preserved=True, original_deadline=old_worker["deadline"],
        actual_new_worker_admitted=True, old_worker_preserved=True,
        interrupted_publication_reconciled=True, preparation_through_new_worker=True,
        source_clients_unchanged=True, production_runtime_preflight=False,
        production_capacity_admission=False, final_handoff=False)


def rehearse_terminal_cancellation(*, state, kwargs, history_uuid, control, source, owned):
    """Real confined worker/SQL cancellation; synthetic runtime preflight only."""
    from copy import deepcopy
    import time
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_prepare as initial
    from scripts.automation import storage_online_deadline as amendment
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_terminal as terminal
    with launch.launched_online_worker(state, **kwargs) as (worker, receipt):
        channel=host_boundary.OnlineWorkerChannel(worker,
            deadline=time.monotonic()+receipt["deadline"]-time.time())
        (control/"publish").write_text("publish")
        def phase(step, relation=None):
            return channel.exchange("prepare_step",step=step,relation=relation,max_duration_seconds=30)
        phase("catalog_history","qt_fact_storage_cutover_v1.fact_versions")
        for _ in range(64):
            page=channel.exchange("sql_copy")["result"]
            outcome=page["outcome"]
            if outcome=="raw_relocation_required":phase("raw_history")
            elif outcome=="identity_relocation_required":phase("identity_history")
            elif outcome=="identity_order_required":phase("identity_order")
            elif outcome=="both_tails_observed_empty" and (control/"published").exists():break
        else:raise AssertionError("tiny terminal baseline did not converge")
        phase("identity_capture")
        references=channel.exchange("inspect_references",after=None)["result"]["references"]
        assert references
        # Keep an actual partial native reference set, then cancel without
        # completing the migration or relocating the remaining catalogs.
        phase("reference_prepare",references[0]["relation"])
        channel.exchange("archive_copy")
        channel.exchange("close")
    assert worker.returncode==0
    old=host_boundary.load_receipt(state/launch._STATE)
    owned.append(old["container_id"])
    request=deepcopy(kwargs["request"]);capture_plan=request.pop("capture_preparation")
    keys=state/"terminal-fixture-keys";keys.mkdir(mode=0o700)
    spool=state/"terminal-fixture-spool";spool.mkdir(mode=0o700)
    limits=operation.OperationLimits(preparation_seconds=30,final_seconds=30,recovery_seconds=30,
        runtime_seconds=30,spool_max_bytes=1024**2,spool_max_entries=128,spool_reserve_bytes=1024**2,
        repository_max_bytes=256*1024**2,repository_reserve_bytes=1024**2,recent_free_bytes=1024**2)
    plan=dict(schema_version="qt.storage_online_operation.v1",state_root=str(state),**kwargs,
        source_image=kwargs["image"],history_uuid=history_uuid,history_before=capture_plan["history_before"],
        attempt_seconds=old["capture"]["seconds"],limits=vars(limits),keys_root=str(keys),
        socket_volume=kwargs["project"]+"-unused-socket",spool_destination=str(spool))
    plan["request"]=request;plan["inventory_path"]=str(kwargs["inventory_path"])
    path=state/"terminal-operation.json";host_boundary.save_receipt(path,plan,initial=True)
    manifest=dict(schema_version="qt.storage_online_terminal.v1",plan_sha256=amendment._sha(path.read_bytes()),
        image=kwargs["image"],source_revision=request["source_revision"],source_tree_hash=request["source_tree_hash"])
    manifest_path=state/"terminal-package.json";host_boundary.save_receipt(manifest_path,manifest,initial=True)
    original_files={p:p.read_bytes() for p in (path,state/amendment.REQUEST,state/launch._STATE,kwargs["inventory_path"])}
    actual_preflight,actual_probe=operation.inspect_prepared_operation,terminal._probe
    calls=[]
    def fixture_preflight(state_root,**arguments):
        launch.inspect_candidate_image(arguments["image"],arguments["request"])
        return initial.admit_serving_source(state_root,project=kwargs["project"],
            source_revision=kwargs["source_revision"],operator_id=arguments["operator_id"])
    def lose_committed_reply(*args,**arguments):
        calls.append(arguments["action"])
        result=actual_probe(*args,**arguments)
        saved=host_boundary.load_receipt(state/terminal.PROBE)
        assert saved["retired"] and all(m["readonly"] for m in saved["binding"]["mounts"].values())
        assert not host_boundary.docker("ps","-aq","--filter","id="+saved["container_id"]).strip()
        if arguments["action"]=="apply":raise TimeoutError("owned terminal COMMIT reply lost")
        return result
    operation.inspect_prepared_operation=fixture_preflight
    terminal._probe=lose_committed_reply
    try:
        inspected=operation.run_operation_plan(path,cancel_attempt_file=manifest_path)
        assert inspected["storage_mutations_performed"] is False and not (state/terminal.STATE).exists()
        try:
            operation.run_operation_plan(path,cancel_attempt_file=manifest_path,execute=True)
            raise AssertionError("lost terminal COMMIT reply did not interrupt")
        except TimeoutError as exc:assert str(exc)=="owned terminal COMMIT reply lost"
        before=host_boundary.load_receipt(state/terminal.STATE,max_bytes=524288)
        assert before["phase"]=="dispatched"
        completed=operation.run_operation_plan(path,cancel_attempt_file=manifest_path,execute=True)
        after=host_boundary.load_receipt(state/terminal.STATE,max_bytes=524288)
        assert after["phase"]=="complete" and completed["source_retained"]
        assert calls.count("apply")==1
        assert all(after[k]==before[k] for k in ("intent_sha256","wall_deadline","monotonic_deadline","boot_id"))
        assert all(p.read_bytes()==data for p,data in original_files.items())
        try:amendment.require_settled(state)
        except RuntimeError as exc:assert str(exc)=="storage_online_terminal_intent_requires_terminal_owner"
        else:raise AssertionError("terminal intent permitted normal migration")
    finally:
        operation.inspect_prepared_operation=actual_preflight;terminal._probe=actual_probe
    status=json.loads(host_boundary.docker("inspect","--format","{{json .State}}",old["container_id"]))
    assert not status["Running"] and status["Pid"]==0 and not status["OOMKilled"]
    assert host_boundary.identities(host_boundary.inventory(kwargs["project"],operator_id=old["container_id"]))==source
    return dict(read_only_worker_mounts=True,partial_native_references_and_archives=True,
        committed_reply_loss_reconciled=True,apply_dispatches=calls.count("apply"),
        original_inputs_and_clocks_preserved=True,old_worker_preserved=True,transient_workers_retired=True,
        source_clients_unchanged=True,production_runtime_preflight=False,production_cardinality=False,
        expired_host_attempt_qualified=False,final_handoff=False)
