"""Disposable runtime configuration shared by online host rehearsals."""
import json
from contextlib import contextmanager
import hashlib
import inspect
from pathlib import Path
import time
from scripts.automation import storage_host_boundary as host_boundary


@contextmanager
def missing_repository_configuration():
    """Reproduce the old host command against a fixture with only recovery keys."""
    from scripts.automation import storage_online_repositories as repositories
    original = repositories.prepare_repositories
    source = inspect.getsource(original)
    needle = 'incremental_configuration=incremental_configuration(state_root),'
    assert source.count(needle) == 1
    source = source.replace(needle, 'incremental_config="/run/quanttrad/recovery/incremental-config.json",')
    helper = repositories._PREPARE
    begin = helper.index('# Reuse the admitted maintenance configuration.')
    end = helper.index("for name in ('inventory','incremental_config'", begin)
    namespace = dict(vars(repositories), _PREPARE=helper[:begin]+helper[end:])
    exec(compile(source, '<disposable-old-repository-preparer>', 'exec'), namespace)
    repositories.prepare_repositories = namespace['prepare_repositories']
    try:
        yield
    finally:
        repositories.prepare_repositories = original


@contextmanager
def historical_archiver_preflight(enabled):
    """Fixture-only reproduction of an already-committed pre-guard deployment.

    The initial legacy attempt predates archiver admission. Production guards
    remain unchanged; the correcting v2 request uses the real guard and Docker.
    """
    from scripts.automation import storage_online_recovery as recovery
    original = recovery.inspect_database_archiver
    if enabled:
        recovery.inspect_database_archiver = lambda *args: {'pgbackrest_version': 'fixture historical preflight'}
    try:
        yield
    finally:
        recovery.inspect_database_archiver = original


def recover_missing_database_archiver(operation_file, *, state, owned, run, image):
    """Real expired WAL-wait failure, image replacement and normal runtime recovery."""
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_final as final, storage_online_runtime as runtime
    from scripts.automation import storage_online_recovery as recovery
    from scripts.automation import storage_online_repositories as repositories
    names = (final.STATE, recovery.CONTINUATION_STATE, recovery.RECIPE, runtime.RUNTIME_RECIPE)
    prior = {name: (state/name).read_bytes() for name in names}
    saved = final._load(state/final.STATE)
    old_id = saved['recovery']['replacement_id']
    old_details = host_boundary.database_details(old_id)
    helper = saved['repositories']['helper_id']
    owned.append(helper)
    helper_state = run(['inspect', helper, '--format', '{{json .State}}']).stdout
    assert saved['repositories']['completed'] == list(repositories._ACTIONS)
    assert json.loads(host_boundary.maintenance_query(old_id,
        'SELECT to_json(archived_count)::text FROM pg_stat_archiver')) == 0
    assert 'pgbackrest: not found' in run(['logs', '--tail', '100', old_id]).stderr
    try:
        recovery.inspect_database_archiver(old_id)
        raise AssertionError('missing tool passed normal preflight')
    except RuntimeError as exc:
        assert 'storage_database_archiver_unavailable' in str(exc)
    image = run(['image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip()
    request = state/'archiver-continuation-request.json'
    host_boundary.save_receipt(request, dict(schema_version='qt.storage_repository_continuation.v2',
        operation_sha256=hashlib.sha256(operation_file.read_bytes()).hexdigest(),
        final_sha256=hashlib.sha256(prior[final.STATE]).hexdigest(), duration_seconds=300,
        previous_continuation_sha256=hashlib.sha256(prior[recovery.CONTINUATION_STATE]).hexdigest(),
        database_image=image), initial=True)
    observed = operation.run_operation_plan(operation_file, recover_repositories_file=request)
    assert observed['storage_mutations_performed'] is False
    assert {name: (state/name).read_bytes() for name in names} == prior
    result = operation.run_operation_plan(operation_file, recover_repositories_file=request, execute=True)
    audit = host_boundary.load_receipt(state/recovery.CONTINUATION_STATE)
    current = final._load(state/final.STATE)
    assert audit['phase'] == 'runtime_ready'
    assert audit['database_replacement']['completed'] == ['stop', 'remove', 'create', 'start', 'publish']
    assert audit['deadline']-audit['started_at'] <= 300
    for name in names:
        assert Path(audit['previous_records'][name]).read_bytes() == prior[name]
    new_id = current['recovery']['replacement_id']
    owned.append(new_id)
    details = host_boundary.database_details(new_id)
    assert details['image'] == image and new_id != old_id
    assert host_boundary.database_contract(details) == host_boundary.database_contract(old_details)
    assert sorted(details['mounts'], key=lambda v:v['Destination']) == sorted(old_details['mounts'], key=lambda v:v['Destination'])
    assert current['repositories']['completed'] == saved['repositories']['completed']
    assert current['repositories']['helper_id'] == helper
    assert run(['inspect', helper, '--format', '{{json .State}}']).stdout == helper_state
    assert all(current[k] == saved[k] for k in ('started_at', 'started_boot', 'boot_id', 'commit', 'binding'))
    try:
        operation.run_operation_plan(operation_file, recover_repositories_file=request, execute=True)
        raise AssertionError('archiver correction replay admitted')
    except RuntimeError as exc:
        assert str(exc) == 'storage_archiver_recovery_previous_attempt_unresolved'
    return result


def recover_missing_repository_configuration(operation_file, *, state, owned, run, archiver_image=None):
    """Exercise the public recovery command after actual original-clock expiry."""
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_final as final
    from scripts.automation import storage_online_recovery as recovery
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_repositories as recovery_repositories
    # Includes actual application startup and encrypted recovery checks; native
    # runners measured about 120s through the last UI restoration check.
    recovery_seconds = 90 if archiver_image else 180
    original = (state/final.STATE).read_bytes()
    saved = final._load(state/final.STATE)
    worker = host_boundary.load_receipt(state/launch._STATE)
    assert saved['phase'] == 'recovery_repository_preparing'
    assert saved['repositories']['completed'] == ['logins', 'create']
    assert saved['repositories']['inflight'] == 'prepare'
    failed = saved['repositories']['helper_id']
    failed_project = json.loads(run(['inspect', failed, '--format', '{{json .Config.Labels}}']).stdout)['com.docker.compose.project']
    owned.append(failed)
    while time.time() <= saved['deadline']+.1:
        time.sleep(min(.2, saved['deadline']+.2-time.time()))
    request = state/'repository-continuation-request.json'
    host_boundary.save_receipt(request, dict(schema_version='qt.storage_repository_continuation.v1',
        operation_sha256=hashlib.sha256(operation_file.read_bytes()).hexdigest(),
        final_sha256=hashlib.sha256(original).hexdigest(), duration_seconds=recovery_seconds), initial=True)
    with historical_archiver_preflight(bool(archiver_image)):
        observed = operation.run_operation_plan(operation_file, recover_repositories_file=request)
    assert observed['storage_mutations_performed'] is False
    assert not (state/recovery.CONTINUATION_STATE).exists()
    assert (state/final.STATE).read_bytes() == original
    if archiver_image:
        try:
            with historical_archiver_preflight(True):
                operation.run_operation_plan(operation_file, recover_repositories_file=request, execute=True)
            raise AssertionError('database missing archiver unexpectedly completed')
        except RuntimeError:
            failed_final = final._load(state/final.STATE)
            assert failed_final['phase'] == 'recovery_repository_preparing'
            assert failed_final['repositories']['completed'] == list(recovery_repositories._ACTIONS)
            assert failed_final['repositories']['inflight'] is None
            assert time.time() >= failed_final['deadline']
        result = recover_missing_database_archiver(operation_file, state=state, owned=owned,
                                                  run=run, image=archiver_image)
        audit = host_boundary.load_receipt(Path(host_boundary.load_receipt(
            state/recovery.CONTINUATION_STATE)['previous_records'][recovery.CONTINUATION_STATE]))
    else:
        result = operation.run_operation_plan(operation_file, recover_repositories_file=request, execute=True)
        audit = host_boundary.load_receipt(state/recovery.CONTINUATION_STATE)
    assert Path(audit['original_final']).read_bytes() == original
    assert host_boundary.load_receipt(state/launch._STATE) == worker
    assert audit['phase'] == ('preparing' if archiver_image else 'runtime_ready')
    assert audit['deadline']-audit['started_at'] <= recovery_seconds
    current = final._load(state/final.STATE)
    replacement_project = json.loads(run(['inspect', current['repositories']['helper_id'],
        '--format', '{{json .Config.Labels}}']).stdout)['com.docker.compose.project']
    assert replacement_project != failed_project
    assert all(current[k] == saved[k] for k in ('started_at', 'started_boot', 'boot_id', 'commit', 'binding'))
    status = json.loads(run(['inspect', failed, '--format', '{{json .State}}']).stdout)
    assert status['Status'] == 'exited' and status['Pid'] == 0 and status['ExitCode'] == 1
    try:
        operation.run_operation_plan(operation_file, recover_repositories_file=request, execute=True)
        raise AssertionError('completed continuation replay admitted')
    except RuntimeError as exc:
        assert str(exc) == 'storage_repository_continuation_intent_exists_reconcile_required'
    return result


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
      # Match the production backend/maintenance startup grace. The same real
      # readiness probe must pass; a cold multi-worker import can exceed 40s.
      service['healthcheck']=dict(test=probe,interval='1s',timeout='3s',retries=15,
        start_period='45s' if service_name in ('backend','storage-maintenance') else '2s')
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


def rehearse_terminal_cancellation(*, state, kwargs, history_uuid, control, source, owned, expired=False, terminal_image=None, source_image=None, final_seconds=30):
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
    limits=operation.OperationLimits(preparation_seconds=30,final_seconds=final_seconds,recovery_seconds=30,
        runtime_seconds=30,spool_max_bytes=1024**2,spool_max_entries=128,spool_reserve_bytes=1024**2,
        repository_max_bytes=256*1024**2,repository_reserve_bytes=1024**2,recent_free_bytes=1024**2)
    plan=dict(schema_version="qt.storage_online_operation.v1",state_root=str(state),**kwargs,
        source_image=source_image or kwargs["image"],history_uuid=history_uuid,history_before=capture_plan["history_before"],
        attempt_seconds=old["capture"]["seconds"],limits=vars(limits),keys_root=str(keys),
        socket_volume=kwargs["project"]+"-unused-socket",spool_destination=str(spool))
    plan["request"]=request;plan["inventory_path"]=str(kwargs["inventory_path"])
    path=state/"terminal-operation.json";host_boundary.save_receipt(path,plan,initial=True)
    terminal_identity = host_boundary.docker("image", "inspect", terminal_image or kwargs["image"],
        "--format", "{{.Id}}").strip()
    manifest=dict(schema_version="qt.storage_online_terminal.v1",plan_sha256=amendment._sha(path.read_bytes()),
        image=terminal_identity,source_revision=request["source_revision"],source_tree_hash=request["source_tree_hash"])
    manifest_path=state/"terminal-package.json";host_boundary.save_receipt(manifest_path,manifest,initial=True)
    original_files={p:p.read_bytes() for p in (path,state/amendment.REQUEST,state/launch._STATE,kwargs["inventory_path"])}
    if expired:
        # This fixture creates its original 180-second capture once. Wait for
        # natural expiry; never amend its start, lifetime, or worker receipt.
        remaining=old["deadline"]-time.time()
        assert old["capture"]["seconds"]==180 and remaining<=180
        wait_deadline=time.monotonic()+max(0,remaining)+1
        while time.monotonic()<wait_deadline:
            time.sleep(max(0,min(.1,wait_deadline-time.monotonic())))
        assert time.time()>=old["deadline"]
        assert all(p.read_bytes()==data for p,data in original_files.items())
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
    return dict(terminal_image=manifest["image"],read_only_worker_mounts=True,partial_native_references_and_archives=True,
        committed_reply_loss_reconciled=True,apply_dispatches=calls.count("apply"),
        original_inputs_and_clocks_preserved=True,old_worker_preserved=True,transient_workers_retired=True,
        source_clients_unchanged=True,production_runtime_preflight=False,production_cardinality=False,
        expired_host_attempt_qualified=expired,final_handoff=False)



def rehearse_forward_package(*, state, kwargs, candidate_image, source, launch_worker=False, retire_worker=False, operation_route=False, final_mode=None, prepare_keys=False, retirement_recovery=False):
    """Actual canceled SQL proof and durable publication; synthetic runtime peers."""
    from datetime import datetime, timezone, timedelta
    from scripts.automation import storage_online_forward as forward
    from scripts.automation import storage_online_terminal as terminal
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_prepare as initial
    from scripts.automation import storage_online_deadline as publication
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_runtime as runtime
    path=state/"terminal-operation.json"
    canceled=host_boundary.load_receipt(state/terminal.STATE,max_bytes=524288)
    old_worker=canceled["worker"]
    original={p:p.read_bytes() for p in (path,state/terminal.STATE,kwargs["inventory_path"])}
    candidate=host_boundary.docker("image","inspect",candidate_image,"--format","{{.Id}}").strip()
    assert candidate!=kwargs["image"], "fixture must change actual candidate provenance"
    environment=dict(v.split("=",1) for v in json.loads(host_boundary.docker(
        "image","inspect",candidate,"--format","{{json .Config.Env}}")))
    manifest=dict(schema_version="qt.storage_online_forward_package.v1",
        plan_sha256=publication._sha(path.read_bytes()),image=candidate,
        source_revision=environment["QT_IMAGE_SOURCE_REVISION"],source_tree_hash=environment["QT_IMAGE_SOURCE_TREE_HASH"],
        forward_plan_path=str(state/"forward-operation.json"),
        end_day=(datetime.now(timezone.utc).date()+timedelta(days=0 if operation_route or final_mode else 1)).isoformat())
    package=state/"forward-package.json";host_boundary.save_receipt(package,manifest,initial=True)
    recipe=dict(name=kwargs["project"],services={name:dict(image=kwargs["image"]) for name in runtime._APPLICATIONS})
    # The synthetic peer still carries the real backend identity fields. The
    # legacy publisher retains these labels; its successor must replace them.
    recipe["services"]["backend"]["environment"] = dict(
        SOURCE_REVISION=kwargs["request"]["source_revision"],
        SOURCE_TREE_HASH=kwargs["request"]["source_tree_hash"],
        QT_BOT_RUNTIME_IMAGE="quanttrad-backend:"+kwargs["request"]["source_revision"],
        FIXTURE_PRESERVED="unchanged")
    recipe["services"]["tsdb"]=dict(image="unchanged-disposable-database")
    host_boundary.save_receipt(state/runtime.RUNTIME_RECIPE,recipe,initial=True)
    actual_preflight,actual_replace=operation.inspect_prepared_operation,publication._replace
    preflights=[]
    def synthetic_preflight(state_root,**arguments):
        launch.inspect_candidate_image(arguments["image"],arguments["request"])
        assert arguments["proposed_runtime_recipe"]["services"]["backend"]["image"]==arguments["image"]
        preflights.append(arguments["image"])
        return initial.admit_serving_source(state_root,project=kwargs["project"],
            source_revision=kwargs["source_revision"],operator_id=arguments["operator_id"])
    def lose_reply(target,before,after):
        actual_replace(target,before,after)
        if target.name==publication.REQUEST: raise TimeoutError("forward request publication reply lost")
    operation.inspect_prepared_operation=synthetic_preflight
    key_report = {}
    try:
        if prepare_keys:
            from scripts.automation import storage_online_keys as key_owner
            key_original = {p:p.read_bytes() for p in (path,state/terminal.STATE,state/publication.REQUEST,
                                                      state/launch._STATE,state/runtime.RUNTIME_RECIPE)}
            earlier = {**manifest, "image": kwargs["image"],
                       "source_revision": kwargs["request"]["source_revision"],
                       "source_tree_hash": kwargs["request"]["source_tree_hash"]}
            earlier_package = state/"earlier-key-inspection-package.json"
            host_boundary.save_receipt(earlier_package, earlier, initial=True)
            operation.run_operation_plan(path, prepare_forward_keys_file=earlier_package)
            previous_probe = host_boundary.load_receipt(state/terminal.KEY_PROBE)
            previous_bytes = (state/terminal.KEY_PROBE).read_bytes()
            assert previous_probe["retired"] and not (state/key_owner.STATE).exists()
            inspected_keys = operation.run_operation_plan(path,prepare_forward_keys_file=package)
            assert not inspected_keys["keys_prepared"] and not (state/key_owner.STATE).exists()
            retained = (state/terminal.KEY_PROBE).with_name(
                Path(terminal.KEY_PROBE).stem+".retired-"+host_boundary.digest(previous_probe)+".json")
            assert retained.read_bytes() == previous_bytes
            assert host_boundary.load_receipt(state/terminal.KEY_PROBE)["owner"]["package"] == manifest
            actual_save, actual_probe = host_boundary.save_receipt, terminal._probe
            def lose_key_create(target,value,**options):
                actual_save(target,value,**options)
                if (target.name==terminal.KEY_PROBE and value.get("container_id")
                        and (state/key_owner.STATE).exists()
                        and host_boundary.load_receipt(state/key_owner.STATE)["phase"]=="dispatched"):
                    raise TimeoutError("key worker create reply lost")
            host_boundary.save_receipt=lose_key_create
            try:
                operation.run_operation_plan(path,prepare_forward_keys_file=package,execute=True)
                raise AssertionError("missing key create interruption")
            except TimeoutError as exc: assert str(exc)=="key worker create reply lost"
            finally: host_boundary.save_receipt=actual_save
            key_intent=host_boundary.load_receipt(state/key_owner.STATE)
            key_created=host_boundary.load_receipt(state/terminal.KEY_PROBE)["container_id"]
            key_status=json.loads(host_boundary.docker("inspect","--format","{{json .State}}",key_created))
            assert key_status["Status"]=="created" and key_status["Pid"]==0
            key_actions=[]
            def lose_key_reply(*args,**options):
                key_actions.append(options["action"])
                value=actual_probe(*args,**options)
                if options["action"]=="prepare_keys":raise TimeoutError("key preparation acknowledgement lost")
                return value
            terminal._probe=lose_key_reply
            try:
                operation.run_operation_plan(path,prepare_forward_keys_file=package,execute=True)
                raise AssertionError("missing key acknowledgement interruption")
            except TimeoutError as exc: assert str(exc)=="key preparation acknowledgement lost"
            finally: terminal._probe=actual_probe
            assert key_actions.count("prepare_keys")==1
            assert not host_boundary.docker("ps","-aq","--filter","id="+key_created).strip()
            reconciled=operation.run_operation_plan(path,prepare_forward_keys_file=package)
            key_complete=host_boundary.load_receipt(state/key_owner.STATE)
            assert reconciled["keys_prepared"] and key_complete["phase"]=="complete"
            assert key_owner._immutable(key_complete)==key_owner._immutable(key_intent)
            assert all(p.read_bytes()==data for p,data in key_original.items())
            assert not (state/forward.STATE).exists() and not Path(manifest["forward_plan_path"]).exists()
            probe=host_boundary.load_receipt(state/terminal.KEY_PROBE)
            assert probe["retired"] and not host_boundary.docker("ps","-aq","--filter","id="+probe["container_id"]).strip()
            observed=json.loads(host_boundary.database_query(old_worker["binding"]["database_id"],
                "SELECT json_build_object('initialization',to_regclass('qt_fact_header_forward_v2.initialization'),"
                "'adoption',to_regclass('qt_fact_header_forward_v2.adoption'))::text",read_only_seconds=5))
            assert observed==dict(initialization=None,adoption=None)
            assert preflights==[kwargs["image"],kwargs["image"]]+[kwargs["image"],candidate]*4
            preflights.clear()
            key_report=dict(canonical_key_only_preparation=True,actual_key_confined_entrypoint=True,
                retired_key_inspection_candidate_change=True,previous_inspection_receipt_preserved=True,
                key_create_interruption_retired=True,key_acknowledgement_reconciled_without_redispatch=True,
                key_original_clocks_preserved=True,key_only_no_initialization_or_adoption=True)
        inspected=operation.run_operation_plan(path,forward_package_file=package)
        assert not inspected["storage_mutations_performed"] and not (state/forward.STATE).exists()
        publication._replace=lose_reply
        try:
            operation.run_operation_plan(path,forward_package_file=package,execute=True)
            raise AssertionError("missing interrupted forward publication")
        except TimeoutError as exc: assert str(exc)=="forward request publication reply lost"
        finally: publication._replace=actual_replace
        before=host_boundary.load_receipt(state/forward.STATE,max_bytes=forward._MAX_BYTES)
        assert before["phase"]=="publishing"
        assert host_boundary.load_receipt(state/publication.REQUEST)["forward"]==before["forward"]
        completed=operation.run_operation_plan(path,forward_package_file=package,execute=True)
        after=host_boundary.load_receipt(state/forward.STATE,max_bytes=forward._MAX_BYTES)
        assert completed["phase"]=="forward_package_published" and not completed["forward_worker_authorized"]
        assert {k:v for k,v in after.items() if k!="phase"}=={k:v for k,v in before.items() if k!="phase"}
        assert preflights==[kwargs["image"],candidate]*3
        assert all(p.read_bytes()==data for p,data in original.items())
        try: publication.require_settled(state)
        except RuntimeError as exc: assert str(exc)=="storage_online_terminal_intent_requires_terminal_owner"
        else: raise AssertionError("metadata publication authorized legacy launch")
    finally:
        operation.inspect_prepared_operation=actual_preflight;publication._replace=actual_replace
    status=json.loads(host_boundary.docker("inspect","--format","{{json .State}}",old_worker["container_id"]))
    assert not status["Running"] and status["Pid"]==0 and not status["OOMKilled"]
    assert host_boundary.identities(host_boundary.inventory(kwargs["project"],operator_id=old_worker["container_id"]))==source
    probe=host_boundary.load_receipt(state/terminal.PROBE)
    assert probe["retired"] and not host_boundary.docker("ps","-aq","--filter","id="+probe["container_id"]).strip()
    launched = {}
    if launch_worker:
        import time
        from scripts.automation import storage_online_final as final
        published=forward.inspect_published_operation(state)
        arguments={**kwargs,"image":candidate,"request":published["new_request"]}
        actual_save=host_boundary.save_receipt
        def lose_worker_reply(target,value,**options):
            actual_save(target,value,**options)
            if target.name==launch._STATE and value.get("container_id"):
                raise TimeoutError("forward worker publication reply lost")
        host_boundary.save_receipt=lose_worker_reply
        try:
            with launch.launched_online_worker(state,**arguments):
                raise AssertionError("missing interrupted forward launch")
        except TimeoutError as exc: assert str(exc)=="forward worker publication reply lost"
        finally: host_boundary.save_receipt=actual_save
        intent=forward.load_launch(state)
        created=intent["pending_worker"]["container_id"]
        actual=json.loads(host_boundary.docker("inspect","--format","{{json .State}}",created))
        assert actual["Status"]=="created" and actual["Pid"]==0 and not actual["Running"]
        if final_mode:
            assert not retire_worker and not operation_route
            launched = _rehearse_forward_final(state=state, arguments=arguments, created=created,
                launch_intent=intent, plan_path=Path(manifest["forward_plan_path"]), mode=final_mode)
        elif operation_route:
            assert retire_worker, "canonical operation fixture requires preserving failure retirement"
            actual_stop = final.stop_online_source_locked
            actual_stop_admission = operation._admit_forward_stop
            operation_preflights = []
            def operation_preflight(state_root, **options):
                launch.inspect_candidate_image(options["image"], options["request"])
                operation_preflights.append(options.get("operator_id"))
                return initial.admit_serving_source(state_root, project=kwargs["project"],
                    source_revision=kwargs["source_revision"], operator_id=options.get("operator_id", created))
            def refuse_before_stop(state_root, **options):
                nonlocal rows, launched
                assert options["worker_id"] == created
                with host_boundary.docker_deadline(time.monotonic()+30):
                    bound, _, rows, limits = final._observe(state_root, **{k:options[k] for k in
                        ("project", "source_revision", "controller_id", "worker_id")})
                ready = forward.load_launch(state_root)
                from scripts.automation.storage_online_forward_worker import capture_binding
                assert ready["pending_worker"] is None and capture_binding(ready["capture"]) == bound["capture"]
                assert ready["forward"] == bound["forward"] and ready["deadline"] == limits["capture_deadline"]
                for key in ("started_at", "started_monotonic", "started_boot", "key_deadline", "key_deadline_monotonic", "key_deadline_boot"):
                    assert ready[key] == intent[key]
                assert host_boundary.source_clients_serving(rows)
                assert operation_preflights == [None, None, created]
                assert not (state/final.STATE).exists()
                launched = dict(forward_worker_started=True, interrupted_launch_reused_created_worker=True,
                    actual_confined_entrypoint=True, actual_host_adoption_observation=True,
                    original_launch_and_sql_clocks_preserved=True, forward_retirement_via_controller=False,
                    canonical_normal_dispatch=True, real_forward_background_prepared=True,
                    injected_pre_stop_refusal=True, canonical_failure_retirement=False, login_closure=False)
                raise RuntimeError("fixture refusal before forward source stop")
            operation.inspect_prepared_operation = operation_preflight
            final.stop_online_source_locked = refuse_before_stop
            # This old-day disposable fixture covers worker lifecycle/reentry,
            # not a real midnight. Unit tests own early/late stop admission;
            # preserve the real clock for all launch and SQL expiry checks.
            operation._admit_forward_stop = lambda intent, **options: operation._forward_cutover_day(intent, **options)
            try:
                forward_path = Path(manifest["forward_plan_path"])
                inspected = operation.run_operation_plan(forward_path, prepare_forward_only=True)
                assert inspected["phase"] == "forward_inspected" and not inspected["storage_mutations_performed"]
                prepared = operation.run_operation_plan(forward_path, prepare_forward_only=True, execute=True)
                assert prepared["phase"] == "forward_background_prepared" and prepared["adoption_active"]
                assert not any(prepared[k] for k in ("source_stopped", "final_switch_authorized", "runtime_activated"))
                prepared_launch = forward.load_launch(state)
                prepared_sql = forward.observe_adoption(old_worker["binding"]["database_id"])
                assert prepared["adoption_deadline"] == prepared_launch["deadline"]
                status = json.loads(host_boundary.docker("inspect", "--format", "{{json .State}}", created))
                assert not status["Running"] and status["Pid"] == 0 and not status["OOMKilled"]
                serving = host_boundary.inventory(kwargs["project"], operator_id=created)
                assert host_boundary.source_clients_serving(serving)
                assert host_boundary.identities(serving) == source
                assert not (state/final.STATE).exists()
                assert all(p.read_bytes() == data for p,data in original.items())
                assert operation_preflights == [None, None, None, created]
                operation_preflights.clear()
                actual_exchange = host_boundary.OnlineWorkerChannel.exchange
                def resumed_exchange(channel, command, **options):
                    if command == "prepare_step":
                        assert options["step"] not in {"reference_prepare", "reference_validate"}, \
                            "completed native references must not reacquire preparation locks"
                    return actual_exchange(channel, command, **options)
                host_boundary.OnlineWorkerChannel.exchange = resumed_exchange
                try:
                    operation.run_operation_plan(Path(manifest["forward_plan_path"]), execute=True)
                finally:
                    host_boundary.OnlineWorkerChannel.exchange = actual_exchange
                raise AssertionError("missing canonical forward pre-stop refusal")
            except RuntimeError as exc:
                assert str(exc) == "fixture refusal before forward source stop"
            finally:
                operation.inspect_prepared_operation = actual_preflight
                final.stop_online_source_locked = actual_stop
                operation._admit_forward_stop = actual_stop_admission
            assert launched.get("canonical_normal_dispatch") and not (state/final.STATE).exists()
            resumed_launch = forward.load_launch(state)
            assert resumed_launch == prepared_launch
            assert forward.observe_adoption(old_worker["binding"]["database_id"]) == prepared_sql
            launched.update(canonical_preparation_only=True, preparation_reader_retired=True,
                source_serving_after_preparation=True, preparation_reentry_original_deadline=True,
                prepared_references_reused_without_schema_steps=True)
        else:
            with launch.launched_online_worker(state,**arguments) as (worker,receipt):
                assert receipt["container_id"]==created
                channel=host_boundary.OnlineWorkerChannel(worker,deadline=time.monotonic()+receipt["deadline"]-time.time())
                with host_boundary.docker_deadline(time.monotonic()+30):
                    bound,_,rows,limits=final._observe(state,project=kwargs["project"],source_revision=kwargs["source_revision"],
                        controller_id=channel.greeting["controller_id"],worker_id=created)
                ready=forward.load_launch(state)
                from scripts.automation.storage_online_forward_worker import capture_binding
                assert ready["pending_worker"] is None and capture_binding(ready["capture"])==bound["capture"]
                assert ready["forward"]==bound["forward"] and ready["deadline"]==limits["capture_deadline"]
                for key in ("started_at","started_monotonic","started_boot","key_deadline","key_deadline_monotonic","key_deadline_boot"):
                    assert ready[key]==intent[key]
                assert host_boundary.source_clients_serving(rows)
                if retire_worker:
                    channel.exchange("close")
                else:
                    canceled_forward=channel.exchange("cancel")
                    assert canceled_forward["state"]=="cancelled"
                    try: forward.observe_adoption(rows["tsdb"]["id"])
                    except RuntimeError as exc: assert str(exc)=="storage_forward_active_adoption_required"
                    else: raise AssertionError("retired forward adoption admitted")
                launched=dict(forward_worker_started=True,interrupted_launch_reused_created_worker=True,
                    actual_confined_entrypoint=True,actual_host_adoption_observation=True,
                    original_launch_and_sql_clocks_preserved=True,forward_retirement_via_controller=not retire_worker,
                    canonical_failure_retirement=False,login_closure=False)
        if retire_worker:
            # A stopped reader alone leaves adoption mirrors active. Exercise
            # the actual canonical terminal command, then lose its COMMIT reply.
            forward.observe_adoption(rows["tsdb"]["id"])
            from scripts.db import fact_header_v2_copy as header_copy, raw_mapping_v2_copy as raw_copy
            from scripts.db import fact_header_v2_capture as original_capture
            relations=sorted({header_copy.SOURCE,header_copy.SCHEMA+".fact_versions",original_capture.SCHEMA+".fact_identities",
                raw_copy.SOURCE,raw_copy.TARGET,raw_copy.QUEUE,original_capture.QUEUE})
            def retained_counts():
                with host_boundary.docker_deadline(time.monotonic()+20):
                    return host_boundary.database_query(rows["tsdb"]["id"],
                        "SELECT json_build_array("+",".join("(SELECT count(*) FROM "+r+")" for r in relations)+")::text",
                        read_only_seconds=5)
            counts_before=retained_counts()
            plan_path=Path(manifest["forward_plan_path"])
            retirement_package = dict(schema_version="qt.storage_online_terminal.v1",
                plan_sha256=publication._sha(plan_path.read_bytes()), image=candidate,
                source_revision=manifest["source_revision"], source_tree_hash=manifest["source_tree_hash"])
            retirement_path=state/"forward-retirement-package.json"
            host_boundary.save_receipt(retirement_path,retirement_package,initial=True)
            from scripts.automation import storage_online_operation as operation
            actual_probe=terminal._probe
            calls=[]
            def lost_retirement(*args,**options):
                calls.append(options["action"])
                value=actual_probe(*args,**options)
                if options["action"]=="apply":raise TimeoutError("forward retirement COMMIT reply lost")
                return value
            def retirement_preflight(state_root,**arguments):
                launch.inspect_candidate_image(arguments["image"],arguments["request"])
                return initial.admit_serving_source(state_root,project=kwargs["project"],
                    source_revision=kwargs["source_revision"],operator_id=arguments.get("operator_id"))
            operation.inspect_prepared_operation=retirement_preflight
            terminal_state = terminal.FORWARD_STATE
            terminal_probe = terminal.FORWARD_PROBE
            failed_retirement_bytes = None
            if retirement_recovery:
                # Real disposable SQL writer: keep its transaction open across the
                # ordinary probe. No credential or production configuration is used.
                import subprocess, select
                writer = subprocess.Popen(["docker", "exec", "-i", rows["tsdb"]["id"], "sh", "-ec",
                    'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -qAt -v ON_ERROR_STOP=1'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    writer.stdin.write("BEGIN; LOCK TABLE market.fact_versions IN ROW EXCLUSIVE MODE; SELECT 'writer_held';\n")
                    writer.stdin.flush()
                    assert select.select([writer.stdout], [], [], 10)[0], "owned writer lock unavailable"
                    assert writer.stdout.readline().strip() == "writer_held"
                    try:
                        operation.run_operation_plan(plan_path, cancel_attempt_file=retirement_path, execute=True)
                        raise AssertionError("ordinary retirement did not refuse its conflicting writer")
                    except RuntimeError as exc:
                        assert str(exc) == "storage_online_terminal_worker_failed_or_output_exceeded"
                finally:
                    writer.stdin.write("ROLLBACK;\n\\q\n"); writer.stdin.flush(); writer.stdin.close()
                    assert writer.wait(timeout=10) == 0
                failed_retirement_bytes = (state/terminal.FORWARD_STATE).read_bytes()
                failed_retirement = host_boundary.load_receipt(state/terminal.FORWARD_STATE,max_bytes=524288)
                assert failed_retirement["phase"] == "dispatched"
                assert retained_counts() == counts_before
                retirement_package = {**retirement_package, "schema_version": terminal.RECOVERY_SCHEMA,
                    "previous_terminal_sha256": publication._sha(failed_retirement_bytes)}
                retirement_path = state/"forward-retirement-recovery-package.json"
                host_boundary.save_receipt(retirement_path, retirement_package, initial=True)
                terminal_state = terminal.FORWARD_RECOVERY_STATE
                terminal_probe = terminal.FORWARD_RECOVERY_PROBE
            terminal._probe=lost_retirement
            try:
                operation.run_operation_plan(plan_path,cancel_attempt_file=retirement_path,execute=True)
                raise AssertionError("missing forward retirement reply interruption")
            except TimeoutError as exc:assert str(exc)=="forward retirement COMMIT reply lost"
            finally:
                terminal._probe=actual_probe
                operation.inspect_prepared_operation=actual_preflight
            interrupted=host_boundary.load_receipt(state/terminal_state,max_bytes=524288)
            assert interrupted["phase"]=="dispatched" and calls.count("apply")==1
            operation.inspect_prepared_operation=retirement_preflight
            try:result=operation.run_operation_plan(plan_path,cancel_attempt_file=retirement_path)
            finally:operation.inspect_prepared_operation=actual_preflight
            assert result["phase"]=="forward_retired"
            complete=host_boundary.load_receipt(state/terminal_state,max_bytes=524288)
            assert complete["phase"]=="complete"
            assert all(complete[k]==interrupted[k] for k in ("wall_deadline","monotonic_deadline","boot_id","intent_sha256"))
            probe=host_boundary.load_receipt(state/terminal_probe)
            assert probe["retired"] and not host_boundary.docker("ps","-aq","--filter","id="+probe["container_id"]).strip()
            try:forward.observe_adoption(rows["tsdb"]["id"])
            except RuntimeError as exc:assert str(exc)=="storage_forward_active_adoption_required"
            else:raise AssertionError("host retired adoption still admitted")
            assert retained_counts()==counts_before
            if failed_retirement_bytes is not None:
                assert (state/terminal.FORWARD_STATE).read_bytes() == failed_retirement_bytes
                launched.update(explicit_retirement_recovery=True, conflicting_writer_refusal_reproduced=True,
                    failed_retirement_journal_preserved=True)
            launched.update(canonical_failure_retirement=True,actual_forward_terminal_command=True,
                retirement_lost_commit_reconciled_without_dispatch=True,original_terminal_preserved=True,
                fixture_source_copy_and_queue_counts_preserved=True)
            if retirement_recovery:
                successor_report, created = _rehearse_retained_successor(state=state, plan_path=plan_path,
                    manifest=manifest, terminal_file=terminal_state, preflight=retirement_preflight)
                launched.update(successor_report)
        assert all(p.read_bytes()==data for p,data in original.items())
        assert host_boundary.identities(host_boundary.inventory(kwargs["project"],operator_id=created))==source
    return dict(candidate_image=candidate,interrupted_publication_reconciled=True,
        actual_cancellation_reconciled_with_original_request=True,original_plan_terminal_and_inventory_preserved=True,
        original_publication_clocks_preserved=True,old_worker_preserved=True,source_clients_unchanged=final_mode != "commit",
        production_runtime_preflight=False,production_cardinality=False,final_handoff=final_mode == "commit",
        **key_report, **({"forward_worker_started":False}|launched))


def _rehearse_retained_successor(*, state, plan_path, manifest, terminal_file, preflight):
    """Actual confined lookup/publication/worker/terminal after retained retirement."""
    from scripts.automation import storage_online_forward as forward
    from scripts.automation import storage_online_terminal as terminal
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_deadline as publication
    from scripts.automation import storage_online_keys as lookup_owner
    prior = forward.inspect_published_operation(state)
    retired = host_boundary.load_receipt(state/terminal_file, max_bytes=524288)
    protected = {path:path.read_bytes() for path in (plan_path, state/terminal.STATE,
        state/terminal.FORWARD_STATE, state/terminal.FORWARD_RECOVERY_STATE, state/forward.STATE, state/forward.LAUNCH_STATE)}
    package = dict(schema_version="qt.storage_online_lookup_package.v1", plan_sha256=publication._sha(plan_path.read_bytes()),
        **{k:manifest[k] for k in ("image", "source_revision", "source_tree_hash")},
        operation_sha256=host_boundary.digest(dict(predecessor=prior["forward"]["operation_sha256"], purpose="lookup-fixture")),
        predecessor_operation_sha256=prior["forward"]["operation_sha256"],
        predecessor_terminal_sha256=retired["receipt"]["terminal_sha256"], predecessor_terminal_file=terminal_file,
        predecessor_terminal_file_sha256=publication._sha((state/terminal_file).read_bytes()))
    file = state/"lookup-package.json"
    host_boundary.save_receipt(file, package, initial=True)
    actual_preflight, actual_probe = operation.inspect_prepared_operation, terminal._probe
    calls = []
    def lose_move(*args, **kwargs):
        calls.append(kwargs["action"])
        result = actual_probe(*args, **kwargs)
        if kwargs["action"] == "place_lookups": raise TimeoutError("lookup placement COMMIT reply lost")
        return result
    operation.inspect_prepared_operation = preflight
    try:
        assert not operation.run_operation_plan(plan_path, place_forward_lookups_file=file)["storage_mutations_performed"]
        terminal._probe = lose_move
        try:
            operation.run_operation_plan(plan_path, place_forward_lookups_file=file, execute=True)
            raise AssertionError("missing lookup lost reply")
        except TimeoutError as exc: assert str(exc) == "lookup placement COMMIT reply lost"
        finally: terminal._probe = actual_probe
        assert operation.run_operation_plan(plan_path, place_forward_lookups_file=file)["indexes_placed"]
        lookup_journal = host_boundary.load_receipt(state/forward.operation_file(lookup_owner.LOOKUP_STATE,
            operation_sha256=package["operation_sha256"]), max_bytes=forward._MAX_BYTES)
        assert lookup_journal["phase"] == "complete" and calls.count("place_lookups") == 1
        successor_package = {k:v for k,v in package.items() if k != "operation_sha256"}
        successor_package.update(schema_version="qt.storage_online_forward_package.v2",
            lookup_operation_sha256=package["operation_sha256"], end_day=manifest["end_day"],
            forward_plan_path=str(state/"successor-operation.json"))
        successor_file = state/"successor-package.json"
        host_boundary.save_receipt(successor_file, successor_package, initial=True)
        assert not operation.run_operation_plan(plan_path, forward_package_file=successor_file)["storage_mutations_performed"]
        operation.run_operation_plan(plan_path, forward_package_file=successor_file, execute=True)
        published_runtime = host_boundary.load_receipt(state/forward.runtime.RUNTIME_RECIPE)
        assert published_runtime["services"]["backend"]["environment"] == dict(
            SOURCE_REVISION=successor_package["source_revision"],
            SOURCE_TREE_HASH=successor_package["source_tree_hash"],
            QT_BOT_RUNTIME_IMAGE="quanttrad-backend:"+successor_package["source_revision"],
            FIXTURE_PRESERVED="unchanged")
        successor_path = Path(successor_package["forward_plan_path"])
        prepared = operation.run_operation_plan(successor_path, prepare_forward_only=True, execute=True)
        assert prepared["phase"] == "forward_background_prepared" and prepared["adoption_active"]
        selected = forward.inspect_published_operation(state, operation_path=successor_path)
        ready = forward.load_launch(state, request=selected["new_request"])
        worker = host_boundary.load_receipt(state/launch._STATE)
        assert ready["worker"] == worker and ready["pending_worker"] is None
        initialization, capture = forward.observe_adoption(worker["binding"]["database_id"], request=selected["new_request"])
        forward.admit_launched_adoption(state, selected["new_request"], worker, initialization=initialization, capture=capture)
        successor_path, selected, worker = _rehearse_forward_reschedule(
            state=state, plan_path=successor_path, selected=selected, worker=worker)
        successor_path, selected, worker = _rehearse_forward_reschedule(
            state=state, plan_path=successor_path, selected=selected, worker=worker, version=2)
        successor_path, selected, worker = _rehearse_forward_reschedule(
            state=state, plan_path=successor_path, selected=selected, worker=worker, version=3)
        cancel_package = dict(schema_version="qt.storage_online_terminal.v1", plan_sha256=publication._sha(successor_path.read_bytes()),
            **{k:manifest[k] for k in ("image", "source_revision", "source_tree_hash")})
        cancel_file = state/"successor-retirement-package.json"
        host_boundary.save_receipt(cancel_file, cancel_package, initial=True)
        assert operation.run_operation_plan(successor_path, cancel_attempt_file=cancel_file, execute=True)["phase"] == "forward_retired"
        assert all(path.read_bytes() == data for path,data in protected.items())
        completed = host_boundary.load_receipt(state/forward.operation_file(terminal.FORWARD_STATE, request=selected["new_request"]))
        assert completed["phase"] == "complete" and completed["receipt"]["retired"]
        return dict(actual_successor_host_route=True, actual_lookup_command=True,
            lookup_lost_commit_no_second_move=True, retained_predecessor_files_unchanged=True,
            successor_preparation_and_terminal=True, successor_final_handoff=False,
            actual_reschedule_sql_transport=True, reschedule_lost_commit_no_replay=True,
            reschedule_original_clocks_and_proof=True, rescheduled_worker_and_terminal=True,
            explicit_extension_transport_and_reentry=True, extension_preserved_start_and_progress=True,
            retained_raw_amendment_transport_and_reentry=True,
            package_correction_transport_reentry_and_unchanged_clocks=True), worker["container_id"]
    finally:
        operation.inspect_prepared_operation, terminal._probe = actual_preflight, actual_probe


def _rehearse_forward_reschedule(*, state, plan_path, selected, worker, version=1):
    """Actual confined SQL, uncertain reply, retained proof and worker reentry."""
    import os, time
    from datetime import datetime, timezone, timedelta
    from scripts.automation import storage_online_reschedule as reschedule
    from scripts.automation import storage_online_forward as forward
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_deadline as publication

    plan=operation.load_operation_plan(plan_path)
    old_launch=forward.load_launch(state,request=selected["new_request"])
    capture=old_launch["capture"]
    original={path:path.read_bytes() for path in (plan_path,state/forward.operation_file(
        forward.STATE,request=selected["new_request"]))}
    seconds = capture["attempt_seconds"]+(86400 if version == 2 else 0)
    expiry = datetime.fromisoformat(capture["started_at"])+timedelta(seconds=seconds)
    suffix = "" if version == 1 else "-v"+str(version)
    # Finite, idle synthetic fixture: these allowances are not production evidence.
    forecast=dict(schema_version="qt.storage_deadline_capacity.v1",plan_sha256=publication._sha(plan_path.read_bytes()),
        attempt_seconds=seconds,through_epoch=expiry.timestamp(),
        observed_at=time.time(),filesystems=[])
    for key in ("/app/logs/market-structure","/qt-history"):
        path=Path(worker["binding"]["mounts"][key]["host_source"]);space=os.statvfs(path)
        floor=(max(plan["limits"]["spool_reserve_bytes"],plan["limits"]["recent_free_bytes"])
            if key=="/app/logs/market-structure" else plan["limits"]["repository_reserve_bytes"])
        forecast["filesystems"].append(dict(path=str(path),device=path.stat().st_dev,
            reserve_bytes=max(floor,(space.f_blocks*space.f_frsize*plan["request"]["policy"]["reserve_percent"]+99)//100),
            remaining_peak_bytes={k:1024 for k in ("targets","growth","queue","wal","temporary","maintenance","recovery")},
            evidence_sha256=host_boundary.digest(dict(scope="idle disposable reschedule fixture",capture=capture))))
    capacity_file=state/("reschedule-capacity"+suffix+".json")
    host_boundary.save_receipt(capacity_file,forecast,initial=True)
    package=dict(schema_version="qt.storage_online_forward_reschedule_package.v1",
        plan_sha256=forecast["plan_sha256"],operation_sha256=selected["forward"]["operation_sha256"],
        image=plan["image"],source_revision=selected["new_request"]["source_revision"],
        source_tree_hash=selected["new_request"]["source_tree_hash"],
        end_day=(datetime.now(timezone.utc).date()+timedelta(days=1)).isoformat(),
        max_objects=selected["new_request"]["max_objects"]+16,descriptor_limit=plan["descriptor_limit"]+16,
        forward_plan_path=str(state/("rescheduled-operation"+suffix+".json")),capacity_file=str(capacity_file))
    if version == 2:
        previous_path = state/reschedule.state_file(package["operation_sha256"])
        original[previous_path] = previous_path.read_bytes()
        package.update(schema_version="qt.storage_online_forward_reschedule_package.v2",
            previous_amendment_sha256=publication._sha(original[previous_path]), attempt_seconds=seconds,
            raw_mapping_mode="retain_source", continue_guarded_proof=True,
            end_day=(datetime.fromisoformat(capture["expires_at"]).date()+timedelta(days=1)).isoformat())
    if version == 3:
        # A distinct immutable image of the same attested source exercises the
        # real replacement path; only a disposable fixture label is added.
        context = state/"package-image-context"
        context.mkdir(mode=0o700)
        tag = "qt-package-correction-fixture:"+host_boundary.digest(dict(path=str(state)))[:16]
        host_boundary.docker("tag", plan["image"], tag)
        image = host_boundary.docker("build", "--network", "none", "--pull=false", "--quiet",
            "--file", "-", str(context), input="FROM "+tag+"\nLABEL qt.storage.package-correction-fixture=true\n",
            timeout=120).strip().splitlines()[-1]
        previous_path = state/reschedule.state_file(package["operation_sha256"], version=2)
        original[previous_path] = previous_path.read_bytes()
        package.update(schema_version="qt.storage_online_forward_reschedule_package.v3",
            previous_amendment_sha256=publication._sha(original[previous_path]), attempt_seconds=seconds,
            end_day=selected["new_request"]["forward_reschedule"]["end_day"],
            max_objects=selected["new_request"]["max_objects"], descriptor_limit=plan["descriptor_limit"], image=image)
    package_file=state/("reschedule-package"+suffix+".json")
    host_boundary.save_receipt(package_file,package,initial=True)
    assert not operation.run_operation_plan(plan_path,reschedule_forward_file=package_file)["storage_mutations_performed"]
    actual_probe=reschedule._probe
    dispatches=[]
    apply_action = "replace_package" if version == 3 else "apply"
    def lose_commit(*args,**kwargs):
        result=actual_probe(*args,**kwargs)
        dispatches.append(kwargs["action"])
        if kwargs["action"]==apply_action:raise TimeoutError("reschedule COMMIT reply lost")
        return result
    reschedule._probe=lose_commit
    try:
        try:
            operation.run_operation_plan(plan_path,reschedule_forward_file=package_file,execute=True)
            raise AssertionError("missing reschedule lost reply")
        except TimeoutError as exc:assert str(exc)=="reschedule COMMIT reply lost"
        assert operation.run_operation_plan(plan_path,reschedule_forward_file=package_file,execute=True)["deadline_renewed"] is (version == 2)
    finally:reschedule._probe=actual_probe
    assert dispatches.count(apply_action)==1
    journal=host_boundary.load_receipt(state/reschedule.state_file(package["operation_sha256"],version=version),max_bytes=reschedule.MAX_BYTES)
    assert actual_probe(journal["new_plan"],worker,request=journal["new_request"],action="inspect")==reschedule._after_sql(journal)
    new_path=Path(package["forward_plan_path"])
    prepared=operation.run_operation_plan(new_path,prepare_forward_only=True,execute=True)
    assert prepared["phase"]=="forward_background_prepared" and prepared["adoption_active"]
    selected=forward.inspect_published_operation(state,operation_path=new_path)
    current=forward.load_launch(state,request=selected["new_request"])
    for key in ("started_at","started_monotonic","started_boot","key_deadline","key_deadline_monotonic",
            "key_deadline_boot"):
        assert current[key]==old_launch[key],key
    assert current["deadline"] == expiry.timestamp()
    assert current["capture"] == {**capture,"attempt_seconds":seconds,"expires_at":expiry.isoformat()}
    assert all(path.read_bytes()==value for path,value in original.items())
    return new_path,selected,host_boundary.load_receipt(state/launch._STATE)


def _rehearse_forward_final(*, state, arguments, created, launch_intent, plan_path, mode):
    """Real final owners; synthetic guarded source/runtime peers and old-day rows."""
    import time
    from copy import deepcopy
    from scripts.automation import storage_online_final as final
    from scripts.automation import storage_online_forward as forward
    from scripts.automation import storage_online_launch as launch
    from scripts.automation import storage_online_operation as operation
    from scripts.automation import storage_online_prepare as initial
    assert mode in {"commit", "rollback"}
    plan = operation.load_operation_plan(plan_path)
    assert plan["limits"]["final_seconds"] == 60
    request = forward.inspect_published_operation(state, operation_path=plan_path)["new_request"]
    phases = {}
    preflights = []
    def preflight(root, **kw):
        launch.inspect_candidate_image(kw["image"], kw["request"])
        preflights.append(kw.get("operator_id"))
        return initial.admit_serving_source(root, project=arguments["project"],
            source_revision=arguments["source_revision"], operator_id=kw.get("operator_id", created))
    actual_preflight = operation.inspect_prepared_operation
    actual_gate = final.close_database_logins_locked
    actual_commit = final.commit_online_handoff_locked
    actual_recovery = final.prepare_recovery_database_locked
    def gate(root, *, exchange):
        phases["pause"] = final._load(root/final.STATE)
        result = actual_gate(root, exchange=exchange)
        saved = final._load(root/final.STATE)
        assert saved["phase"] == "login_closed" and result["new_logins_closed"]
        assert saved["deadline"] == phases["pause"]["deadline"]
        assert saved["deadline_boot"] == phases["pause"]["deadline_boot"]
        with host_boundary.docker_deadline(saved["switch"]["deadline_monotonic"]):
            rows = host_boundary.inventory(arguments["project"], operator_id=created)
            observed = json.loads(host_boundary.maintenance_query(rows["tsdb"]["id"], final._GATE_OBSERVE))
            assert observed == {**saved["login_gate"]["database"], "allow_connections":False}
            try:
                host_boundary.database_query(rows["tsdb"]["id"], "SELECT 1")
                raise AssertionError("closed target accepted new login")
            except RuntimeError as exc:
                assert str(exc).startswith("storage_pause_docker_failed")
        phases["gate"] = saved
        return result
    def commit(root, *, exchange):
        calls = []
        def uncertain(operation_name, **kw):
            reply = exchange(operation_name, **kw)
            calls.append(operation_name)
            if operation_name == "commit_database":
                # Actual SQL committed; remove acknowledgement certainty only.
                # The unchanged host must inspect the durable SQL certificate.
                assert reply["state"] == "committed"
                reply = deepcopy(reply)
                reply["state"] = "commit_unknown"
                reply["result"]["database_handoff_committed"] = None
                reply["result"]["initial_policy_activated"] = None
            return reply
        result = actual_commit(root, exchange=uncertain)
        assert calls.count("commit_database") == 1 and calls[-1] == "inspect_outcome"
        assert result["outcome"] == "committed" and result["initial_policy_activated"]
        phases["commit"] = result
        return result
    def before_recovery(root, *, worker_process, **kw):
        assert worker_process.poll() is not None
        status = json.loads(host_boundary.docker("inspect", "--format", "{{json .State}}", created))
        assert not status["Running"] and status["Pid"] == 0
        saved = final._load(root/final.STATE)
        assert saved["phase"] == "committed"
        assert saved["deadline"] == phases["pause"]["deadline"] and saved["deadline_boot"] == phases["pause"]["deadline_boot"]
        phases["reader_retired"] = True
        raise RuntimeError("fixture stopped before recovery mount transition")
    operation.inspect_prepared_operation = preflight
    final.close_database_logins_locked = gate
    final.commit_online_handoff_locked = commit
    final.prepare_recovery_database_locked = before_recovery
    try:
        if mode == "commit":
            try:
                operation.run_operation_plan(plan_path, execute=True)
                raise AssertionError("fixture crossed recovery boundary")
            except RuntimeError as exc:
                assert str(exc) == "fixture stopped before recovery mount transition"
            assert preflights == [None, None, created] and phases["reader_retired"]
        else:
            with host_boundary.deployment_lock(state):
                with launch.launched_online_worker_locked(state, **arguments) as (worker, receipt):
                    assert receipt["container_id"] == created
                    channel = host_boundary.OnlineWorkerChannel(worker, deadline=time.monotonic()+receipt["deadline"]-time.time())
                    exchange = channel.exchange
                    operation.prepare_background(exchange, preparation_seconds=30, forward=True)
                    paused = final.stop_online_source_locked(state, project=arguments["project"],
                        source_revision=arguments["source_revision"], controller_id=channel.greeting["controller_id"],
                        worker_id=created, max_duration_seconds=60)
                    with final.held_source_writers_locked(state, source_image=plan["source_image"]):
                        final.observe_source_drain_locked(state, exchange=exchange, max_entries=plan["limits"]["spool_max_entries"])
                        operation._enter_forward_day(request["forward"], paused)
                        deadline = time.monotonic()+final._remaining(paused)-0.1
                        final.copy_final_delta_locked(state, exchange=exchange, deadline=deadline, max_rounds=64)
                        final.record_switch_entry_locked(state, deadline=deadline,
                            observe_worker=lambda **kw:exchange("status",response_deadline=kw["deadline"]))
                        gate(state, exchange=exchange)
                        final.copy_final_delta_locked(state, exchange=exchange, deadline=deadline, max_rounds=64)
                    # Release the source namespace hold while logins are still
                    # closed, then the existing live SQL fence owns all starts.
                    final.resume_online_source_locked(state, exchange=exchange)
                    saved = final._load(state/final.STATE)
                    assert saved["phase"] == "source_resumed"
                    assert saved["deadline"] == paused["deadline"] and saved["deadline_boot"] == paused["deadline_boot"]
                    rows = host_boundary.inventory(arguments["project"], operator_id=created)
                    assert host_boundary.source_clients_serving(rows)
                    phases["rollback"] = True
                    exchange("close")
                assert worker.poll() is not None
                phases["reader_retired"] = True
    finally:
        operation.inspect_prepared_operation = actual_preflight
        final.close_database_logins_locked = actual_gate
        final.commit_online_handoff_locked = actual_commit
        final.prepare_recovery_database_locked = actual_recovery
    ready = forward.load_launch(state)
    assert ready["pending_worker"] is None and ready["worker"]["container_id"] == created
    for key in ("started_at", "started_monotonic", "started_boot", "key_deadline", "key_deadline_monotonic", "key_deadline_boot"):
        assert ready[key] == launch_intent[key]
    saved = final._load(state/final.STATE)
    elapsed = time.time()-saved["started_at"]
    assert elapsed < saved["duration_seconds"] == 60
    return dict(forward_worker_started=True, interrupted_launch_reused_created_worker=True,
        actual_confined_entrypoint=True, actual_host_adoption_observation=True,
        original_launch_and_sql_clocks_preserved=True, actual_forward_final_mode=mode,
        canonical_normal_dispatch=mode=="commit", login_closure=True, source_kernel_hold=True,
        gated_rollback=mode=="rollback", exact_commit_reconciled=mode=="commit",
        uncertain_worker_acknowledgement=mode=="commit", reader_retired=True,
        original_final_clocks_preserved=True, measured_fixture_final_seconds=elapsed,
        production_pause_qualified=False, recovery_mount_transition=False)
