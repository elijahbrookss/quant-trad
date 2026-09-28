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
