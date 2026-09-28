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
