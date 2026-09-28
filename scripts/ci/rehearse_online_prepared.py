"""Owned prepared-host launcher rehearsal with real QT publication.

Synthetic service peers; optional owned initial pause and held database switch.
No production inputs. Optional modes qualify owned runtime/recovery and deployment.
All created containers, volume, network and scratch files are owned by this run.
"""
import argparse,json,os,select,subprocess,sys,time,uuid
from contextlib import ExitStack
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"src"))
from scripts.automation import storage_host_boundary as host_boundary
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--image',required=True)
parser.add_argument('--database-image',required=True)
parser.add_argument('--output-root',type=Path,required=True)
parser.add_argument('--history-parent',type=Path,required=True)
parser.add_argument('--require-distinct-devices',action='store_true')
parser.add_argument('--prepare-source',action='store_true',help='qualify retained initial preparation before atomic capture and launch')
parser.add_argument('--initial-capture',action='store_true',help='create placement and capture in the real confined worker while source serves')
parser.add_argument('--final-pause',action='store_true',help='qualify interrupted final source stop only; no switch or resumption')
parser.add_argument('--operation-driver',action='store_true',help='qualify the fixed prepared-operation driver through real runtime readiness')
parser.add_argument('--full-operation',action='store_true',help='run the public operator before initial preparation with a retired seed fixture')
parser.add_argument('--worker-phases',action='store_true',help='drive explicit preparation through the launched worker pipe')
parser.add_argument('--worker-shutdown',choices=('clean','fail'),help='actual Docker worker/supervisor signal with controlled adapter; requires final pause')
parser.add_argument('--final-delta',action='store_true',help='bounded held worker tail catch-up, no switch')
parser.add_argument('--switch-entry',action='store_true',help='persist uncertain switch entry, no COMMIT dispatch')
parser.add_argument('--real-worker-publication',action='store_true',help='real collector publication during Docker shutdown with scripted transport')
parser.add_argument('--abort-resume',action='store_true',help='resume exact original clients under the retained live rollback fence')
parser.add_argument('--abort-resume-fence-loss',action='store_true',help='kill owned SQL fence while a real Docker start HTTP reply is withheld')
parser.add_argument('--abort-resume-late-start',action='store_true',help='forward one proxy-queued owned start after SQL fence loss and real CLI reaping')
parser.add_argument('--abort-resume-lost-end',action='store_true',help='discard a fully received terminal fence reply and reconcile without more starts')
parser.add_argument('--worker-attach-loss',action='store_true',help='stop the owned worker Python process and kill its attach CLI before bounded retirement')
parser.add_argument('--close-logins',choices=('success','lost-reply'),help='close owned target logins under durable final intent; no COMMIT or production reopen')
parser.add_argument('--abort-restore-lost-reply',action='store_true',help='discard completed owned login restoration reply; preserve unresolved journal')
parser.add_argument('--commit-switch',action='store_true',help='one-shot real host-to-worker COMMIT and verified worker retirement; no runtime activation')
parser.add_argument('--guarded-source-image',help='owned synthetic source image with the real source lifetime guard')
parser.add_argument('--recovery-mounts',action='store_true',help='preserving already-HDD database recovery mounts after committed reader retirement; no runtime start')
parser.add_argument('--recovery-create-reply-loss',action='store_true',help='discard completed owned recovery database create reply, retaining unresolved intent')
parser.add_argument('--recovery-repositories',action='store_true',help='continue held committed operation through actual encrypted repositories and native WAL delivery')
parser.add_argument('--recovery-repository-reply-loss',action='store_true',help='discard completed preparer reply and retain unresolved repository intent')
parser.add_argument('--recovery-spool',action='store_true',help='prepare preserved pending WAL in a new private SSD root after native WAL readiness')
parser.add_argument('--recovery-spool-reply-loss',action='store_true',help='discard actual completed spool-copy response; retain unresolved intent')
parser.add_argument("--recovery-runtime",action="store_true",help="start actual split application composition after committed recovery preparation")
parser.add_argument('--completion-observation',action='store_true',help='inspect actual paired recovery after the original final window expires, without replay')
parser.add_argument('--canonical-deployment-repository',type=Path,help='complete owned operation into public recipe and existing deployer')
options=parser.parse_args()
if options.canonical_deployment_repository and not options.completion_observation:
 parser.error('--canonical-deployment-repository requires --completion-observation')
if options.completion_observation and not options.full_operation:
 parser.error('--completion-observation requires --full-operation')
if options.full_operation and not options.operation_driver:
 parser.error('--full-operation requires --operation-driver')
if options.operation_driver and not (options.recovery_runtime and options.initial_capture):
 parser.error('--operation-driver requires --recovery-runtime --initial-capture')
if options.initial_capture and not (options.prepare_source and options.worker_phases):
 parser.error("--initial-capture requires --prepare-source --worker-phases")
if options.recovery_runtime and (not options.recovery_spool or options.recovery_spool_reply_loss):
 parser.error("--recovery-runtime requires successful --recovery-spool")
if options.recovery_spool and (not options.recovery_repositories or options.recovery_repository_reply_loss):
 parser.error('--recovery-spool requires successful --recovery-repositories')
if options.recovery_spool_reply_loss and not options.recovery_spool:
 parser.error('--recovery-spool-reply-loss requires --recovery-spool')
if options.recovery_repository_reply_loss and not options.recovery_repositories:
 parser.error('--recovery-repository-reply-loss requires --recovery-repositories')
if options.recovery_repositories and (not options.recovery_mounts or options.recovery_create_reply_loss):
 parser.error('--recovery-repositories requires successful --recovery-mounts')
if options.recovery_create_reply_loss and not options.recovery_mounts:
 parser.error('--recovery-create-reply-loss requires --recovery-mounts')
if options.recovery_mounts and not options.commit_switch:
 parser.error('--recovery-mounts requires --commit-switch')
if options.commit_switch and (options.close_logins!='success' or not options.guarded_source_image
    or options.abort_resume or options.worker_shutdown or options.worker_attach_loss):
 parser.error('--commit-switch requires confirmed gate and guarded fixture image, without abort/signal/attach faults')
if options.guarded_source_image and not options.commit_switch:
 parser.error('--guarded-source-image is only admitted with --commit-switch')
if options.abort_restore_lost_reply and (not options.abort_resume or options.close_logins!='success' or options.abort_resume_fence_loss or options.abort_resume_lost_end):
 parser.error('--abort-restore-lost-reply requires confirmed gated abort without other faults')
if options.close_logins and (not options.switch_entry or options.abort_resume and options.close_logins!='success'):
 parser.error('--close-logins requires switch entry; gated abort requires confirmed success')
if options.worker_attach_loss and (options.final_pause or not options.worker_phases):
 parser.error('--worker-attach-loss requires --worker-phases and excludes final pause')
if options.abort_resume_late_start and not options.abort_resume_fence_loss:
 parser.error('--abort-resume-late-start requires --abort-resume-fence-loss')
if options.abort_resume_lost_end and (not options.abort_resume or options.abort_resume_fence_loss):
 parser.error('--abort-resume-lost-end requires --abort-resume and excludes fence-loss fault')
if options.abort_resume_fence_loss and not options.abort_resume:
 parser.error('--abort-resume-fence-loss requires --abort-resume')
if options.abort_resume and not options.switch_entry:
 parser.error('--abort-resume requires --switch-entry')
if options.real_worker_publication and options.worker_shutdown!='clean':
 parser.error('--real-worker-publication requires --worker-shutdown clean')
if options.switch_entry and not options.final_delta:
 parser.error('--switch-entry requires --final-delta')
if options.final_delta and (not options.final_pause or options.worker_shutdown=='fail'):
 parser.error('--final-delta requires successful --final-pause')
if options.worker_shutdown and not options.final_pause:
 parser.error('--worker-shutdown requires --final-pause')
if options.final_pause and not (options.worker_phases and options.prepare_source):
 parser.error('--final-pause requires --prepare-source --worker-phases')
if options.worker_phases and not options.prepare_source:
 parser.error('--worker-phases requires --prepare-source')
ROOT=options.output_root.resolve(strict=True)
history_parent=options.history_parent.resolve(strict=True)
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_prepare as initial
project='qt-online-host-'+uuid.uuid4().hex[:12]
state=ROOT/project;state.mkdir(mode=0o700)
history=history_parent/project
network=project+'_quanttrad'
source_holds=ExitStack()
created_network=False
created_history=False
canonical=None
owned=[];volume=project+'-pg';created_volume=False
recovery_socket=project+'-recovery-socket';created_recovery_socket=False
started=time.monotonic()
report={'project':project,'production_inputs':False,'full_host_pause_qualified':False,
        'declared_final_fixture_seconds':120 if options.recovery_runtime else 60,
        'real_collector_performance_qualified':False,'synthetic_service_peers':True}
def run(args,timeout=120,check=True,env=None):
 if args[0] in ('run','create'):
  args=[args[0],'--label','qt.disposable='+project,*args[1:]]
 r=subprocess.run(['docker',*args],text=True,capture_output=True,timeout=timeout,env=env)
 if check and r.returncode:raise RuntimeError('entrypoint_fixture_command_failed: '+r.stderr[-1800:])
 return r
try:
 if options.require_distinct_devices:
  assert history_parent.stat().st_dev != ROOT.stat().st_dev
 assert os.statvfs(ROOT).f_bavail*os.statvfs(ROOT).f_frsize > 5*1024**3
 assert os.statvfs(history_parent).f_bavail*os.statvfs(history_parent).f_frsize > 5*1024**3
 report['distinct_drive_roots']=history_parent.stat().st_dev != ROOT.stat().st_dev
 history.mkdir();created_history=True
 working=state/'working';working.mkdir();control=state/'control';control.mkdir();control.chmod(0o777)
 if options.recovery_spool:
  candidate_working=state/'candidate-working';candidate_working.mkdir(mode=0o700)
 if options.recovery_mounts:
  recovery_keys=state/'fixture-recovery-keys';recovery_keys.mkdir(mode=0o700)
  (recovery_keys/'disposable-marker').write_text('disposable fixture only')
  (recovery_keys/'disposable-marker').chmod(0o600)
  run(['volume','create','--label','qt.disposable='+project,recovery_socket]);created_recovery_socket=True
 image=run(['image','inspect',options.image,'--format','{{.Id}}']).stdout.strip()
 envvalues=dict(v.split('=',1) for v in json.loads(run(['image','inspect',image,'--format','{{json .Config.Env}}']).stdout))
 revision=envvalues['QT_IMAGE_SOURCE_REVISION']
 if options.recovery_repositories:
  # Independent generated keys only in this owned disposable fixture.
  fixture_setup = """import json,os,secrets;from pathlib import Path
root=Path('/keys')
for name in ('database.key','archive.key'):
 p=root/name;p.write_text(secrets.token_hex(32));p.chmod(0o600);os.chown(p,70,70)
config=dict(pgbackrest='/usr/local/bin/pgbackrest',restic='/usr/local/bin/restic',pg_path='/var/lib/postgresql/data',pg_socket_path='/var/run/postgresql',database_key_path='/run/quanttrad/recovery/database.key',archive_key_path='/run/quanttrad/recovery/archive.key',max_chain_backups=4)
p=root/'incremental-config.json';p.write_text(json.dumps(config));p.chmod(0o600);os.chown(p,70,70)
os.chown(root,70,70)
"""
  run(['run','--rm','--network','none','--user','0:0','--cap-drop','ALL','--cap-add','CHOWN','--cap-add','DAC_OVERRIDE','--cap-add','FOWNER',
      '--mount','type=bind,source='+str(recovery_keys)+',target=/keys','--entrypoint','python',image,'-c',fixture_setup])

 source_image=image
 if options.guarded_source_image:
  source_image=run(['image','inspect',options.guarded_source_image,'--format','{{.Id}}']).stdout.strip()
  source_config=json.loads(run(['image','inspect',source_image,'--format','{{json .Config}}']).stdout)
  assert source_config.get('Labels',{}).get('qt.guarded-source-fixture')=='true'
  assert dict(v.split('=',1) for v in source_config['Env'])['QT_IMAGE_SOURCE_REVISION']==revision
  report['guarded_source_fixture']=dict(image=source_image,application_runtime_qualified=False)
 (state/'release.env').write_text('current_revision='+revision+'\ncurrent_source_tree_hash='+envvalues['QT_IMAGE_SOURCE_TREE_HASH']+'\nprevious_revision=\ndeployed_at=fixture\nstorage_layout=\n')
 (state/'release.env').chmod(0o600)
 def labels(service):
  return ['--label','com.docker.compose.project='+project,'--label','com.docker.compose.service='+service,'--label','com.docker.compose.oneoff=False']
 pg=run(['image','inspect',options.database_image,'--format','{{.Id}}']).stdout.strip()
 prep=project+'-prepare';owned.append(prep)
 run(['run','--name',prep,'--user','0:0','--network','none','--memory','64m','--cpus','0.25',
      '--mount','type=bind,source='+str(history)+',target=/h',
      '--mount','type=bind,source='+str(working)+',target=/s','--entrypoint','python',image,'-c',
      "import os;os.mkdir('/s/objects');[(os.chown(p,70,70),os.chmod(p,0o755)) for p in ('/h','/s','/s/objects')];os.chmod('/h',0o777)"+
      (";os.mkdir('/h/archives');os.chown('/h/archives',1000,70);os.chmod('/h/archives',0o2770)" if options.canonical_deployment_repository else "")])
 assert run(['volume','inspect',volume],check=False).returncode != 0
 run(['volume','create','--label','qt.disposable='+project,volume]);created_volume=True
 run(['network','create','--internal',network]);created_network=True
 pgname=project+'-db';owned.append(pgname)
 password=uuid.uuid4().hex
 dbname='qt_migration_online_'+uuid.uuid4().hex[:16]
 if options.canonical_deployment_repository:
  from scripts.ci.online_operation_fixture import prepare_canonical_configuration
  canonical=prepare_canonical_configuration(repository=options.canonical_deployment_repository,
    state=state,project=project,image=image,database_image=pg,revision=revision,
    source_hash=envvalues['QT_IMAGE_SOURCE_TREE_HASH'],password=password,dbname=dbname,
    history=history,candidate_working=candidate_working,volume=volume,network=network,
    recovery_keys=recovery_keys,recovery_socket=recovery_socket)
 history_uuid=canonical['history_uuid'] if canonical else 'uuid-copy-hdd'
 if options.prepare_source:
  pgname=project+'-tsdb-1';owned.append(pgname)
  service=dict(image=pg,pull_policy='never',hostname='tsdb.quanttrad',
    command=['postgres','-c','shared_buffers=32MB','-c','timescaledb.telemetry_level=off'],
    environment=dict(POSTGRES_USER='fixture',POSTGRES_DB=dbname,POSTGRES_PASSWORD=password,PGDATA='/var/lib/postgresql/data'),
    init=True,restart='no',shm_size=134217728,labels={'qt.disposable':project},
    healthcheck=dict(test=initial.held._TCP_PROBE,interval='1s',timeout='2s',retries=90,start_period='10s'),
    volumes=[dict(type='volume',source='postgres-data',target='/var/lib/postgresql/data')],
    networks={'quanttrad':{'aliases':['tsdb.quanttrad']}})
  model=dict(name=project,services={'tsdb':service},
    volumes={'postgres-data':dict(name=volume,external=True)},
    networks={'quanttrad':dict(name=network,external=True)})
  if canonical:model=canonical['database']
  source_recipe=state/'source.compose.json';source_recipe.write_text(json.dumps(model));source_recipe.chmod(0o600)
  recipe=json.loads(json.dumps(model))
  recipe['services']['tsdb']['volumes'].append(dict(type='bind',source=str(history),target='/qt-history',bind=dict(create_host_path=False)))
  target_recipe=state/initial.held.DATABASE_RECIPE;target_recipe.write_text(json.dumps(recipe));target_recipe.chmod(0o600)
  run(['compose','--project-name',project,'--file',str(source_recipe),'up','-d','--no-build','--pull','never','--wait','--wait-timeout','90'])
 else:
  run(['run','-d','--name',pgname,'--network',network,'--network-alias','tsdb','--hostname','tsdb.quanttrad',*labels('tsdb'),'--memory','256m','--cpus','2',
       '--mount','type=volume,source='+volume+',target=/var/lib/postgresql/data',
       '--mount','type=bind,source='+str(history)+',target=/qt-history',
       '--env','POSTGRES_USER=fixture','--env','POSTGRES_DB='+dbname,'--env','POSTGRES_PASSWORD='+password,
       '--env','PGDATA=/var/lib/postgresql/data',pg,'postgres','-c','shared_buffers=32MB','-c','timescaledb.telemetry_level=off'])
 pgid=run(['inspect',pgname,'--format','{{.Id}}']).stdout.strip()
 until=time.monotonic()+90
 while run(['exec',pgid,'pg_isready','-h','127.0.0.1','-U','fixture'],check=False).returncode:
  if time.monotonic()>until:raise RuntimeError('fixture_db_timeout')
  time.sleep(.2)
 for service in host_boundary.STOP+host_boundary.PASSIVE:
  if service=='tsdb' or (service=='market-data-collector' and not options.prepare_source):continue
  name=project+'-'+service;owned.append(name)
  extra=[]
  command_text='exit 0' if service=='initialize' else "trap 'exit 0' TERM; while :; do sleep 1 & wait $!; done"
  if service=='market-data-collector':
   extra=['--mount','type=bind,source='+str(working)+',target=/app/logs/market-structure','--env','PG_DSN=postgresql+psycopg2://fixture:'+password+'@tsdb:5432/'+dbname]
   command_text="trap 'exit 0' TERM; while :; do printf x >> /app/logs/market-structure/objects/native-intake; sleep 1 & wait $!; done"
  from scripts.automation.storage_online_final import _SOURCE_WRITERS
  if options.commit_switch and service in _SOURCE_WRITERS:
   target='/app/logs/market-structure'
   canonical_mounts=[]
   if canonical:
    canonical_mounts=['--mount','type=bind,source='+str(canonical['environment'])+',target=/app/secrets.env,readonly']
    if service=='backend':canonical_mounts+=['--mount','type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock']
   run(['run','-d','--name',name,'--pull','never','--network',network,
     '--user','70:70','--read-only','--memory','128m','--cpus','0.25','--pids-limit','32',
     *labels(service),*canonical_mounts,'--mount','type=bind,source='+str(working)+',target='+target,
     '--env','PG_DSN=postgresql+psycopg2://fixture:'+password+'@tsdb:5432/'+dbname,
     '--env','QT_ONLINE_GUARDED_SOURCE_FIXTURE=1','--env','QT_DISABLE_DOTENV=1',
     '--env','QT_STORAGE_SOURCE_FENCE_ROOT='+target,'--env','MARKET_STRUCTURE_STORAGE_ROOT='+target,
     '--entrypoint','python',source_image,'-m',_SOURCE_WRITERS[service]])
   continue
  if service=='market-data-collector' and options.worker_shutdown:
   import ast
   fixture_tree=ast.parse((Path(__file__).resolve().parents[2]/"tests/test_market_data/test_collector_shutdown_signal.py").read_text())
   SCRIPT=next(ast.literal_eval(node.value) for node in fixture_tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="SCRIPT" for t in node.targets))
   extra += ['--env','QT_SIGNAL_HOST_FIXTURE=1','--env','QT_ONLINE_RUNTIME_FIXTURE='+str(int(options.recovery_runtime)),'--env','QT_ONLINE_INITIAL_CAPTURE='+str(int(options.initial_capture)),'--env','QT_SIGNAL_REAL_PUBLICATION='+str(int(options.real_worker_publication)),'--env','QT_DISABLE_DOTENV=1',
             '--env','QT_LOGGING_LOKI_URL=','--env','QT_LOGGING_LEVEL=INFO']
   run(['run','-d','--name',name,'--pull','never','--network',network,
     '--user','70:70','--read-only','--memory','512m','--cpus','2','--pids-limit','64',
     *labels(service),*extra,'--entrypoint','python',image,'-c',SCRIPT,
     '/app/logs/market-structure/objects','clean'])
   ready_deadline=time.monotonic()+30
   while not (working/'objects'/'ready').exists():
    if time.monotonic()>ready_deadline:raise RuntimeError('owned worker readiness timeout')
    time.sleep(.05)
  else:
   run(['run','-d','--name',name,'--pull','never','--network',network,
     '--user','70:70','--read-only','--memory','32m','--cpus','0.1',
     '--pids-limit','32',*labels(service),*extra,'--entrypoint','sh',image,'-c',command_text])
 if options.commit_switch:
  source_ready=time.monotonic()+15
  while (not (working/'objects'/'native-intake').exists()
         or run(['inspect',project+'-initialize','--format','{{.State.Status}}']).stdout.strip()!='exited'):
   if time.monotonic()>source_ready:raise RuntimeError('owned_guarded_source_start_timeout')
   time.sleep(.05)
 if options.prepare_source:
  udev=state/'initial-udev';udev.mkdir()
  device=history.stat().st_dev
  (udev/f'b{os.major(device)}:{os.minor(device)}').write_text('E:ID_FS_UUID=uuid-copy-hdd\n')
  if canonical:udev=Path('/run/udev/data')
  os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
  original_cluster=host_boundary.cluster_identifier(pgid)
  if not options.full_operation:
   preparation=initial.prepare_online_source(state,project=project,source_revision=revision,history_uuid=history_uuid)
   prepared_bytes=(state/initial.STATE).read_bytes()
   assert preparation['phase']=='serving' and preparation['deadline']-preparation['started_at']==600
   assert initial.prepare_online_source(state,project=project,source_revision=revision,history_uuid=history_uuid)==preparation
   pgid=host_boundary.inventory(project)['tsdb']['id']
   assert host_boundary.cluster_identifier(pgid)==original_cluster
   assert not host_boundary.inventory(project)['initialize']['running']
   intake_before=(working/'objects'/'native-intake').stat().st_size
   report['initial_preparation_seconds']=preparation['completed_at']-preparation['started_at']
   report['initial_preparation_receipt_retained']=True
 test=project+'-collector';owned.append(test)
 args=['docker','run','--name',test,'--label','qt.disposable='+project,'--user','70:70','--network','container:'+pgid,'--pid','container:'+pgid,
   *(['--label','com.docker.compose.project='+project+'-application','--label','com.docker.compose.service=fixture'] if options.prepare_source else labels('market-data-collector')),'--memory','2g','--cpus','2','--mount','type=volume,source='+volume+',target=/var/lib/postgresql/data',
   '--mount','type=bind,source='+str(history)+',target=/qt-history',
   '--mount','type=bind,source='+str(working)+',target=/app/logs/market-structure',
   '--mount','type=bind,source='+str(control)+',target=/qt-control',
   '--env','PG_DSN','--env','QT_DISABLE_DOTENV=1','--env','QT_LOGGING_LOKI_URL=','--env','QT_ONLINE_RUNTIME_FIXTURE='+str(int(options.recovery_runtime)),
   '--env','QT_STORAGE_DEMO=1','--env','QT_DB_TEST_ISOLATED=1','--env','RUN_DB_TESTS=1',
   '--env','QT_ONLINE_FULL_OPERATION='+str(int(options.full_operation)),
   '--env','QT_ONLINE_INITIAL_CAPTURE='+str(int(options.initial_capture)),'--env','QT_SIGNAL_REAL_PUBLICATION='+str(int(options.real_worker_publication)),'--env','QT_ONLINE_FINAL_DELTA='+str(int(options.final_delta)),'--env','QT_ONLINE_WORKER_PHASES='+str(int(options.worker_phases)),'--env','QT_ONLINE_ATOMIC_PREPARE='+str(int(options.prepare_source)),'--env','QT_ONLINE_HOST_FIXTURE=1','--env','QT_ONLINE_ENTRYPOINT_FIXTURE=1','--entrypoint','python',image,'-m','pytest','-q','-s',
   '--basetemp','/qt-control/testtmp','-o','cache_dir=/tmp/qt-entry-pytest',
   'tests/test_market_data/test_storage_online_entrypoint_db.py']
 if canonical:
  position=args.index('--entrypoint')
  args[position:position]=['--env','QT_ONLINE_CANONICAL_FIXTURE=1',
    '--mount','type=bind,source=/run/udev/data,target=/run/qt-host-udev/data,readonly',
    '--mount','type=bind,source='+str(Path(__file__).resolve().parents[2]/'tests/test_market_data/test_storage_online_entrypoint_db.py')+',target=/app/tests/test_market_data/test_storage_online_entrypoint_db.py,readonly']
 log=(state/'fixture.log').open('w')
 fixture=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT,env={**os.environ,'PG_DSN':'postgresql+psycopg2://fixture:'+password+'@tsdb:5432/'+dbname})
 until=time.monotonic()+180
 def waitfile(name):
  while not (control/name).exists():
   if fixture.poll() is not None or time.monotonic()>until:raise RuntimeError('fixture_wait_failed: '+name+' '+(state/'fixture.log').read_text()[-2500:])
   time.sleep(.1)
 waitfile('ready.json')
 if options.real_worker_publication:
  received_deadline=time.monotonic()+30
  while not (working/'objects'/'real-frame-received').exists():
   if time.monotonic()>received_deadline:raise RuntimeError('real Docker collector did not receive frame')
   time.sleep(.05)
 request=json.loads((control/'request.json').read_text())
 if options.initial_capture:
  if not options.full_operation:
   request['capture_preparation'].update(requested_at=time.time(),deadline=preparation['deadline'])
  assert launch._capture_observation(pgid) is None
  report['capture_absent_before_owned_worker']=True
 inventory=state/'inventory.json';inventory.write_bytes((control/'inventory.json').read_bytes());inventory.chmod(0o644)
 udev=Path('/run/udev/data') if canonical else control/Path(json.loads((control/'ready.json').read_text())['udev']).relative_to('/qt-control')
 os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
 source=host_boundary.identities(host_boundary.inventory(project))
 original_source_metadata=[working.stat().st_uid,working.stat().st_gid,working.stat().st_mode]
 kwargs=dict(project=project,source_revision=revision,image=image,request=request,
             inventory_path=inventory,descriptor_limit=1024,memory_bytes=1024**3)
 if options.operation_driver:
  from scripts.automation import storage_online_operation as operation
  from scripts.automation import storage_online_final as final_host
  from scripts.automation import storage_online_recovery as recovery_host
  from scripts.ci.online_operation_fixture import write_runtime_recipe
  owned.extend([project+'-storage-online',project+'-storage-repository-prepare',project+'-storage-spool-prepare'])
  # Reviewed fixture configuration is prepared BEFORE any driver dispatch.
  base=host_boundary.load_receipt(state/initial.held.DATABASE_RECIPE)
  recovery_model=recovery_host.database_recipe(base,keys_root=recovery_keys,socket_volume=recovery_socket,history=history)
  if canonical:
   from scripts.automation import storage_online_runtime as runtime_host
   from scripts.automation.storage_online_release import _service_definition
   assert _service_definition(canonical['runtime']['services']['tsdb'],image_environment={})==_service_definition(recovery_model['services']['tsdb'],image_environment={})
   canonical['runtime']['services']['tsdb']=recovery_model['services']['tsdb']
   assert canonical['runtime']['volumes']==recovery_model['volumes']
   host_boundary.save_receipt(state/runtime_host.RUNTIME_RECIPE,canonical['runtime'],initial=True)
   owned.extend(project+'-'+name+'-1' for name in runtime_host._APPLICATIONS)
  else:
   write_runtime_recipe(state=state,runtime_model=recovery_model,inventory=inventory,udev=udev,
     image=image,password=password,dbname=dbname,history=history,project=project,
     candidate_working=candidate_working,owned=owned)
  # Exercise prepared-operation preflight with an actual retired owned worker.
  # Capture is created once and keeps its original clock across driver reentry.
  if not options.full_operation:
   with launch.launched_online_worker(state,**kwargs) as (first_worker,first_receipt):
    first_channel=host_boundary.OnlineWorkerChannel(first_worker,
      deadline=time.monotonic()+first_receipt['deadline']-time.time())
    assert first_channel.greeting['state']=='background'
    original_capture=host_boundary.load_receipt(state/launch._STATE)['capture']
    original_capture_deadline=first_receipt['deadline']
  background=operation.prepare_background
  frozen_sql="SELECT jsonb_build_object('datasets',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM market.datasets t),'dataset_series',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM market.dataset_series t))::text"
  before_final_frozen=host_boundary.database_query(pgid,frozen_sql)
  def publish_during_background(*args,**kw):
   (control/'publish').write_text('publish')
   result=background(*args,**kw)
   (control/'phases-finished').write_text('finished');waitfile('catalogs-verified')
   (control/'final-publish').write_text('publish');waitfile('final-published')
   (control/'finished').write_text('finished');fixture.wait(timeout=30)
   assert fixture.returncode==0
   assert not json.loads(run(['inspect',test,'--format','{{json .State}}']).stdout)['Running']
   return result
  if options.full_operation:
   fixture.wait(timeout=30)
   assert fixture.returncode==0
   assert not json.loads(run(['inspect',test,'--format','{{json .State}}']).stdout)['Running']
  else:
   operation.prepare_background=publish_during_background
  actual_action=host_boundary.supervised_source_action
  def recover_before_maintenance(args,**kw):
   saved=final_host._load(state/final_host.STATE)
   if saved.get('runtime',{}).get('inflight')=='start:storage-maintenance':
    recovery_spec=json.loads((control/'runtime-recovery.json').read_text())
    code="import json,sys;from tests.test_market_data.online_runtime_recovery_fixture import verify;print('QT_CONNECTED_RECOVERY='+json.dumps(verify(json.loads(sys.argv[1]))))"
    recovered=run(['exec',saved['runtime']['candidate_ids']['market-data-collector'],
      'python','-c',code,json.dumps(recovery_spec)],timeout=kw['deadline']-time.monotonic(),check=False)
    (state/'runtime-recovery.log').write_text(recovered.stdout+recovered.stderr)
    assert recovered.returncode==0, 'ordinary connected recovery failed; see runtime-recovery.log'
    replies=[json.loads(line.split('=',1)[1]) for line in recovered.stdout.splitlines() if line.startswith('QT_CONNECTED_RECOVERY=')]
    assert len(replies)==1;report['normal_runtime_recovery']=replies[0]
   return actual_action(args,**kw)
  host_boundary.supervised_source_action=recover_before_maintenance
  try:
   limits=operation.OperationLimits(preparation_seconds=30,final_seconds=120,recovery_seconds=30,runtime_seconds=60,
     spool_max_bytes=64*1024**2,spool_max_entries=4096,spool_reserve_bytes=8*1024**2,
     repository_max_bytes=256*1024**2,repository_reserve_bytes=8*1024**2,recent_free_bytes=8*1024**2)
   if options.full_operation:
    capture_plan=request.pop('capture_preparation')
    plan=dict(schema_version='qt.storage_online_operation.v1',state_root=str(state),**kwargs,
      source_image=source_image,history_uuid=history_uuid,history_before=capture_plan['history_before'],
      attempt_seconds=capture_plan['attempt_seconds'],limits=vars(limits),keys_root=str(recovery_keys),
      socket_volume=recovery_socket,spool_destination=str(candidate_working))
    plan['inventory_path']=str(inventory)
    if canonical:plan.update(deployment_environment=str(canonical['environment']),deployment_repository=str(options.canonical_deployment_repository.resolve()))
    operation_file=state/'operation.json'
    host_boundary.save_receipt(operation_file,plan,initial=True)
    inspected=operation.run_operation_plan(operation_file)
    assert inspected['phase']=='inspected' and not (state/initial.STATE).exists()
    result=operation.run_operation_plan(operation_file,execute=True)
    preparation=initial._load(state)
    prepared_bytes=(state/initial.STATE).read_bytes()
    report['initial_preparation_seconds']=result['initial_preparation_seconds']
    report['single_operation_from_before_initial_preparation']=True
   else:
    result=operation.run_prepared_operation(state,**kwargs,source_image=source_image,limits=limits,
      keys_root=recovery_keys,socket_volume=recovery_socket,spool_destination=candidate_working)
  finally:
   operation.prepare_background=background;host_boundary.supervised_source_action=actual_action
  saved=final_host._load(state/final_host.STATE)
  assert saved['phase']=='recovery_runtime_ready'
  assert result['runtime']['collector_process_healthy'] and not result['ordinary_relaunch_authorized']
  assert result['runtime']['original_ui_admin_restored']
  with host_boundary.docker_deadline(time.monotonic()+10):
   restored_rows=host_boundary.inventory(project,operator_id=saved['binding']['worker_id'],activating=True,runtime_maintenance=True)
   for service in ('frontend','frontend-v2','grafana','pgadmin'):
    expected=preparation['clients'][service]
    assert restored_rows[service]['id']==expected['identity']['id']
    assert restored_rows[service]['running']==expected['was_running']
  report['original_ui_admin_restored']=True
  pgid=saved['recovery']['replacement_id'];owned.append(pgid)
  worker_receipt=host_boundary.load_receipt(state/launch._STATE)
  if not options.full_operation:
   assert worker_receipt['capture']==original_capture and worker_receipt['deadline']==original_capture_deadline
   report['operation_preflight_retired_worker_reentry']=True
  else:
   from datetime import datetime
   assert worker_receipt['deadline']-datetime.fromisoformat(worker_receipt['capture']['started_at']).timestamp() <= 180
  retired=json.loads(run(['inspect',worker_receipt['container_id'],'--format','{{json .State}}']).stdout)
  assert not retired['Running'] and retired['Pid']==0
  assert original_source_metadata==[working.stat().st_uid,working.stat().st_gid,working.stat().st_mode]
  assert (state/initial.STATE).read_bytes()==prepared_bytes
  assert host_boundary.cluster_identifier(pgid)==original_cluster
  assert host_boundary.database_query(pgid,frozen_sql)==before_final_frozen
  report['operation_driver']=dict(**result,final_entry_to_applications_ready_seconds=saved['runtime']['finished_at']-saved['started_at'],
    initial_recipe_preserved=True,source_metadata_preserved=True,reader_retired=True,frozen_preserved=True,
    fixture_only_publication_and_verification=True,production_admission=False)
  if options.completion_observation:
   final_bytes=(state/final_host.STATE).read_bytes()
   # Actual expiry, not altered clocks or a restarted migration allowance.
   while time.time() <= saved['deadline']+.1:
    time.sleep(min(.2,saved['deadline']+.2-time.time()))
   observed_deadline=time.monotonic()+180  # fixture observation only; services already serving
   def no_dispatch(*a,**kw):raise AssertionError('completion observation replayed a mutation')
   host_boundary.supervised_source_action=no_dispatch
   try:
    while True:
     assert time.monotonic()<observed_deadline, 'complete paired recovery observation timed out'
     observed=operation.run_operation_plan(operation_file)
     if observed['ready']:break
     time.sleep(.2)
    again=operation.run_operation_plan(operation_file,execute=True)
    if not canonical:assert again['ready'] and not again['ordinary_relaunch_authorized']
    assert observed['plan_id']==saved['commit']['confirmed_plan_id']
    if not canonical:assert again['recovery_generation']==observed['recovery_generation']
    if not canonical:assert (state/final_host.STATE).read_bytes()==final_bytes
    assert (state/initial.STATE).read_bytes()==prepared_bytes
    report['completion_observation']=dict(observed,after_original_final_deadline=True,
      repeated_without_dispatch=True,journals_unchanged=not bool(canonical))
   finally:host_boundary.supervised_source_action=actual_action
  if canonical:
   published=final_host._load(state/final_host.STATE)
   assert published['release']['status']=='published'
   report['canonical_publication']=dict(retained_marker=True,exact_public_recipe=True)
   env={**os.environ,'QT_SINGLE_NODE_ENV_FILE':str(canonical['environment']),
        'QT_SINGLE_NODE_STATE_ROOT':str(state)}
   deployment_log=state/'ordinary-deployment.log'
   deployment_started=time.monotonic()
   with deployment_log.open('w') as stream:
    deployment_log.chmod(0o600)
    deployment_command=['bash',str(options.canonical_deployment_repository/'scripts/automation/server_deploy.sh'),'deploy',revision]
    deployed=subprocess.run(deployment_command,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=1800)
   assert deployed.returncode==0, 'canonical ordinary deployment failed; see private fixture log'
   terminal=final_host._load(state/final_host.STATE)
   assert terminal['release']['status']=='deployed'
   # Compose may adopt the preserving container by recreation. Resolve its
   # exact service identity, then verify the original cluster/data, not a stale ID.
   databases=run(['ps','-aq','--filter','label=com.docker.compose.project='+project,
     '--filter','label=com.docker.compose.service=tsdb']).stdout.split()
   assert len(databases)==1, 'ordinary deployment database identity is ambiguous'
   deployed_database=databases[0]
   assert host_boundary.cluster_identifier(deployed_database)==original_cluster
   assert host_boundary.database_query(deployed_database,frozen_sql)==before_final_frozen
   from urllib.request import urlopen
   pgadmin_deadline=time.monotonic()+30  # post-deployment fixture observation only
   while True:
    try:
     with urlopen(canonical['pgadmin_url'],timeout=2) as response:
      assert response.status==200
     break
    except (OSError, AssertionError):
     if time.monotonic()>=pgadmin_deadline:raise
     time.sleep(.2)
   report['ordinary_deployment']=dict(recorded=True,cluster_preserved=True,frozen_preserved=True,
     database_container_recreated=deployed_database!=pgid,pgadmin_http_ready=True,
     deployment_seconds=time.monotonic()-deployment_started,
     retained_marker=True,production_admission=False)
  log.close()
  report.update(passed=True,image=image,fixture_seconds=time.monotonic()-started)
 else:
  name=project+'-storage-online';owned.append(name)
  def command(op,*,response_deadline=None,**extra):
   global last_reply
   last_reply=channel.exchange(op,response_deadline=response_deadline,**extra)
   return last_reply
  first_deadline=None
  launch_context=launch.launched_online_worker
  if options.commit_switch:
   source_holds.enter_context(host_boundary.deployment_lock(state))
   launch_context=launch.launched_online_worker_locked
  if options.initial_capture:
   capture_entry=time.monotonic()
   with launch_context(state,**kwargs) as (worker,receipt):
    first_channel=host_boundary.OnlineWorkerChannel(worker,deadline=time.monotonic()+receipt['deadline']-time.time())
    assert first_channel.greeting['state']=='background'
    original_capture=host_boundary.load_receipt(state/launch._STATE)['capture']
    original_capture_deadline=receipt['deadline']
    report['initial_capture_ready_seconds']=time.monotonic()-capture_entry
   # Actual clean worker retirement, then original capture/controller reentry.
   # No copy, final pause or recovery action was dispatched by the first worker.
  for attempt in range(1 if options.final_pause else 2):
   with launch_context(state,**kwargs) as (worker,receipt):
    channel=host_boundary.OnlineWorkerChannel(worker,deadline=time.monotonic()+receipt['deadline']-time.time());greeting=channel.greeting
    if options.initial_capture:
     saved_worker=host_boundary.load_receipt(state/launch._STATE)
     assert saved_worker['capture']==launch._capture_observation(pgid)
     assert saved_worker['deadline']==receipt['deadline']
     assert saved_worker['capture']==original_capture and receipt['deadline']==original_capture_deadline
     report['initial_capture_restart_preserved_original_clock']=True
     assert host_boundary.source_clients_serving(host_boundary.inventory(project,operator_id=receipt['container_id']))
     report['initial_capture_while_source_serving']=True
     report['initial_capture_binding']=saved_worker['capture']
    assert not greeting['final_switch_authorized']
    assert greeting['background_hashed_bytes']==0
    assert receipt['source_clients_unchanged']
    if attempt==0:
     first_deadline=receipt['deadline']
     previous_id=greeting['controller_id']
     try:
      with launch.launched_online_worker(state,**kwargs):
       raise AssertionError('second owning launcher admitted')
     except RuntimeError as exc:
      assert str(exc)=='storage_pause_deployment_lock_busy'
     for _ in range(3):command('reprove')
     assert command('status')['background_hashed_bytes']>0
     (control/'publish').write_text('publish')
     if options.worker_phases:
      def phase(step,relation=None):
       reply=command('prepare_step',step=step,relation=relation,max_duration_seconds=30)
       assert reply['result']['committed'] and not reply['final_switch_authorized']
       return reply
      phase('catalog_history','qt_fact_storage_cutover_v1.fact_versions')
      for _ in range(64):
       reply=command('sql_copy')
       outcome=reply['result']['outcome']
       if outcome=='identity_relocation_required':phase('identity_history')
       elif outcome=='raw_relocation_required':phase('raw_history')
       elif outcome=='both_tails_observed_empty' and (control/'published').exists():break
      else:raise RuntimeError('tiny_worker_phases_did_not_converge')
      phase('identity_capture')
      relations=[];after=None;catalog_relations=None
      while True:
       page=command('inspect_references',after=after)['result']
       assert len(page['references'])<=32
       if catalog_relations is None:catalog_relations=page['catalogs']
       assert page['catalogs']==catalog_relations
       relations.extend(row['relation'] for row in page['references'])
       after=page['next_after']
       if after is None:break
      assert len(relations)==len(set(relations))<=8192
      for relation in relations:
       phase('reference_prepare',relation);phase('reference_validate',relation)
      phase('reference_adopt')
      for relation in catalog_relations:phase('catalog_history',relation)
      (control/'phases-finished').write_text('finished')
      waitfile('catalogs-verified')
      report['reference_discovery_and_catalog_moves_owned_by_worker']=True
      report['explicit_preparation_through_worker']=True
      report['worker_reference_relations']=len(relations)
     baselines=set()
     def archive_page():
      page=command('archive_copy')['result']
      if page.get('baseline_complete') is True:baselines.add(page['family'])
     archive_page()
     waitfile('published')
     for i in range(30):
      archive_page();command('reprove');reply=command('sql_copy')
      if (reply['result']['outcome']=='both_tails_observed_empty'
          and len(reply['reproved_families_at_observation'])==3
          and (not options.final_delta or len(baselines)==3)):break
     else:raise RuntimeError('tiny_host_tails_did_not_converge')
     final=command('status');assert final['background_hashed_bytes']>0
     first_id=receipt['container_id']
     if options.final_pause:
      from scripts.automation import storage_online_final as final_host
      import multiprocessing,signal
      pause_args=dict(project=project,source_revision=revision,worker_id=first_id,
                      controller_id=greeting['controller_id'],max_duration_seconds=120 if options.recovery_runtime else 60)
      # The independent diagnostic publisher shares source mounts but is outside
      # the admitted project/network. Prove refusal, then let it finish while the
      # real source peers still serve. Never exempt it from production admission.
      with host_boundary.docker_deadline(time.monotonic()+5):
       try:
        final_host._admit_mount_writers(host_boundary.inventory(project,operator_id=first_id),operator_id=first_id)
        raise AssertionError('independent fixture publisher admitted')
       except RuntimeError as exc:
        assert str(exc)=='storage_online_unadmitted_mount_writer'
      assert not (state/final_host.STATE).exists()
      assert initial._source_healthy(initial._admit_source(state,preparation,require_running=True,operator_id=first_id))
      if options.final_delta:
       (control/'final-publish').write_text('publish')
       waitfile('final-published')
      (control/'finished').write_text('finished');fixture.wait(timeout=30)
      assert fixture.returncode==0
      publisher_state=json.loads(run(['inspect',test,'--format','{{json .State}}']).stdout)
      assert not publisher_state['Running'] and publisher_state['Pid']==0
      frozen_sql="SELECT jsonb_build_object('datasets',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM market.datasets t),'dataset_series',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM market.dataset_series t),'capture',(SELECT to_jsonb(c) FROM qt_fact_header_cutover_v2.capture c WHERE id=1))::text"
      before_final_frozen=host_boundary.database_query(pgid,frozen_sql)
      report['independent_publisher_retired_before_final']=dict(unadmitted_writer_refused=True,
        original_clients_still_serving=True,fixture_exited=True,source_pause_started=False,
        publisher_exclusion_authorized=False)
      if options.worker_shutdown:
       # Fail only the final drain, after original initial preparation/resumption.
       marker=project+'-finalizer-marker';owned.append(marker)
       run(['run','--name',marker,'--user','70:70','--network','none','--memory','64m',
         '--mount','type=bind,source='+str(working)+',target=/s','--entrypoint','python',image,'-c',
         "from pathlib import Path;p=Path('/s/objects');[(p/n).unlink(missing_ok=True) for n in ('lifecycle-stopped','heartbeat-stopped')];"+
         ("(p/'fail-final-drain').write_text('fail')" if options.worker_shutdown=='fail' else "None")])
      def interrupt_first_stop():
       original=host_boundary.docker
       def stop_then_die(*args,**kwargs):
        value=original(*args,**kwargs)
        if args[0]=='stop':
         assert final_host._load(state/final_host.STATE)['phase']=='stopping'
         os.kill(os.getpid(),signal.SIGKILL)
        return value
       host_boundary.docker=stop_then_die
       final_host.stop_online_source_locked(state,**pause_args)
      child=multiprocessing.get_context('fork').Process(target=interrupt_first_stop)
      child.start();child.join(timeout=30)
      if child.is_alive():
       child.kill();child.join(timeout=10)
       raise RuntimeError('owned final pause child exceeded fixture deadline')
      assert child.exitcode == -signal.SIGKILL
      first=final_host._load(state/final_host.STATE)
      assert first['phase']=='stopping'
      rows=host_boundary.inventory(project,operator_id=first_id)
      assert sum(not rows[n]['running'] for n in host_boundary.STOP)==2
      try:
       paused=final_host.stop_online_source_locked(state,**pause_args)
      except RuntimeError as exc:
       if options.worker_shutdown!='fail' or str(exc)!='storage_pause_unclean_stop: service=market-data-collector':raise
       observed=command('status')
       assert observed['controller_id']==greeting['controller_id']
       assert observed['background_hashed_bytes']==final['background_hashed_bytes']
       report['failed_drain_live_proof_retained_before_exit']=True
       raise
      assert paused['phase']=='paused'
      assert paused['deadline']==first['deadline'] and paused['deadline_boot']==first['deadline_boot']
      before=(state/final_host.STATE).read_bytes()
      assert final_host.stop_online_source_locked(state,**pause_args)==paused
      assert (state/final_host.STATE).read_bytes()==before
      try:
       final_host.stop_online_source_locked(state,**(pause_args|dict(controller_id='f'*32)))
       raise AssertionError('changed controller admitted')
      except RuntimeError as exc:
       assert str(exc)=='storage_online_final_binding_changed'
      inventory_before=inventory.read_bytes()
      try:
       inventory.write_bytes(inventory_before+b' ')
       try:
        final_host.stop_online_source_locked(state,**pause_args)
        raise AssertionError('changed admitted inventory accepted')
       except RuntimeError as exc:
        assert str(exc)=='storage_online_final_inventory_changed'
      finally:
       inventory.write_bytes(inventory_before)
      assert (state/final_host.STATE).read_bytes()==before
      rows=host_boundary.inventory(project,operator_id=first_id)
      assert not any(rows[n]['running'] for n in host_boundary.STOP)
      assert all(rows[n]['running'] for n in host_boundary.PASSIVE)
      assert command('status')['controller_id']==greeting['controller_id']
      drained=final_host.observe_source_drain_locked(state,exchange=command,max_entries=10000)
      assert not drained['spool_empty_at_observation'] and not drained['publisher_drain_authorized']
      assert drained['pending_files']>0  # Existing real QT fixture intentionally retains sealed WAL.
      report['fixture_retained_spool_observation']=drained
      if options.real_worker_publication:
       result=json.loads((working/'objects'/'real-publication-result.json').read_text())
       assert result['facts_manifests_mappings']==[1,1,1]
       assert result['wal_retired_after_canonical_ack'] and result['stop_waited_for_publication']
       report['real_docker_publication']=result
      if options.worker_shutdown:
       collector=rows['market-data-collector']['id']
       stopped=json.loads(run(['inspect',collector,'--format','{{json .State}}']).stdout)
       assert not stopped['Running'] and not stopped['OOMKilled']
       assert stopped['ExitCode']==(5 if options.worker_shutdown=='fail' else 0)
       assert (working/'objects'/'lifecycle-stopped').exists()
       assert (working/'objects'/'heartbeat-stopped').exists()
       logs=run(['logs','--tail','120',collector]).stdout+run(['logs','--tail','120',collector]).stderr
       (state/'worker-shutdown.log').write_text(logs)
       assert 'market_data_collector_shutdown_signal' in logs
       if options.worker_shutdown=='fail':
        assert 'market_data_collector_shutdown_failed' in logs
        assert (working/'spool'/'qt-signal-owned-fixture'/'pending.sealed').read_bytes()==b'owned failed-finalizer WAL fixture'
       report['docker_worker_shutdown']=dict(exit_code=stopped['ExitCode'],
         supervisor_failure_propagated=options.worker_shutdown=='fail',
         lifecycle_and_heartbeat_stopped=True, controlled_adapter=True,
         publisher_drain_authorized=False, same_migration_controller=True)
      # Inject ONLY this owned diagnostic WAL after the synthetic source stops.
      # The observer must retain it even beside a misleading acknowledgement.
      probe=project+'-spool-probe';owned.append(probe)
      probe_args=['run','--name',probe,'--user','70:70','--network','none','--memory','64m',
        '--mount','type=bind,source='+str(working)+',target=/s','--entrypoint','python',image,'-c']
      run(probe_args+["from pathlib import Path;p=Path('/s/spool/qt-final-owned-probe');p.mkdir(parents=True);(p/'segment.sealed').write_bytes(b'pending-WAL');(p/'segment.ack.json').write_text('{}')"])
      pending=final_host.observe_source_drain_locked(state,exchange=command,max_entries=10000)
      assert not pending['spool_empty_at_observation'] and pending['pending_files']==drained['pending_files']+1
      assert (working/'spool/qt-final-owned-probe/segment.sealed').read_bytes()==b'pending-WAL'
      # Remove only diagnostic bytes created immediately above; never source WAL.
      cleanup_probe=project+'-spool-probe-cleanup';owned.append(cleanup_probe)
      run(['run','--name',cleanup_probe,'--user','70:70','--network','none','--memory','64m',
        '--mount','type=bind,source='+str(working)+',target=/s','--entrypoint','python',image,'-c',
        "from pathlib import Path;p=Path('/s/spool/qt-final-owned-probe');(p/'segment.sealed').unlink();(p/'segment.ack.json').unlink();p.rmdir()"])
      restored=final_host.observe_source_drain_locked(state,exchange=command,max_entries=10000)
      assert restored['pending_files']==drained['pending_files'] and restored['pending_bytes']==drained['pending_bytes']
      assert not restored['spool_empty_at_observation']
      assert (state/final_host.STATE).read_bytes()==before
      report['read_only_spool_observation']=True
      report['pending_spool_preserved']=True
      report['final_stop_interrupted_reentry']=True
      report['final_stop_seconds']=paused['paused_at']-paused['started_at']
      report['final_original_deadline_preserved']=True
      report['final_stopped_only_exact_clients']=True
      report['final_worker_and_proof_retained']=True
      report['final_source_resumption_qualified']=False
      if options.commit_switch:
       source_check=source_holds.enter_context(final_host.held_source_writers_locked(state,source_image=source_image))
      if options.final_delta:
       # The independent fixture published its late tail before source pause,
       # then exited. Its writable mounts cannot remain live during admission.
       # Bind the already persisted final window once. The older diagnostic's
       # extra 25-second subwindow is insufficient for repeated global admission
       # plus terminal reconciliation; the original 60-second fixture ceiling is
       # unchanged and no later phase may renew or extend this absolute deadline.
       deadline=time.monotonic()+final_host._remaining(paused)-1
       def lost_reply(operation,**args):
        command(operation,**args)
        raise TimeoutError('fixture dropped fully received delta reply')
       try:
        final_host.copy_final_delta_locked(state,exchange=lost_reply,deadline=deadline,max_rounds=1)
        raise AssertionError('lost delta reply ignored')
       except TimeoutError:
        pass
       # The pipe owner consumed that response and keeps its sequence. This is
       # not blind recovery of an unread/partial reply or a dead worker.
       delta=final_host.copy_final_delta_locked(state,exchange=command,deadline=deadline,max_rounds=8)
       assert delta['last_observation']['sql']['outcome']=='both_tails_observed_empty'
       assert all(x['captured_tail_empty_at_observation'] for x in delta['last_observation']['archives'])
       assert not delta['publisher_drain_authorized'] and not delta['final_switch_authorized']
       assert (state/final_host.STATE).read_bytes()==before
       assert command('status')['controller_id']==greeting['controller_id']
       assert command('status')['background_hashed_bytes']>final['background_hashed_bytes']
       report['held_final_delta']=dict(rounds_after_lost_reply=delta['rounds'],
         original_deadline_preserved=True,pre_pause_publication_copied=True,
         fully_received_reply_loss=True,final_switch_authorized=False)
       final=command('status')
       if options.switch_entry:
        original_save=host_boundary.save_receipt
        def save_then_interrupt(path,value,**args):
         original_save(path,value,**args)
         if path==state/final_host.STATE and value['phase']=='switch_entered':
          raise RuntimeError('fixture interrupted after durable switch intent')
        host_boundary.save_receipt=save_then_interrupt
        try:
         try:
          final_host.record_switch_entry_locked(state,deadline=deadline,
            observe_worker=lambda **args:command('status',response_deadline=args['deadline']))
          raise AssertionError('switch intent interruption was lost')
         except RuntimeError as exc:
          assert str(exc)=='fixture interrupted after durable switch intent'
        finally:host_boundary.save_receipt=original_save
        entered=final_host._load(state/final_host.STATE)
        assert entered['phase']=='switch_entered' and entered['binding']==paused['binding']
        assert entered['deadline']==paused['deadline'] and entered['deadline_boot']==paused['deadline_boot']
        assert entered['switch']['deadline_monotonic']==deadline
        checkpoint=(state/final_host.STATE).read_bytes()
        def unexpected_observation(**args):raise AssertionError('recorded switch intent was replayed')
        for action in (
          lambda:final_host.record_switch_entry_locked(state,deadline=deadline,observe_worker=unexpected_observation),
          lambda:final_host.stop_online_source_locked(state,**pause_args)):
         try:action();raise AssertionError('switch-entered pause reentry admitted')
         except RuntimeError as exc:assert str(exc)=='storage_online_final_switch_reconciliation_required'
        try:
         final_host.copy_final_delta_locked(state,exchange=command,deadline=deadline,max_rounds=1)
         raise AssertionError('tail mutation admitted after switch entry')
        except RuntimeError as exc:assert str(exc)=='storage_online_final_delta_paused_source_required'
        assert (state/final_host.STATE).read_bytes()==checkpoint
        final=command('status')
        assert final['controller_id']==greeting['controller_id'] and final['background_hashed_bytes']>0
        outcome=final_host.inspect_switch_outcome_locked(state,exchange=command)
        assert outcome['outcome']=='uncommitted' and not outcome['collection_resume_authorized']
        assert not outcome['runtime_activation_authorized']
        assert (state/final_host.STATE).read_bytes()==checkpoint
        report['fresh_outcome_observation']=dict(outcome='uncommitted',intent_preserved=True,
          collection_resume_authorized=False,runtime_activation_authorized=False)
        final=command('status')
        report['switch_entry_checkpoint']=dict(interrupted_after_durable_save=True,
          original_deadline_preserved=True,replay_refused=True,source_held=True,
          database_commit_dispatched=False,collection_resume_authorized=False)
        if options.close_logins:
         report['database_job_environment']=json.loads(host_boundary.database_query(pgid,
           "SELECT json_build_object('extensions',(SELECT jsonb_object_agg(extname,extversion) FROM pg_extension),"
           "'preload',current_setting('shared_preload_libraries'))"))
         real_maintenance=host_boundary.maintenance_query
         def lost_gate_reply(container,sql):
          result=real_maintenance(container,sql)
          if "ALTER DATABASE" in sql:
           raise TimeoutError('fixture lost fully received login-close reply')
          return result
         if options.close_logins=='lost-reply':host_boundary.maintenance_query=lost_gate_reply
         gate_started=time.monotonic()
         try:
          if options.close_logins=='lost-reply':
           try:
            final_host.close_database_logins_locked(state,exchange=command)
            raise AssertionError('lost gate reply accepted')
           except TimeoutError as exc:assert str(exc)=='fixture lost fully received login-close reply'
          else:
           gate_result=final_host.close_database_logins_locked(state,exchange=command)
           assert gate_result['new_logins_closed'] and not gate_result['database_switch_authorized']
         finally:host_boundary.maintenance_query=real_maintenance
         gated=final_host._load(state/final_host.STATE)
         assert gated['phase']==('login_closing' if options.close_logins=='lost-reply' else 'login_closed')
         assert gated['binding']==entered['binding'] and gated['switch']==entered['switch']
         assert gated['deadline']==entered['deadline'] and gated['deadline_boot']==entered['deadline_boot']
         with host_boundary.docker_deadline(deadline):
          database=json.loads(real_maintenance(pgid,final_host._GATE_OBSERVE))
          assert database=={**gated['login_gate']['database'],'allow_connections':False}
          try:host_boundary.database_query(pgid,'SELECT 1');raise AssertionError('new target login accepted')
          except RuntimeError as exc:assert str(exc).startswith('storage_pause_docker_failed')
         current=command('final_session_check',deadline=deadline)
         assert current['result']['database']==database and current['controller_id']==greeting['controller_id']
         fresh=command('inspect_outcome',deadline=min(deadline,time.monotonic()+4))
         assert fresh['result']['outcome']=='uncommitted' and not fresh['result']['collection_resume_authorized']
         held_bytes=(state/final_host.STATE).read_bytes()
         if options.close_logins=='success':
          residual_started=time.monotonic()
          residual=final_host.copy_final_delta_locked(state,exchange=command,deadline=deadline,max_rounds=2)
          assert residual['last_observation']['sql']['outcome']=='both_tails_observed_empty'
          assert all(p['captured_tail_empty_at_observation'] for p in residual['last_observation']['archives'])
          assert not residual['publisher_drain_authorized'] and not residual['final_switch_authorized']
          assert (state/final_host.STATE).read_bytes()==held_bytes
          report['gated_residual']=dict(rounds=residual['rounds'],same_worker=True,
            final_intent_unchanged=True,elapsed_seconds=time.monotonic()-residual_started,
            limitation='Host route with already-converged fixture tail; nonempty late QT tail qualified separately.')
         try:final_host.close_database_logins_locked(state,exchange=command);raise AssertionError('gate replay')
         except RuntimeError as exc:assert str(exc)=='storage_online_login_switch_intent_required'
         if options.close_logins=='lost-reply':
          try:final_host.resume_online_source_locked(state,exchange=command);raise AssertionError('uncertain gate source resume')
          except RuntimeError as exc:assert str(exc)=='storage_online_resume_switch_intent_required'
         assert (state/final_host.STATE).read_bytes()==held_bytes
         report['login_gate']=dict(mode=options.close_logins,phase=gated['phase'],same_worker=True,
           new_logins_refused=True,fresh_negative_without_authority=True,original_clocks_preserved=True,
           replay_refused=True,uncertain_gate_resumption_refused=options.close_logins=='lost-reply',elapsed_seconds=time.monotonic()-gate_started)
         (state/'login-gate-intent.json').write_text(json.dumps(gated,indent=2))
        if options.commit_switch:
         commit_started=time.monotonic()
         committed=final_host.commit_online_handoff_locked(state,exchange=command)
         assert committed['outcome']=='committed' and committed['database_handoff_committed']
         assert not committed['runtime_activation_authorized'] and not committed['collection_resume_authorized']
         source_check()
         final=last_reply  # Fresh inspect_outcome already received by the host seam.
         assert final['operation']=='inspect_outcome' and final['state']=='committed'
         report['held_database_commit']=dict(same_worker=True,fresh_committed_outcome=True,
           source_kernel_hold_retained=True,elapsed_seconds=time.monotonic()-commit_started,
           runtime_activation_authorized=False)
        if options.abort_resume:
         resume_started=time.monotonic()
         if options.abort_restore_lost_reply:
          action=host_boundary.supervised_source_action
          actions=[]
          def lose_restore_reply(*args,**kwargs):
           action(*args,**kwargs)
           actions.append(True)
           assert len(actions)==1
           raise TimeoutError('fixture discarded fully received login restoration reply')
          host_boundary.supervised_source_action=lose_restore_reply
          try:
           try:final_host.resume_online_source_locked(state,exchange=command);raise AssertionError('lost restoration reply accepted')
           except TimeoutError as exc:assert str(exc)=='fixture discarded fully received login restoration reply'
          finally:host_boundary.supervised_source_action=action
          pending=final_host._load(state/final_host.STATE)
          assert pending['phase']=='source_resuming'
          assert pending['resume']['gate_restore']=={'completed':[],'inflight':'logins'}
          assert pending['resume']['completed']==[] and pending['resume']['inflight'] is None
          assert pending['login_gate']==gated['login_gate'] and pending['switch']==entered['switch']
          with host_boundary.docker_deadline(deadline):
           assert host_boundary.database_query(pgid,"SELECT datallowconn FROM pg_database WHERE datname=current_database()").strip()=='t'
           assert not any(host_boundary.inventory(project,operator_id=receipt['container_id'])[n]['running'] for n in host_boundary.STOP)
          try:final_host.resume_online_source_locked(state,exchange=command);raise AssertionError('unresolved restoration replayed')
          except RuntimeError as exc:assert str(exc)=='storage_online_resume_switch_intent_required'
          report['gated_abort_restore_lost_reply']=dict(actual_login_reopened=True,
            fully_received_reply_discarded=True,restoration_unresolved=True,jobs_restart_dispatched=False,
            source_starts_dispatched=False,original_clocks_preserved=True,replay_refused=True,
            elapsed_seconds=time.monotonic()-resume_started,outer_loss_qualified=False)
         elif options.abort_resume_fence_loss:
          from scripts.ci.online_start_reply_fixture import held_start_reply
          first_service=next(n for n in host_boundary.STOP if preparation['clients'][n]['was_running'])
          first_source=source[first_service]['id']
          killed=[]
          def kill_owned_fence():
           with host_boundary.docker_deadline(deadline):
            if options.abort_resume_late_start:
             before=host_boundary.inventory(project,operator_id=receipt['container_id'])
             assert not any(before[n]['running'] for n in host_boundary.STOP)
            pending=final_host._load(state/final_host.STATE)
            assert pending['phase']=='source_resuming'
            assert pending['resume']['inflight']['container_id']==first_source
            sql="SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid<>pg_backend_pid() AND classid=((hashtextextended('quant-trad:fact-header-cutover:v2',0)>>32)&4294967295)::oid AND objid=(hashtextextended('quant-trad:fact-header-cutover:v2',0)&4294967295)::oid"
            pid=host_boundary.database_query(pgid,sql).strip()
            assert pid.isdigit()
            assert host_boundary.database_query(pgid,'SELECT pg_terminate_backend('+pid+',5000)').strip()=='t'
            killed.append(int(pid))
          fault_arguments={'on_queued' if options.abort_resume_late_start else 'on_started':kill_owned_fence}
          with held_start_reply(first_source,deadline=deadline,**fault_arguments) as fault:
           try:
            final_host.resume_online_source_locked(state,exchange=command)
            raise AssertionError('source resume ignored lost SQL ownership')
           except (RuntimeError,EOFError,BrokenPipeError) as exc:
            refusal=type(exc).__name__
           if options.abort_resume_late_start:
            assert fault['completed'].wait(max(0,deadline-time.monotonic()))
          assert killed
          if options.abort_resume_late_start:
           assert fault['request_queued_at'] < fault['fence_loss_completed_at'] < fault['cli_reaped_before_forward_at'] < fault['daemon_started_after_cli_reaped_at']
          else:
           assert fault.get('cli_pending_after_daemon_start')
          assert fault['cli'] is not None and fault['cli'].poll() is not None
          assert fault['start_count']==1 and fault.get('fault_completed')
          pending=final_host._load(state/final_host.STATE)
          assert pending['phase']=='source_resuming' and pending['resume']['completed']==[]
          assert pending['resume']['inflight']['container_id']==first_source
          assert pending['deadline']==entered['deadline'] and pending['deadline_boot']==entered['deadline_boot']
          assert pending['switch']==entered['switch'] and pending['binding']==entered['binding']
          exact=host_boundary.inventory(project,operator_id=receipt['container_id'])
          assert [n for n in host_boundary.STOP if exact[n]['running']]==[first_service]
          unresolved=(state/final_host.STATE).read_bytes()
          try:
           final_host.resume_online_source_locked(state,exchange=command)
           raise AssertionError('unresolved start replayed')
          except RuntimeError as exc:assert str(exc)=='storage_online_resume_switch_intent_required'
          assert (state/final_host.STATE).read_bytes()==unresolved
          report['source_abort_resumption_fault']=dict(real_daemon_start_status=fault['daemon_start_status'],
            actual_cli_pending_at_fence_loss=True,local_cli_reaped=True,started_services=[first_service],
            intermediary_queued_start_after_reap=options.abort_resume_late_start,
            ordering={k:v for k,v in fault.items() if k.endswith("_at")},
            no_further_start=True,original_deadlines_preserved=True,inflight_intent_retained=True,
            replay_refused=True,partial_source_running=True,refusal=refusal,
            daemon_late_completion_qualified=False,production_readiness=False)
         else:
          if options.abort_resume_lost_end:
           def lose_end(operation,**kwargs):
            reply=command(operation,**kwargs)
            if operation=='rollback_fence_end':
             raise TimeoutError('diagnostic fully received end acknowledgement discarded')
            return reply
           try:
            final_host.resume_online_source_locked(state,exchange=lose_end)
            raise AssertionError('lost end acknowledgement was ignored')
           except TimeoutError:pass
           pending=final_host._load(state/final_host.STATE)
           assert pending['phase']=='source_resuming' and pending['resume']['inflight'] is None
           assert pending['resume']['completed']==[n for n in host_boundary.STOP if preparation['clients'][n]['was_running']]
           original_start=final_host._supervised_source_start
           def forbid_start(*args,**kwargs):raise AssertionError('terminal reconciliation replayed a start')
           final_host._supervised_source_start=forbid_start
           try:
            resumed=final_host.reconcile_source_resumed_locked(state,exchange=command)
           finally:final_host._supervised_source_start=original_start
           report['source_resume_terminal_reconciliation']=dict(fully_received_end_reply_discarded=True,
             same_worker=True,no_starts_replayed=True,original_deadline_preserved=True,
             partial_or_unread_response_qualified=False,outer_worker_loss_qualified=False)
          else:
           resumed=final_host.resume_online_source_locked(state,exchange=command)
          terminal=final_host._load(state/final_host.STATE)
          assert terminal['phase']=='source_resumed' and resumed['original_source_resumed']
          if options.close_logins:
           assert resumed['original_login_gate_restored'] and resumed['database_jobs_restart_requested']
           assert terminal['login_gate']==gated['login_gate']
           assert terminal['resume']['gate_restore']=={'completed':['logins','jobs'],'inflight':None}
           with host_boundary.docker_deadline(deadline):
            assert host_boundary.database_query(pgid,"SELECT datallowconn FROM pg_database WHERE datname=current_database()").strip()=='t'
           report['gated_abort_restore']=dict(original_access_restored=True,
             jobs_restart_request_accepted=True,all_jobs_recovery_qualified=False,original_gate_evidence_retained=True)

          assert all(terminal[k]==v for k,v in entered.items() if k!='phase')
          assert initial._source_healthy(initial._admit_source(state,initial._load(state),
            require_running=True,operator_id=receipt['container_id']))
          assert not run(['inspect',source['initialize']['id'],'--format','{{.State.Running}}']).stdout.strip()=='true'
          terminal_bytes=(state/final_host.STATE).read_bytes()
          try:
           final_host.resume_online_source_locked(state,exchange=command)
           raise AssertionError('terminal source resumption replayed')
          except RuntimeError as exc:assert str(exc)=='storage_online_resume_switch_intent_required'
          assert (state/final_host.STATE).read_bytes()==terminal_bytes
          report['source_abort_resumption']=dict(exact_original_clients_healthy=True,
            initializer_stays_stopped=True,original_deadlines_preserved=True,
            final_marker_retained=True,same_live_controller=True,
            elapsed_seconds=time.monotonic()-resume_started,production_readiness=False)
    else:
     assert greeting['controller_id']!=previous_id
     assert receipt['container_id']==first_id and receipt['deadline']==first_deadline
     report['reentry_empty_proof']=True
    if options.worker_attach_loss and attempt==0:
     # Only the owned migration Python process in its admitted shared PG PID
     # namespace; no PG/source peer receives a signal. Stop it so EOF cannot
     # accidentally make a detached CLI look like a successful retirement.
     stop_code="from pathlib import Path;import os,signal;ids=[int(p.name) for p in Path('/proc').iterdir() if p.name.isdigit() and (p/'cmdline').is_file() and (p/'cmdline').read_bytes().split(bytes([0]))[:3]==[b'python',b'-m',b'scripts.automation.storage_online_worker']];assert len(ids)==1;os.kill(ids[0],signal.SIGSTOP);print('owned_worker_stopped')"
     assert run(['exec','--user','70:70',receipt['container_id'],'python','-c',stop_code]).stdout.strip()=='owned_worker_stopped'
     worker.kill();worker.wait(timeout=5)
     observed=json.loads(run(['inspect',receipt['container_id'],'--format','{{json .State}}']).stdout)
     assert observed['Running'] and observed['Pid']>0
     report['worker_attach_loss']=dict(cli_reaped_before_retirement=True,daemon_worker_still_running=True)
    elif not options.abort_resume_fence_loss:command('close')
   if options.worker_attach_loss and attempt==0:
    observed=json.loads(run(['inspect',receipt['container_id'],'--format','{{json .State}}']).stdout)
    assert not observed['Running'] and not observed['Restarting'] and observed['Pid']==0
    report['worker_attach_loss']['daemon_worker_retired']=True
   assert worker.returncode != 0 if (options.abort_resume_fence_loss or options.worker_attach_loss and attempt==0) else worker.returncode == 0
   if options.close_logins and not options.abort_resume:
    retired=json.loads(run(['inspect',receipt['container_id'],'--format','{{json .State}}']).stdout)
    assert not retired['Running'] and retired['Pid']==0
    if options.commit_switch:
     source_check()
     try:
      with host_boundary.deployment_lock(state):raise AssertionError('deployment lock released before recovery handover')
     except RuntimeError as exc:assert str(exc)=='storage_pause_deployment_lock_busy'
     report['held_database_commit']['deployment_lock_retained_after_worker_retirement']=True
     if options.recovery_mounts:
      before=final_host._load(state/final_host.STATE)
      original_recipe=(state/initial.held.DATABASE_RECIPE).read_bytes()
      started_recovery=time.monotonic()
      if options.recovery_create_reply_loss:
       actual_action=host_boundary.supervised_source_action
       def lose_created_reply(arguments,**kwargs):
        actual_action(arguments,**kwargs)
        if arguments[0]=='compose' and 'create' in arguments:
         raise EOFError('owned completed recovery create reply discarded')
       host_boundary.supervised_source_action=lose_created_reply
      result=final_host.prepare_recovery_database_locked(state,worker_process=worker,
          keys_root=recovery_keys,socket_volume=recovery_socket,max_duration_seconds=30)
      assert result['database_recovery_mounts_ready'] and not result['runtime_activation_authorized']
      pgid=result['database_id'];owned.append(pgid)
      source['tsdb']=host_boundary.identities(host_boundary.inventory(project,operator_id=receipt['container_id']))['tsdb']
      after=final_host._load(state/final_host.STATE)
      assert after['phase']=='recovery_database_ready'
      assert all(after[k]==before[k] for k in ('deadline','deadline_boot','switch','commit','binding','login_gate'))
      assert (state/initial.held.DATABASE_RECIPE).read_bytes()==original_recipe
      assert host_boundary.cluster_identifier(pgid,maintenance=True)==original_cluster
      report['recovery_mounts']=dict(preserved_cluster=True,original_recipe_preserved=True,
          login_gate_still_closed=True,reader_retired_before_mounts=True,
          phase_seconds=time.monotonic()-started_recovery,
          final_entry_to_database_ready_seconds=after['recovery']['finished_at']-after['started_at'],
          runtime_activation_authorized=False,encrypted_recovery_qualified=False)
      try:
       final_host.prepare_recovery_database_locked(state,worker_process=worker,
           keys_root=recovery_keys,socket_volume=recovery_socket,max_duration_seconds=30)
       raise AssertionError('completed recovery phase replay admitted')
      except RuntimeError as exc:assert str(exc)=='storage_online_recovery_committed_live_hold_required'
     if options.recovery_repositories:
      owned.append(project+'-storage-repository-prepare')
      before_repositories=final_host._load(state/final_host.STATE)
      start_repositories=time.monotonic()
      actual_repository_action=host_boundary.supervised_source_action
      if options.recovery_repository_reply_loss:
       def lose_preparer_reply(arguments,**kwargs):
        actual_repository_action(arguments,**kwargs)
        if arguments[:2]==['start','--attach']:
         raise EOFError('owned completed repository preparation reply discarded')
       host_boundary.supervised_source_action=lose_preparer_reply
      try:
       result=final_host.prepare_online_repositories_locked(state,worker_process=worker,
           max_bytes=256*1024**2,reserve_bytes=8*1024**2,recent_free_bytes=8*1024**2,max_duration_seconds=30)
       assert not options.recovery_repository_reply_loss
      except EOFError as exc:
       assert options.recovery_repository_reply_loss and str(exc)=='owned completed repository preparation reply discarded'
       unresolved=final_host._load(state/final_host.STATE)
       assert unresolved['phase']=='recovery_repository_preparing'
       assert unresolved['repositories']['inflight']=='prepare'
       assert unresolved['repositories']['completed']==['logins','create']
       assert unresolved['repositories']['report'] is None
       assert all(unresolved[k]==before_repositories[k] for k in ('deadline','deadline_boot','switch','commit','binding','login_gate','recovery'))
       helper_state=json.loads(run(['inspect',unresolved['repositories']['helper_id'],'--format','{{json .State}}']).stdout)
       assert not helper_state['Running'] and helper_state['Pid']==0 and helper_state['ExitCode']==0
       assert host_boundary.maintenance_query(pgid,"SELECT current_setting('archive_mode')").strip()=='off'
       assert not any(host_boundary.inventory(project,operator_id=receipt['container_id'])[n]['running'] for n in host_boundary.STOP)
       report['recovery_repository_lost_reply']=dict(actual_preparer_completed=True,
           unresolved_intent_preserved=True,settings_not_dispatched=True,source_not_restarted=True,
           limitation='Fully received response discarded; not late execution, unread framing or outer-controller loss.')
      finally:
       host_boundary.supervised_source_action=actual_repository_action
      if not options.recovery_repository_reply_loss:
       after_repositories=final_host._load(state/final_host.STATE)
       assert after_repositories['phase']=='recovery_wal_ready'
       assert result['native_wal_delivered'] and result['repositories_initialized'] and not result['backup_created']
       assert all(after_repositories[k]==before_repositories[k] for k in ('deadline','deadline_boot','switch','commit','binding','login_gate','recovery'))
       assert host_boundary.cluster_identifier(pgid)==original_cluster
       report['recovery_repositories']=dict(native_wal_delivered=True,encrypted_repositories_prepared=True,
           reader_retired_before_keys=True,original_clocks_preserved=True,
           component_seconds=time.monotonic()-start_repositories,
           final_entry_to_wal_ready_seconds=after_repositories['repositories']['finished_at']-after_repositories['started_at'],
           runtime_activation_authorized=False,backup_created=False)
      try:
       final_host.prepare_online_repositories_locked(state,worker_process=worker,
           max_bytes=256*1024**2,reserve_bytes=8*1024**2,recent_free_bytes=8*1024**2,max_duration_seconds=30)
       raise AssertionError('completed repository preparation replay admitted')
      except RuntimeError as exc:assert str(exc)=='storage_online_repository_live_transition_required'
     if options.recovery_spool:
      owned.append(project+'-storage-spool-prepare')
      spool_before=final_host._load(state/final_host.STATE)
      spool_started=time.monotonic()
      actual_spool_action=host_boundary.supervised_source_action
      if options.recovery_spool_reply_loss:
       def lose_copy_reply(arguments,**kwargs):
        actual_spool_action(arguments,**kwargs)
        if arguments[:2]==['start','--attach']:
         raise EOFError('owned completed spool copy reply discarded')
       host_boundary.supervised_source_action=lose_copy_reply
      try:
       result=final_host.prepare_online_runtime_spool_locked(state,worker_process=worker,
           destination=candidate_working,max_bytes=64*1024**2,max_entries=4096,
           reserve_bytes=8*1024**2,max_duration_seconds=15)
       assert not options.recovery_spool_reply_loss
       spool_after=final_host._load(state/final_host.STATE)
       assert spool_after['phase']=='recovery_spool_ready'
       assert result['source_preserved'] and not result['runtime_activation_authorized']
       assert result['copied_files']==drained['pending_files'] and result['copied_bytes']==drained['pending_bytes']
       assert all(spool_after[k]==spool_before[k] for k in ('deadline','deadline_boot','switch','commit','binding','login_gate','recovery','repositories'))
       report['recovery_spool']=dict(source_preserved=True,private_candidate_owner=1000,
           copied_files=result['copied_files'],copied_bytes=result['copied_bytes'],
           component_seconds=time.monotonic()-spool_started,
           final_entry_to_spool_ready_seconds=spool_after['runtime_spool']['finished_at']-spool_after['started_at'],
           runtime_activation_authorized=False)
       probe="import json,hashlib,stat;from pathlib import Path;items=json.loads(__import__('sys').argv[1]);root=Path('/candidate');assert (root.stat().st_uid,root.stat().st_gid,stat.S_IMODE(root.stat().st_mode))==(1000,1000,0o700);[(lambda p,v: (p.read_bytes(),None) if hashlib.sha256(p.read_bytes()).hexdigest()==v['sha256'] and (p.stat().st_uid,p.stat().st_gid,stat.S_IMODE(p.stat().st_mode))==(1000,1000,0o600) else (_ for _ in ()).throw(AssertionError('copied WAL differs')))(root/v['path'],v) for v in items];print('private candidate copy readable')"
       run(['run','--rm','--network','none','--read-only','--user','1000:1000','--cap-drop','ALL','--security-opt','no-new-privileges','--memory','64m',
           '--mount','type=bind,source='+str(candidate_working)+',target=/candidate,readonly',
           '--entrypoint','python',image,'-c',probe,json.dumps(json.loads((candidate_working/'.qt-recovery-copy.json').read_text())['copied_files'])])
      except EOFError as exc:
       assert options.recovery_spool_reply_loss and str(exc)=='owned completed spool copy reply discarded'
       spool_after=final_host._load(state/final_host.STATE)
       assert spool_after['phase']=='recovery_spool_preparing'
       assert spool_after['runtime_spool']['completed']==['create'] and spool_after['runtime_spool']['inflight']=='copy'
       assert spool_after['runtime_spool']['report'] is None
       status=json.loads(run(['inspect',spool_after['runtime_spool']['helper_id'],'--format','{{json .State}}']).stdout)
       assert status['Pid']==0 and not status['Running'] and status['ExitCode']==0
       assert not any(host_boundary.inventory(project,operator_id=receipt['container_id'])[n]['running'] for n in host_boundary.STOP)
       report['recovery_spool_lost_reply']=dict(actual_copy_completed=True,unresolved_intent_retained=True,
           source_not_restarted=True,runtime_activation_authorized=False)
      finally:host_boundary.supervised_source_action=actual_spool_action
      try:
       final_host.prepare_online_runtime_spool_locked(state,worker_process=worker,
           destination=candidate_working,max_bytes=64*1024**2,max_entries=4096,
           reserve_bytes=8*1024**2,max_duration_seconds=15)
       raise AssertionError('completed or uncertain spool copy replay admitted')
      except RuntimeError as exc:assert str(exc)=='storage_online_runtime_spool_live_transition_required'
     if options.recovery_runtime:
      from scripts.automation import storage_online_runtime as runtime_host
      from scripts.automation import storage_online_recovery as recovery_host
      from scripts.ci.online_operation_fixture import write_runtime_recipe
      write_runtime_recipe(state=state,runtime_model=host_boundary.load_receipt(state/recovery_host.RECIPE),
        inventory=inventory,udev=udev,image=image,password=password,dbname=dbname,history=history,
        project=project,candidate_working=candidate_working,owned=owned)
      started_runtime=time.monotonic()
      actual_runtime_action=host_boundary.supervised_source_action
      recovery_spec=json.loads((control/'runtime-recovery.json').read_text())
      def recover_before_maintenance(args,**kwargs):
       current=final_host._load(state/final_host.STATE)
       if current['runtime']['inflight']=='start:storage-maintenance':
        # Test-only transport seam, inside the already admitted ordinary app.
        # Normal recovery/fresh intake completes BEFORE maintenance can back up.
        remaining=min(kwargs['deadline']-time.monotonic(),final_host._remaining(current))
        assert remaining>0
        code="import json,sys;from tests.test_market_data.online_runtime_recovery_fixture import verify;print('QT_CONNECTED_RECOVERY='+json.dumps(verify(json.loads(sys.argv[1]))))"
        recovered=run(['exec',current['runtime']['candidate_ids']['market-data-collector'],
          'python','-c',code,json.dumps(recovery_spec)],timeout=remaining,check=False)
        (state/'runtime-recovery.log').write_text(recovered.stdout+recovered.stderr)
        assert recovered.returncode==0, 'ordinary connected recovery failed; see runtime-recovery.log'
        replies=[json.loads(line.split('=',1)[1]) for line in recovered.stdout.splitlines() if line.startswith('QT_CONNECTED_RECOVERY=')]
        assert len(replies)==1
        report['normal_runtime_recovery']=replies[0]
       return actual_runtime_action(args,**kwargs)
      host_boundary.supervised_source_action=recover_before_maintenance
      try:
       result=final_host.activate_online_runtime_locked(state,worker_process=worker,max_duration_seconds=60)
      finally:host_boundary.supervised_source_action=actual_runtime_action
      activated=final_host._load(state/final_host.STATE)
      assert activated['phase']=='recovery_runtime_ready' and result['collector_process_healthy']
      report['recovery_runtime']=dict(component_seconds=time.monotonic()-started_runtime,
        final_entry_to_applications_ready_seconds=activated['runtime']['finished_at']-activated['started_at'],
        actual_application_entrypoints=True,collector_process_healthy=True,actual_collection_throughput_measured=False,complete_pair_confirmed=False)
      # Process health explicitly permits degraded workers. Require the real
      # lifecycle result as separate evidence; use only the original final clock.
      while True:
       final_host._remaining(activated)
       with host_boundary.docker_deadline(activated['switch']['deadline_monotonic']):
        lifecycle=json.loads(host_boundary.database_query(pgid,
          "SELECT coalesce(jsonb_agg(context->'storage_lifecycle'),'[]'::jsonb)::text FROM market.collector_worker_state WHERE worker_role='market_storage_maintenance'"))
       assert len(lifecycle)==1
       outcome=lifecycle[0].get('last_run')
       if outcome is not None:
        assert outcome['status']=='completed' and outcome['failure_count']==0, outcome
        pair=outcome['local_recovery']
        assert pair['state']=='completed' and pair['policy_hash']==recovery_spec['policy_hash'], pair
        code="import json,sys;from pathlib import Path;paths=list(Path('/qt-history/recovery-incremental').glob('*/'+sys.argv[1]+'/complete.json'));assert len(paths)==1;print(json.dumps(json.loads(paths[0].read_text())))"
        remaining=final_host._remaining(activated)
        observed=run(['exec',activated['runtime']['candidate_ids']['storage-maintenance'],'python','-c',code,pair['generation']],timeout=remaining)
        certificate=json.loads(observed.stdout)
        assert certificate['schema_version']=='qt.encrypted_recovery_pair.v1' and certificate['name']==pair['generation']
        assert certificate['database_type']=='full' and certificate['archive_objects']>0
        (state/'runtime-pair-certificate.json').write_text(json.dumps(certificate))
        report['recovery_runtime'].update(maintenance_outcome=outcome,complete_pair_confirmed=True)
        break
       time.sleep(.2)
      try:
       final_host.activate_online_runtime_locked(state,worker_process=worker,max_duration_seconds=30)
       raise AssertionError('runtime replay admitted')
      except RuntimeError as exc:assert str(exc)=='storage_online_runtime_live_transition_required'
     source_holds.close()
     report['held_database_commit']['read_worker_pid0_before_hold_release']=True
    # Fixture teardown only, AFTER verified worker retirement. This does not
    # authorize production gate restoration or remove its retained final marker.
    with host_boundary.docker_deadline(time.monotonic()+5):
     host_boundary.maintenance_query(pgid,"SELECT format('ALTER DATABASE %I ALLOW_CONNECTIONS true',datname) FROM pg_database WHERE datname=:'target'\n\\gexec\n")
    report['login_gate']['worker_retired_before_fixture_restore']=True
   if not options.recovery_runtime:
    assert host_boundary.identities(host_boundary.inventory(project,operator_id=receipt['container_id']))==source
  if not options.final_pause:
   # A host exception must close only its background worker; source keeps serving.
   class HostInterrupted(RuntimeError):
    pass
   try:
    with launch.launched_online_worker(state,**kwargs) as (worker,receipt):
     channel=host_boundary.OnlineWorkerChannel(worker,deadline=time.monotonic()+receipt['deadline']-time.time());greeting=channel.greeting
     assert greeting['background_hashed_bytes']==0
     assert receipt['deadline']==first_deadline
     raise HostInterrupted()
   except HostInterrupted:
    pass
   assert worker.returncode==0
   assert host_boundary.identities(host_boundary.inventory(project,operator_id=first_id))==source
   assert not json.loads(run(['inspect',first_id,'--format','{{json .State}}']).stdout)['Running']
   report['host_exception_stopped_only_worker']=True
   assert original_source_metadata==[working.stat().st_uid,working.stat().st_gid,working.stat().st_mode]
   saved=(state/launch._STATE).read_bytes()
   try:
    with launch.launched_online_worker(state,**(kwargs|dict(descriptor_limit=2048))):
     raise AssertionError('changed resource binding admitted')
   except RuntimeError as exc:
    assert str(exc)=='storage_online_saved_launch_changed'
   assert (state/launch._STATE).read_bytes()==saved
   report['original_deadline_preserved']=True
   report['first_process_hashed_bytes']=final['background_hashed_bytes']
   report['source_clients_unchanged']=True
  else:
   try:
    with launch.launched_online_worker(state,**kwargs):
     raise AssertionError('final intent permitted ordinary worker relaunch')
   except RuntimeError as exc:
    assert str(exc)=='storage_online_final_requires_reconciliation'
   report['final_intent_blocks_relaunch']=True
   assert original_source_metadata==[working.stat().st_uid,working.stat().st_gid,working.stat().st_mode]
   report['first_process_hashed_bytes']=final['background_hashed_bytes']
   report['source_clients_unchanged']=not options.recovery_runtime
  if options.prepare_source:
   assert (state/initial.STATE).read_bytes()==prepared_bytes
   if options.final_pause and not options.recovery_mounts:
    initial._admit_source(state,preparation,require_running=False,operator_id=first_id)
   elif not options.final_pause:
    assert initial.admit_serving_source(state,project=project,source_revision=revision,operator_id=first_id)==preparation
   assert (working/'objects'/'native-intake').stat().st_size>intake_before
   assert not host_boundary.inventory(project,operator_id=first_id,**(dict(activating=True,runtime_maintenance=True) if options.recovery_runtime else {}))['initialize']['running']
   report['initial_to_worker_receipt_admission']=True
   report['synthetic_intake_continued']=True
  if options.final_pause:
   assert host_boundary.database_query(pgid,frozen_sql)==before_final_frozen
   if options.recovery_mounts:
    retained=json.loads(host_boundary.database_query(pgid,'SELECT to_jsonb(c)::text FROM qt_fact_header_cutover_v2.capture c WHERE id=1'))
    assert retained==final_host._load(state/final_host.STATE)['binding']['capture']
   report['frozen_and_original_capture_retained_after_final']=True
  report['durable_request_and_receipt_retained']=True
  (control/'finished').write_text('finished');fixture.wait(timeout=30);log.close();assert fixture.returncode==0
  report.update(passed=True,image=image,first_process_commands=channel.sequence if options.final_pause else final['last_sequence']+(0 if options.worker_attach_loss else 1),final_status=final,source_owner=working.stat().st_uid,fixture_seconds=time.monotonic()-started)

except BaseException as exc:
 if (options.recovery_create_reply_loss and isinstance(exc,EOFError)
     and str(exc)=='owned completed recovery create reply discarded'):
  saved=final_host._load(state/final_host.STATE)
  assert saved['phase']=='recovery_preparing'
  assert saved['recovery']['completed']==['stop','remove'] and saved['recovery']['inflight']=='create'
  pending=host_boundary.inventory(project,database_preparing=True,operator_id=receipt['container_id'])
  assert all(not pending[n]['running'] for n in host_boundary.STOP)
  assert pending['tsdb']['status']=='created' and not pending['tsdb']['running'] and pending['tsdb']['pid']==0
  assert pending['tsdb']['id']!=saved['recovery']['original_id']
  retired=json.loads(run(['inspect',receipt['container_id'],'--format','{{json .State}}']).stdout)
  assert retired['Pid']==0 and not retired['Running'] and worker.poll() is not None
  before=(state/final_host.STATE).read_bytes()
  try:
   final_host.prepare_recovery_database_locked(state,worker_process=worker,
       keys_root=recovery_keys,socket_volume=recovery_socket,max_duration_seconds=30)
   raise AssertionError('uncertain recovery creation replay admitted')
  except RuntimeError as refusal:assert str(refusal)=='storage_online_recovery_committed_live_hold_required'
  assert (state/final_host.STATE).read_bytes()==before
  report.update(passed=True,recovery_create_reply_loss=dict(actual_creation_completed=True,
      replacement_not_started=True,inflight_retained=True,replay_refused=True,
      reader_retired=True,source_not_restarted=True,runtime_activation_authorized=False),
      fixture_seconds=time.monotonic()-started)
 elif (options.worker_shutdown=='fail'  and str(exc)=='storage_pause_unclean_stop: service=market-data-collector'
     and report.get('failed_drain_live_proof_retained_before_exit')):
  # Expected refusal from both final admission and launcher exit. Never restart
  # failed source, suppress the guard, or turn stopped status into drain authority.
  saved=final_host._load(state/final_host.STATE)
  assert saved['phase']=='stopping' and saved['deadline']==first['deadline']
  assert saved['deadline_boot']==first['deadline_boot']
  assert (state/initial.STATE).read_bytes()==prepared_bytes
  collector=project+'-market-data-collector'
  stopped=json.loads(run(['inspect',collector,'--format','{{json .State}}']).stdout)
  assert stopped['ExitCode']==5 and not stopped['Running'] and not stopped['OOMKilled']
  assert (working/'objects'/'lifecycle-stopped').exists() and (working/'objects'/'heartbeat-stopped').exists()
  assert (working/'spool'/'qt-signal-owned-fixture'/'pending.sealed').read_bytes()==b'owned failed-finalizer WAL fixture'
  logs=run(['logs','--tail','120',collector]);(state/'worker-shutdown.log').write_text(logs.stdout+logs.stderr)
  assert 'market_data_collector_shutdown_failed' in logs.stdout+logs.stderr
  assert not json.loads(run(['inspect',first_id,'--format','{{json .State}}']).stdout)['Running']
  try:
   with launch.launched_online_worker(state,**kwargs):
    raise AssertionError('failed final drain permitted ordinary relaunch')
  except RuntimeError as refusal:
   assert str(refusal)=='storage_online_final_requires_reconciliation'
  (control/'finished').write_text('finished');fixture.wait(timeout=30);log.close();assert fixture.returncode==0
  report.update(passed=True,expected_unclean_stop_refusal=True,final_intent_blocks_relaunch=True,
    failed_source_not_restarted=True,pending_spool_preserved=True,final_original_deadline_preserved=True,
    docker_worker_shutdown=dict(exit_code=5,lifecycle_and_heartbeat_stopped=True,controlled_adapter=True),
    image=image,fixture_seconds=time.monotonic()-started,final_switch_authorized=False,
    collection_resume_authorized=False)
 else:
  report.update(passed=False,error_type=type(exc).__name__,error=str(exc))
  raise
finally:
 try:source_holds.close()
 except RuntimeError as exc:
  report['source_hold_exit_error']=str(exc)
  report['passed']=False
 if options.worker_shutdown:
  diagnostic=run(['logs','--tail','160',project+'-market-data-collector'],check=False)
  (state/'worker-shutdown.log').write_text(diagnostic.stdout+diagnostic.stderr)
 if options.recovery_runtime:
  for service_name in ('initialize','backend','market-data-collector','storage-maintenance',*(('pgadmin',) if canonical else ())):
   diagnostic=run(['logs','--tail','120',project+'-'+service_name+'-1'],check=False)
   (state/('runtime-'+service_name+'.log')).write_text(diagnostic.stdout+diagnostic.stderr)
   diagnostic=run(['inspect',project+'-'+service_name+'-1','--format','{{json .State}}'],check=False)
   (state/('runtime-'+service_name+'-state.json')).write_text(diagnostic.stdout)
 cleanup_failures=[]
 if canonical:
  owned.extend(run(['ps','-aq','--filter','label=com.docker.compose.project='+project]).stdout.split())
 for name in reversed(owned):
  observed=run(['inspect',name,'--format','{{json .}}'],check=False)
  if observed.returncode:continue
  details=json.loads(observed.stdout)
  mine=details['Config']['Labels'].get('qt.disposable')==project
  if canonical and details['Config']['Labels'].get('com.docker.compose.project')==project:mine=True
  if name==project+'-storage-online' and (state/launch._STATE).exists():
   mine=json.loads((state/launch._STATE).read_text())['container_id']==details['Id']
  if name==project+'-storage-spool-prepare':
   mine=details['Config']['Labels'].get('qt.storage-spool-operation')==final_host._load(state/final_host.STATE)['binding']['controller_id']
  if name==project+'-storage-repository-prepare':
   mine=details['Config']['Labels'].get('com.docker.compose.project')==project+'-recovery' and details['Config']['Labels'].get('com.docker.compose.service')=='prepare'
  if not mine or run(['rm','-f',details['Id']],check=False).returncode:cleanup_failures.append(name)
 if canonical:
  for name in run(['volume','ls','-q','--filter','label=com.docker.compose.project='+project]).stdout.split():
   details=json.loads(run(['volume','inspect',name]).stdout)[0]
   assert details['Labels'].get('com.docker.compose.project')==project
   run(['volume','rm',name])
 if created_volume:run(['volume','rm',volume])
 if created_recovery_socket:run(['volume','rm',recovery_socket])
 if created_network:run(['network','rm',network])
 if created_history:
  assert history.parent==history_parent and history.name==project
  cleanup=project+'-cleanup'
  r=run(['run','--rm','--name',cleanup,'--user','0:0','--network','none','--memory','64m','--cpus','0.25','--mount','type=bind,source='+str(history)+',target=/h','--mount','type=bind,source='+str(state)+',target=/s','--entrypoint','python',options.image,'-c',"import shutil;from pathlib import Path;[(shutil.rmtree(p) if p.is_dir() else p.unlink()) for p in Path('/h').iterdir()];[shutil.rmtree(Path('/s')/n) for n in ('working','control','fixture-recovery-keys','candidate-working') if (Path('/s')/n).exists()]"],check=False)
  report['cleanup_exit_code']=r.returncode
  if r.returncode==0:history.rmdir()
 report['remaining_containers']=run(['ps','-aq','--filter','name='+project]).stdout.strip()
 report['cleanup_failures']=cleanup_failures
 if cleanup_failures or report['remaining_containers'] or report.get('cleanup_exit_code',0):report['passed']=False
 (state/'outcome.json').write_text(json.dumps(report,indent=2))
 print('QT_PREPARED_HOST_OUTCOME='+json.dumps(report),flush=True)
 if cleanup_failures or report['remaining_containers'] or report.get('cleanup_exit_code',0):raise RuntimeError('owned_host_fixture_cleanup_failed')
