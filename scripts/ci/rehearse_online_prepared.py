"""Owned prepared-host launcher rehearsal with real QT publication.

Synthetic service peers; optional owned initial pause, no production or final switch.
All created containers, volume, network and scratch files are owned by this run.
"""
import argparse,json,os,select,subprocess,sys,time,uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--image',required=True)
parser.add_argument('--database-image',required=True)
parser.add_argument('--output-root',type=Path,required=True)
parser.add_argument('--history-parent',type=Path,required=True)
parser.add_argument('--require-distinct-devices',action='store_true')
parser.add_argument('--prepare-source',action='store_true',help='qualify retained initial preparation before atomic capture and launch')
parser.add_argument('--final-pause',action='store_true',help='qualify interrupted final source stop only; no switch or resumption')
parser.add_argument('--worker-phases',action='store_true',help='drive explicit preparation through the launched worker pipe')
options=parser.parse_args()
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
created_network=False
created_history=False
owned=[];volume=project+'-pg';created_volume=False
started=time.monotonic()
report={'project':project,'production_inputs':False,'full_host_pause_qualified':False,
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
 image=run(['image','inspect',options.image,'--format','{{.Id}}']).stdout.strip()
 envvalues=dict(v.split('=',1) for v in json.loads(run(['image','inspect',image,'--format','{{json .Config.Env}}']).stdout))
 revision=envvalues['QT_IMAGE_SOURCE_REVISION']
 (state/'release.env').write_text('current_revision='+revision+'\n')
 def labels(service):
  return ['--label','com.docker.compose.project='+project,'--label','com.docker.compose.service='+service,'--label','com.docker.compose.oneoff=False']
 pg=run(['image','inspect',options.database_image,'--format','{{.Id}}']).stdout.strip()
 prep=project+'-prepare';owned.append(prep)
 run(['run','--name',prep,'--user','0:0','--network','none','--memory','64m','--cpus','0.25',
      '--mount','type=bind,source='+str(history)+',target=/h',
      '--mount','type=bind,source='+str(working)+',target=/s','--entrypoint','python',image,'-c',
      "import os;os.mkdir('/s/objects');[(os.chown(p,70,70),os.chmod(p,0o755)) for p in ('/h','/s','/s/objects')];os.chmod('/h',0o777)"])
 assert run(['volume','inspect',volume],check=False).returncode != 0
 run(['volume','create','--label','qt.disposable='+project,volume]);created_volume=True
 run(['network','create','--internal',network]);created_network=True
 pgname=project+'-db';owned.append(pgname)
 password=uuid.uuid4().hex
 dbname='qt_migration_online_'+uuid.uuid4().hex[:16]
 if options.prepare_source:
  pgname=project+'-tsdb-1';owned.append(pgname)
  service=dict(image=pg,pull_policy='never',hostname='tsdb.quanttrad',
    command=['postgres','-c','shared_buffers=32MB','-c','timescaledb.telemetry_level=off'],
    environment=dict(POSTGRES_USER='fixture',POSTGRES_DB=dbname,POSTGRES_PASSWORD=password,PGDATA='/var/lib/postgresql/data'),
    init=True,restart='no',shm_size=134217728,labels={'qt.disposable':project},
    healthcheck=dict(test=launch.held._TCP_PROBE,interval='1s',timeout='2s',retries=90,start_period='10s'),
    volumes=[dict(type='volume',source='postgres-data',target='/var/lib/postgresql/data')],
    networks={'quanttrad':{'aliases':['tsdb.quanttrad']}})
  model=dict(name=project,services={'tsdb':service},
    volumes={'postgres-data':dict(name=volume,external=True)},
    networks={'quanttrad':dict(name=network,external=True)})
  source_recipe=state/'source.compose.json';source_recipe.write_text(json.dumps(model));source_recipe.chmod(0o600)
  recipe=json.loads(json.dumps(model))
  recipe['services']['tsdb']['volumes'].append(dict(type='bind',source=str(history),target='/qt-history',bind=dict(create_host_path=False)))
  target_recipe=state/launch.held.DATABASE_RECIPE;target_recipe.write_text(json.dumps(recipe));target_recipe.chmod(0o600)
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
 for service in launch.held.STOP+launch.held.PASSIVE:
  if service=='tsdb' or (service=='market-data-collector' and not options.prepare_source):continue
  name=project+'-'+service;owned.append(name)
  extra=[]
  command_text='exit 0' if service=='initialize' else "trap 'exit 0' TERM; while :; do sleep 1 & wait $!; done"
  if service=='market-data-collector':
   extra=['--mount','type=bind,source='+str(working)+',target=/app/logs/market-structure','--env','PG_DSN=postgresql+psycopg2://fixture:'+password+'@tsdb:5432/'+dbname]
   command_text="trap 'exit 0' TERM; while :; do printf x >> /app/logs/market-structure/objects/native-intake; sleep 1 & wait $!; done"
  run(['run','-d','--name',name,'--pull','never','--network',network,
    '--user','70:70','--read-only','--memory','32m','--cpus','0.1',
    '--pids-limit','32',*labels(service),*extra,'--entrypoint','sh',image,'-c',command_text])
 if options.prepare_source:
  udev=state/'initial-udev';udev.mkdir()
  device=history.stat().st_dev
  (udev/f'b{os.major(device)}:{os.minor(device)}').write_text('E:ID_FS_UUID=uuid-copy-hdd\n')
  os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
  original_cluster=launch.held._cluster_identifier(pgid)
  preparation=initial.prepare_online_source(state,project=project,source_revision=revision,history_uuid='uuid-copy-hdd')
  prepared_bytes=(state/initial.STATE).read_bytes()
  assert preparation['phase']=='serving' and preparation['deadline']-preparation['started_at']==600
  assert initial.prepare_online_source(state,project=project,source_revision=revision,history_uuid='uuid-copy-hdd')==preparation
  pgid=launch.held._inventory(project)['tsdb']['id']
  assert launch.held._cluster_identifier(pgid)==original_cluster
  assert not launch.held._inventory(project)['initialize']['running']
  intake_before=(working/'objects'/'native-intake').stat().st_size
  report['initial_preparation_seconds']=preparation['completed_at']-preparation['started_at']
  report['initial_preparation_receipt_retained']=True
 test=project+'-collector';owned.append(test)
 args=['docker','run','--name',test,'--label','qt.disposable='+project,'--user','70:70','--network','container:'+pgid,'--pid','container:'+pgid,
   *(['--label','com.docker.compose.project='+project+'-application','--label','com.docker.compose.service=fixture'] if options.prepare_source else labels('market-data-collector')),'--memory','2g','--cpus','2','--mount','type=volume,source='+volume+',target=/var/lib/postgresql/data',
   '--mount','type=bind,source='+str(history)+',target=/qt-history',
   '--mount','type=bind,source='+str(working)+',target=/app/logs/market-structure',
   '--mount','type=bind,source='+str(control)+',target=/qt-control',
   '--env','PG_DSN','--env','QT_DISABLE_DOTENV=1','--env','QT_LOGGING_LOKI_URL=',
   '--env','QT_STORAGE_DEMO=1','--env','QT_DB_TEST_ISOLATED=1','--env','RUN_DB_TESTS=1',
   '--env','QT_ONLINE_WORKER_PHASES='+str(int(options.worker_phases)),'--env','QT_ONLINE_ATOMIC_PREPARE='+str(int(options.prepare_source)),'--env','QT_ONLINE_HOST_FIXTURE=1','--env','QT_ONLINE_ENTRYPOINT_FIXTURE=1','--entrypoint','python',image,'-m','pytest','-q','-s',
   '--basetemp','/qt-control/testtmp','-o','cache_dir=/tmp/qt-entry-pytest',
   'tests/test_market_data/test_storage_online_entrypoint_db.py']
 log=(state/'fixture.log').open('w')
 fixture=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT,env={**os.environ,'PG_DSN':'postgresql+psycopg2://fixture:'+password+'@tsdb:5432/'+dbname})
 until=time.monotonic()+180
 def waitfile(name):
  while not (control/name).exists():
   if fixture.poll() is not None or time.monotonic()>until:raise RuntimeError('fixture_wait_failed: '+name+' '+(state/'fixture.log').read_text()[-2500:])
   time.sleep(.1)
 waitfile('ready.json')
 request=json.loads((control/'request.json').read_text())
 inventory=state/'inventory.json';inventory.write_bytes((control/'inventory.json').read_bytes())
 udev=control/Path(json.loads((control/'ready.json').read_text())['udev']).relative_to('/qt-control')
 os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
 source=launch.held._identities(launch.held._inventory(project))
 original_source_metadata=[working.stat().st_uid,working.stat().st_gid,working.stat().st_mode]
 kwargs=dict(project=project,source_revision=revision,image=image,request=request,
             inventory_path=inventory,descriptor_limit=1024,memory_bytes=1024**3)
 name=project+'-storage-online';owned.append(name)
 def read():
  data=b'';deadline=time.monotonic()+40
  while not data.endswith(b'\n'):
   if time.monotonic()>deadline or len(data)>16384:raise RuntimeError('worker_protocol_budget')
   if select.select([worker.stdout],[],[],max(.001,deadline-time.monotonic()))[0]:
    piece=os.read(worker.stdout.fileno(),1)
    if not piece:raise RuntimeError('worker_eof')
    data+=piece
  return json.loads(data)
 sequence=0
 def command(op,**extra):
  global sequence
  sequence+=1;worker.stdin.write((json.dumps(dict(controller_id=greeting['controller_id'],sequence=sequence,operation=op,**extra))+'\n').encode());return read()
 first_deadline=None
 for attempt in range(1 if options.final_pause else 2):
  with launch.launched_online_worker(state,**kwargs) as (worker,receipt):
   greeting=read();sequence=0
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
     for _ in range(64):
      reply=command('sql_copy')
      outcome=reply['result']['outcome']
      if outcome=='identity_relocation_required':phase('identity_history')
      elif outcome=='raw_relocation_required':phase('raw_history')
      elif outcome=='both_tails_observed_empty' and (control/'published').exists():break
     else:raise RuntimeError('tiny_worker_phases_did_not_converge')
     phase('identity_capture')
     (control/'inspect-references').write_text('inspect')
     waitfile('references.json')
     relations=json.loads((control/'references.json').read_text())
     assert isinstance(relations,list) and len(relations)<=128
     for relation in relations:
      phase('reference_prepare',relation);phase('reference_validate',relation)
     phase('reference_adopt')
     (control/'phases-finished').write_text('finished')
     waitfile('catalogs-moved')
     report['explicit_preparation_through_worker']=True
     report['worker_reference_relations']=len(relations)
    command('archive_copy')
    waitfile('published')
    for i in range(30):
     command('archive_copy');command('reprove');reply=command('sql_copy')
     if reply['result']['outcome']=='both_tails_observed_empty' and len(reply['reproved_families_at_observation'])==3:break
    else:raise RuntimeError('tiny_host_tails_did_not_converge')
    final=command('status');assert final['background_hashed_bytes']>0
    first_id=receipt['container_id']
    if options.final_pause:
     from scripts.automation import storage_online_final as final_host
     import multiprocessing,signal
     pause_args=dict(project=project,source_revision=revision,worker_id=first_id,
                     controller_id=greeting['controller_id'],max_duration_seconds=60)
     def interrupt_first_stop():
      original=launch.held._docker
      def stop_then_die(*args,**kwargs):
       value=original(*args,**kwargs)
       if args[0]=='stop':
        assert final_host._load(state/final_host.STATE)['phase']=='stopping'
        os.kill(os.getpid(),signal.SIGKILL)
       return value
      launch.held._docker=stop_then_die
      final_host.stop_online_source_locked(state,**pause_args)
     child=multiprocessing.get_context('fork').Process(target=interrupt_first_stop)
     child.start();child.join(timeout=30)
     if child.is_alive():
      child.kill();child.join(timeout=10)
      raise RuntimeError('owned final pause child exceeded fixture deadline')
     assert child.exitcode == -signal.SIGKILL
     first=final_host._load(state/final_host.STATE)
     assert first['phase']=='stopping'
     rows=launch.held._inventory(project,operator_id=first_id)
     assert sum(not rows[n]['running'] for n in launch.held.STOP)==2
     paused=final_host.stop_online_source_locked(state,**pause_args)
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
     rows=launch.held._inventory(project,operator_id=first_id)
     assert not any(rows[n]['running'] for n in launch.held.STOP)
     assert all(rows[n]['running'] for n in launch.held.PASSIVE)
     assert command('status')['controller_id']==greeting['controller_id']
     report['final_stop_interrupted_reentry']=True
     report['final_stop_seconds']=paused['paused_at']-paused['started_at']
     report['final_original_deadline_preserved']=True
     report['final_stopped_only_exact_clients']=True
     report['final_worker_and_proof_retained']=True
     report['final_source_resumption_qualified']=False
   else:
    assert greeting['controller_id']!=previous_id
    assert receipt['container_id']==first_id and receipt['deadline']==first_deadline
    report['reentry_empty_proof']=True
   command('close')
  assert worker.returncode==0
  assert launch.held._identities(launch.held._inventory(project,operator_id=receipt['container_id']))==source
 if not options.final_pause:
  # A host exception must close only its background worker; source keeps serving.
  class HostInterrupted(RuntimeError):
   pass
  try:
   with launch.launched_online_worker(state,**kwargs) as (worker,receipt):
    greeting=read();sequence=0
    assert greeting['background_hashed_bytes']==0
    assert receipt['deadline']==first_deadline
    raise HostInterrupted()
  except HostInterrupted:
   pass
  assert worker.returncode==0
  assert launch.held._identities(launch.held._inventory(project,operator_id=first_id))==source
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
  report['source_clients_unchanged']=True
 if options.prepare_source:
  assert (state/initial.STATE).read_bytes()==prepared_bytes
  if options.final_pause:
   initial._admit_source(state,preparation,require_running=False,operator_id=first_id)
  else:
   assert initial.admit_serving_source(state,project=project,source_revision=revision,operator_id=first_id)==preparation
  assert (working/'objects'/'native-intake').stat().st_size>intake_before
  assert not launch.held._inventory(project,operator_id=first_id)['initialize']['running']
  report['initial_to_worker_receipt_admission']=True
  report['synthetic_intake_continued']=True
 report['durable_request_and_receipt_retained']=True
 (control/'finished').write_text('finished');fixture.wait(timeout=30);log.close();assert fixture.returncode==0
 report.update(passed=True,image=image,first_process_commands=final['last_sequence']+1,final_status=final,source_owner=working.stat().st_uid,fixture_seconds=time.monotonic()-started)
except BaseException as exc:
 report.update(passed=False,error_type=type(exc).__name__,error=str(exc))
 raise
finally:
 cleanup_failures=[]
 for name in reversed(owned):
  observed=run(['inspect',name,'--format','{{json .}}'],check=False)
  if observed.returncode:continue
  details=json.loads(observed.stdout)
  mine=details['Config']['Labels'].get('qt.disposable')==project
  if name==project+'-storage-online' and (state/launch._STATE).exists():
   mine=json.loads((state/launch._STATE).read_text())['container_id']==details['Id']
  if not mine or run(['rm','-f',details['Id']],check=False).returncode:cleanup_failures.append(name)
 if created_volume:run(['volume','rm',volume])
 if created_network:run(['network','rm',network])
 if created_history:
  assert history.parent==history_parent and history.name==project
  cleanup=project+'-cleanup'
  r=run(['run','--rm','--name',cleanup,'--user','0:0','--network','none','--memory','64m','--cpus','0.25','--mount','type=bind,source='+str(history)+',target=/h','--mount','type=bind,source='+str(state)+',target=/s','--entrypoint','python',options.image,'-c',"import shutil;from pathlib import Path;[(shutil.rmtree(p) if p.is_dir() else p.unlink()) for p in Path('/h').iterdir()];[shutil.rmtree(Path('/s')/n) for n in ('working','control') if (Path('/s')/n).exists()]"],check=False)
  report['cleanup_exit_code']=r.returncode
  if r.returncode==0:history.rmdir()
 report['remaining_containers']=run(['ps','-aq','--filter','name='+project]).stdout.strip()
 report['cleanup_failures']=cleanup_failures
 if cleanup_failures or report['remaining_containers'] or report.get('cleanup_exit_code',0):report['passed']=False
 (state/'outcome.json').write_text(json.dumps(report,indent=2))
 print('QT_PREPARED_HOST_OUTCOME='+json.dumps(report),flush=True)
 if cleanup_failures or report['remaining_containers'] or report.get('cleanup_exit_code',0):raise RuntimeError('owned_host_fixture_cleanup_failed')
