"""Owned prepared-host launcher rehearsal with real QT publication.

Synthetic service peers; optional owned initial pause, no production or final switch.
All created containers, volume, network and scratch files are owned by this run.
"""
import argparse,json,os,select,subprocess,sys,time,uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from scripts.automation import storage_host_boundary as host_boundary
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--image',required=True)
parser.add_argument('--database-image',required=True)
parser.add_argument('--output-root',type=Path,required=True)
parser.add_argument('--history-parent',type=Path,required=True)
parser.add_argument('--require-distinct-devices',action='store_true')
parser.add_argument('--prepare-source',action='store_true',help='qualify retained initial preparation before atomic capture and launch')
parser.add_argument('--final-pause',action='store_true',help='qualify interrupted final source stop only; no switch or resumption')
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
options=parser.parse_args()
if options.close_logins and (not options.switch_entry or options.abort_resume):
 parser.error('--close-logins requires switch entry and excludes source resumption')
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
    healthcheck=dict(test=initial.held._TCP_PROBE,interval='1s',timeout='2s',retries=90,start_period='10s'),
    volumes=[dict(type='volume',source='postgres-data',target='/var/lib/postgresql/data')],
    networks={'quanttrad':{'aliases':['tsdb.quanttrad']}})
  model=dict(name=project,services={'tsdb':service},
    volumes={'postgres-data':dict(name=volume,external=True)},
    networks={'quanttrad':dict(name=network,external=True)})
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
  if service=='market-data-collector' and options.worker_shutdown:
   import ast
   fixture_tree=ast.parse((Path(__file__).resolve().parents[2]/"tests/test_market_data/test_collector_shutdown_signal.py").read_text())
   SCRIPT=next(ast.literal_eval(node.value) for node in fixture_tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="SCRIPT" for t in node.targets))
   extra += ['--env','QT_SIGNAL_HOST_FIXTURE=1','--env','QT_SIGNAL_REAL_PUBLICATION='+str(int(options.real_worker_publication)),'--env','QT_DISABLE_DOTENV=1',
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
 if options.prepare_source:
  udev=state/'initial-udev';udev.mkdir()
  device=history.stat().st_dev
  (udev/f'b{os.major(device)}:{os.minor(device)}').write_text('E:ID_FS_UUID=uuid-copy-hdd\n')
  os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
  original_cluster=host_boundary.cluster_identifier(pgid)
  preparation=initial.prepare_online_source(state,project=project,source_revision=revision,history_uuid='uuid-copy-hdd')
  prepared_bytes=(state/initial.STATE).read_bytes()
  assert preparation['phase']=='serving' and preparation['deadline']-preparation['started_at']==600
  assert initial.prepare_online_source(state,project=project,source_revision=revision,history_uuid='uuid-copy-hdd')==preparation
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
   '--env','PG_DSN','--env','QT_DISABLE_DOTENV=1','--env','QT_LOGGING_LOKI_URL=',
   '--env','QT_STORAGE_DEMO=1','--env','QT_DB_TEST_ISOLATED=1','--env','RUN_DB_TESTS=1',
   '--env','QT_SIGNAL_REAL_PUBLICATION='+str(int(options.real_worker_publication)),'--env','QT_ONLINE_FINAL_DELTA='+str(int(options.final_delta)),'--env','QT_ONLINE_WORKER_PHASES='+str(int(options.worker_phases)),'--env','QT_ONLINE_ATOMIC_PREPARE='+str(int(options.prepare_source)),'--env','QT_ONLINE_HOST_FIXTURE=1','--env','QT_ONLINE_ENTRYPOINT_FIXTURE=1','--entrypoint','python',image,'-m','pytest','-q','-s',
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
 if options.real_worker_publication:
  received_deadline=time.monotonic()+30
  while not (working/'objects'/'real-frame-received').exists():
   if time.monotonic()>received_deadline:raise RuntimeError('real Docker collector did not receive frame')
   time.sleep(.05)
 request=json.loads((control/'request.json').read_text())
 inventory=state/'inventory.json';inventory.write_bytes((control/'inventory.json').read_bytes())
 udev=control/Path(json.loads((control/'ready.json').read_text())['udev']).relative_to('/qt-control')
 os.environ['QT_STORAGE_UDEV_ROOT']=str(udev)
 source=host_boundary.identities(host_boundary.inventory(project))
 original_source_metadata=[working.stat().st_uid,working.stat().st_gid,working.stat().st_mode]
 kwargs=dict(project=project,source_revision=revision,image=image,request=request,
             inventory_path=inventory,descriptor_limit=1024,memory_bytes=1024**3)
 name=project+'-storage-online';owned.append(name)
 def read(deadline=None):
  data=b'';deadline=deadline if deadline is not None else time.monotonic()+40
  while not data.endswith(b'\n'):
   if time.monotonic()>deadline or len(data)>16384:raise RuntimeError('worker_protocol_budget')
   if select.select([worker.stdout],[],[],max(.001,deadline-time.monotonic()))[0]:
    piece=os.read(worker.stdout.fileno(),1)
    if not piece:raise RuntimeError('worker_eof')
    data+=piece
  return json.loads(data)
 sequence=0
 def command(op,*,response_deadline=None,**extra):
  global sequence
  sequence+=1;worker.stdin.write((json.dumps(dict(controller_id=greeting['controller_id'],sequence=sequence,operation=op,**extra))+'\n').encode())
  reply=read(extra.get('deadline',response_deadline))
  return reply
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
                     controller_id=greeting['controller_id'],max_duration_seconds=60)
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
        try:final_host.close_database_logins_locked(state,exchange=command);raise AssertionError('gate replay')
        except RuntimeError as exc:assert str(exc)=='storage_online_login_switch_intent_required'
        try:final_host.resume_online_source_locked(state,exchange=command);raise AssertionError('gate source resume')
        except RuntimeError as exc:assert str(exc)=='storage_online_resume_switch_intent_required'
        assert (state/final_host.STATE).read_bytes()==held_bytes
        report['login_gate']=dict(mode=options.close_logins,phase=gated['phase'],same_worker=True,
          new_logins_refused=True,fresh_negative_without_authority=True,original_clocks_preserved=True,
          replay_refused=True,source_resumption_refused=True,elapsed_seconds=time.monotonic()-gate_started)
        (state/'login-gate-intent.json').write_text(json.dumps(gated,indent=2))
       if options.abort_resume:
        resume_started=time.monotonic()
        if options.abort_resume_fence_loss:
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
  if options.close_logins:
   retired=json.loads(run(['inspect',receipt['container_id'],'--format','{{json .State}}']).stdout)
   assert not retired['Running'] and retired['Pid']==0
   # Fixture teardown only, AFTER verified worker retirement. This does not
   # authorize production gate restoration or remove its retained final marker.
   with host_boundary.docker_deadline(time.monotonic()+5):
    host_boundary.maintenance_query(pgid,"SELECT format('ALTER DATABASE %I ALLOW_CONNECTIONS true',datname) FROM pg_database WHERE datname=:'target'\n\\gexec\n")
   report['login_gate']['worker_retired_before_fixture_restore']=True
  assert host_boundary.identities(host_boundary.inventory(project,operator_id=receipt['container_id']))==source
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
  report['source_clients_unchanged']=True
 if options.prepare_source:
  assert (state/initial.STATE).read_bytes()==prepared_bytes
  if options.final_pause:
   initial._admit_source(state,preparation,require_running=False,operator_id=first_id)
  else:
   assert initial.admit_serving_source(state,project=project,source_revision=revision,operator_id=first_id)==preparation
  assert (working/'objects'/'native-intake').stat().st_size>intake_before
  assert not host_boundary.inventory(project,operator_id=first_id)['initialize']['running']
  report['initial_to_worker_receipt_admission']=True
  report['synthetic_intake_continued']=True
 if options.final_pause:
  assert host_boundary.database_query(pgid,frozen_sql)==before_final_frozen
  report['frozen_and_original_capture_retained_after_final']=True
 report['durable_request_and_receipt_retained']=True
 (control/'finished').write_text('finished');fixture.wait(timeout=30);log.close();assert fixture.returncode==0
 report.update(passed=True,image=image,first_process_commands=sequence if options.final_pause else final['last_sequence']+(0 if options.worker_attach_loss else 1),final_status=final,source_owner=working.stat().st_uid,fixture_seconds=time.monotonic()-started)
except BaseException as exc:
 if (options.worker_shutdown=='fail' and str(exc)=='storage_pause_unclean_stop: service=market-data-collector'
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
 if options.worker_shutdown:
  diagnostic=run(['logs','--tail','160',project+'-market-data-collector'],check=False)
  (state/'worker-shutdown.log').write_text(diagnostic.stdout+diagnostic.stderr)
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
