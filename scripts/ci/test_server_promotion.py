#!/usr/bin/env python3
"""Real deployment controller with synthetic services, real Docker/Git/volumes.

GitHub API responses are a local fixture. This does not prove QT schema or
provider behavior; the production-image smoke and DB suites cover those seams.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import zipfile
from datetime import datetime, timedelta, timezone
import uuid

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ('tsdb', 'backend', 'initialize', 'market-data-collector', 'docker-stats', 'frontend', 'frontend-v2')

# Synthetic admission evidence only; the rehearsal never calls GitHub.
GITHUB_API_FIXTURE = r'''#!/usr/bin/env python3
import json, os, sys, hashlib, zipfile
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
path = sys.argv[-1]
identity = Path(__file__).with_suffix('.sha')
repository = 'elijahbrookss/quant-trad'
runtimes = ['current', '24357ff387776822f676ae2a8b1cce7f209c313e']
if 'workflows/' in path:
    sha = parse_qs(urlsplit(path).query)['head_sha'][0]
    identity.write_text(sha)
    response = {'workflow_runs': [dict(id=1, head_sha=sha, event='push', head_branch='develop',
        path='.github/workflows/test.yaml', repository=dict(full_name=repository),
        head_repository=dict(full_name=repository), status='completed',
        conclusion=os.environ.get('QT_FIXTURE_CI', 'success'), run_attempt=1, html_url='fixture://ci')]}
elif '/attempts/1/jobs?' in path:
    sha = identity.read_text()
    names = ['pr-suite', 'frontend', 'deployment-contract', 'clean-database-bootstrap', 'deployment-rehearsal']
    names += ['committed-recovery (' + runtime + ')' for runtime in runtimes]
    jobs = [dict(id=index+1, name=name, head_sha=sha, run_id=1, run_attempt=1,
        status='completed', conclusion='success', started_at='2026-10-10T01:00:00Z',
        completed_at='2026-10-10T02:00:00Z',
        steps=[dict(name='Attest and pack disposable qualification evidence', conclusion='success')])
        for index, name in enumerate(names)]
    if os.environ.get('QT_FIXTURE_PACKAGE'):
        for job in jobs:
            if job['name']=='deployment-contract':
                job['steps'] += [dict(name=name,conclusion='success') for name in
                    ['Export qualified routine images','Preserve qualified routine images']]
    response = dict(total_count=len(jobs), jobs=jobs)
elif '/runs/1/artifacts?' in path:
    sha = identity.read_text()
    artifacts = [dict(id=index+1, name='committed-recovery-' + runtime, expired=False,
        digest='sha256:' + 'b'*64, created_at='2026-10-10T01:59:00Z',
        archive_download_url='fixture://artifact/' + runtime,
        workflow_run=dict(id=1, head_sha=sha)) for index, runtime in enumerate(runtimes)]
    if os.environ.get('QT_FIXTURE_PACKAGE'):
        package=Path(os.environ['QT_FIXTURE_PACKAGE'])
        artifacts.append(dict(id=3,name='routine-release-package',expired=False,
            digest='sha256:'+hashlib.sha256(package.read_bytes()).hexdigest(),
            created_at='2026-10-10T01:59:00Z',workflow_run=dict(id=1,head_sha=sha)))
    response = dict(total_count=len(artifacts), artifacts=artifacts)
else:
    raise SystemExit('unexpected synthetic GitHub API path: ' + path)
print(json.dumps(response))
'''


def run(args, *, cwd=None, env=None, ok=True, timeout=900, input_text=None):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, timeout=timeout, input=input_text)
    if ok and result.returncode:
        raise RuntimeError(f'command failed: {args}\n{result.stdout[-8000:]}\n{result.stderr[-8000:]}')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage-layout", action="store_true")
    parser.add_argument("--routine", action="store_true", help="actual routine path on GitHub-hosted disposable Docker only")
    parser.add_argument("--evidence-root", type=Path)
    args=parser.parse_args()
    if args.routine:
        if args.storage_layout: parser.error("--routine uses an independent non-storage fixture")
        routine_main(args.evidence_root)
        return
    storage_layout = args.storage_layout
    service_names = SERVICES + (("storage-maintenance",) if storage_layout else ())
    project = 'qt-promotion-rehearsal-' + uuid.uuid4().hex[:12]
    print('Owned disposable project: ' + project, flush=True)
    tags, recovery_tags = set(), set()
    with tempfile.TemporaryDirectory(prefix='qt-promotion-rehearsal-') as directory:
        root = Path(directory)
        repo = root / 'app'
        repo.mkdir()
        for name in ('scripts/automation/server_deploy.sh', 'scripts/automation/check_release_ci.py', 'scripts/automation/pin_deploy_recovery.py', 'scripts/automation/routine_configuration.py', 'scripts/provenance/source_tree_hash.py', 'src/core/storage_mounts.py'):
            target = repo / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        (repo / 'docker').mkdir()
        (root / 'archive').mkdir()
        for role in ('history', 'working'):
            (root / role).mkdir()
            (root / role / 'sentinel').write_text(role + '-retained')
        (root / 'bin').mkdir()
        gh = root / 'bin/gh'
        gh.write_text(GITHUB_API_FIXTURE)
        gh.chmod(0o755)
        (repo / 'fixture.py').write_text(r'''import os, signal, time
from pathlib import Path
name = os.environ['FIXTURE_SERVICE']
revision = os.environ['QT_IMAGE_SOURCE_REVISION']
log = Path('/proof/events')
def record(event):
    with log.open('a') as stream:
        stream.write(f'{name} {revision} {event}\n')
        stream.flush()
        os.fsync(stream.fileno())
def stop(*_):
    record('drained')
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
record('started')
if os.environ.get('FIXTURE_STORAGE') == 'true':
    assert Path('/history/sentinel').read_text() == 'history-retained'
    assert Path('/working/sentinel').read_text() == 'working-retained'
    record('storage-retained')
if name == 'initialize':
    raise SystemExit(0)
if name != 'backend' or Path('/app/failure').read_text().strip() != 'unhealthy':
    Path('/tmp/ready').touch()
while True:
    time.sleep(0.1)
''')
        (repo / 'qt').write_text('#!/bin/sh\nexit 0\n')
        (repo / 'Dockerfile').write_text('''FROM python:3.12.3-slim
ARG QT_SOURCE_REVISION
ARG QT_SOURCE_TREE_HASH
ENV QT_IMAGE_SOURCE_REVISION=$QT_SOURCE_REVISION QT_IMAGE_SOURCE_TREE_HASH=$QT_SOURCE_TREE_HASH
WORKDIR /app
COPY fixture.py failure /app/
COPY qt /app/scripts/qt
RUN chmod +x /app/scripts/qt
CMD ["python", "/app/fixture.py"]
''')
        services = {}
        for service in service_names:
            item = dict(image=f'{project}-{service}:${{QT_RELEASE_REVISION}}', build=dict(context='..', args=dict(QT_SOURCE_REVISION='${QT_RELEASE_REVISION}', QT_SOURCE_TREE_HASH='${QT_SOURCE_TREE_HASH}')), environment={'FIXTURE_SERVICE': service}, volumes=['proof:/proof'], stop_grace_period='10s', init=True)
            if service != 'initialize':
                item['healthcheck'] = dict(test=['CMD', 'test', '-f', '/tmp/ready'], interval='1s', timeout='2s', start_period='30s', retries=10)
            services[service] = item
        for service in ('initialize', 'market-data-collector', 'docker-stats'):
            services[service]['image'] = services['backend']['image']
        if storage_layout:
            services['storage-maintenance']['image'] = services['backend']['image']
            services['storage-maintenance'].pop('build')
        services['market-data-collector']['depends_on'] = {'initialize': {'condition': 'service_completed_successfully'}}
        if storage_layout:
            # One prebuilt disposable database stand-in, preserved across app revisions.
            services['tsdb'].pop('build')
            services['tsdb']['image']='${QT_STORAGE_DATABASE_IMAGE:?Fixture database image is required}'
            services['tsdb']['pull_policy']='never'
        config = dict(name=project, services=services, volumes={'proof': {}}, networks={'default': {'internal': True}})
        compose_path = repo / 'docker/docker-compose.server.yml'
        compose_path.write_text(json.dumps(config))
        storage_path = repo / 'docker/docker-compose.storage-server.yml'
        storage_model = {'services': {service: {
            'environment': {'FIXTURE_STORAGE': 'true'},
            'volumes': [
                {'type': 'bind', 'source': '${QT_FIXTURE_HISTORY}', 'target': '/history', 'bind': {'create_host_path': False}},
                {'type': 'bind', 'source': '${QT_FIXTURE_WORKING}', 'target': '/working', 'bind': {'create_host_path': False}},
            ],
        } for service in service_names}}
        if storage_layout:
            storage_path.write_text(json.dumps(storage_model))
        (repo / '.gitignore').write_text('*.pyc\n__pycache__/\n')
        (repo / 'failure').write_text('healthy')
        run(['git', 'init', '-b', 'develop'], cwd=repo)
        run(['git', 'config', 'user.name', 'QT deployment rehearsal'], cwd=repo)
        run(['git', 'config', 'user.email', 'rehearsal@quanttrad.test'], cwd=repo)
        def commit(message):
            run(['git', 'add', '.'], cwd=repo)
            run(['git', 'commit', '-m', message], cwd=repo)
            return run(['git', 'rev-parse', 'HEAD'], cwd=repo).stdout.strip()
        first = commit('fixture initial')
        (repo / 'release-note').write_text('second')
        second = commit('fixture compatible update')
        missing_overlay = None
        if storage_layout:
            storage_path.unlink()
            missing_overlay = commit('fixture incompatible missing storage overlay')
            storage_path.write_text(json.dumps(storage_model))
        (repo / 'failure').write_text('unhealthy')
        bad = commit('fixture unhealthy update')
        origin = root / 'origin.git'
        run(['git', 'clone', '--bare', str(repo), str(origin)])
        run(['git', 'remote', 'add', 'origin', str(origin)], cwd=repo)
        run(['git', 'fetch', 'origin'], cwd=repo)
        run(['git', 'switch', '--detach', first], cwd=repo)
        env = {k: v for k, v in os.environ.items() if not k.startswith(('QT_', 'PG_', 'POSTGRES_', 'COMPOSE_'))}
        env.update(QT_SINGLE_NODE_ENV_FILE=str(root / 'secrets.env'), QT_SINGLE_NODE_STATE_ROOT=str(root / 'state'), QT_MARKET_DATA_ROOT=str(root / 'archive'), QT_DEPLOY_WAIT_SECONDS='90', QT_REBUILD_DATABASE_IMAGE='1', PATH=str(root / 'bin') + ':' + os.environ['PATH'])
        if storage_layout:
            database_tag=project+'-database:fixture'
            tags.add(database_tag)
            first_hash=run(['python3','scripts/provenance/source_tree_hash.py','--root',str(repo),
                           '--git-revision',first],cwd=repo).stdout.strip()
            run(['docker','build','--tag',database_tag,'--build-arg','QT_SOURCE_REVISION='+first,
                 '--build-arg','QT_SOURCE_TREE_HASH='+first_hash,str(repo)])
            database_image=run(['docker','image','inspect','--format','{{.Id}}',database_tag]).stdout.strip()
            env.update(QT_FIXTURE_HISTORY=str(root / 'history'), QT_FIXTURE_WORKING=str(root / 'working'),
                       QT_STORAGE_DATABASE_IMAGE=database_image,QT_REBUILD_DATABASE_IMAGE='0')
        deploy = ['bash', 'scripts/automation/server_deploy.sh']
        run(deploy + ['init-env'], cwd=repo, env=env)
        if storage_layout:
            # Only disposable fixture admission. Production first activation is
            # still owned by the preserving operator, not this field alone.
            (root / 'state/release.env').write_text('storage_layout=ssd-hdd-v1\n')
        compose = ['docker', 'compose', '--project-name', project, '--file', str(compose_path)]
        def state():
            return dict(line.split('=', 1) for line in (root / 'state/release.env').read_text().splitlines())
        def collect_tags():
            recovery = root / 'state/recovery.compose.json'
            if recovery.exists():
                recovery_tags.update(s['image'] for s in json.loads(recovery.read_text())['services'].values())
        try:
            print('Rehearsal: initial deployment', flush=True)
            run(deploy + ['deploy', first], cwd=repo, env=env)
            assert state()['current_revision'] == first
            database_id=run(['docker','ps','-q','--filter',f'label=com.docker.compose.project={project}',
                             '--filter','label=com.docker.compose.service=tsdb']).stdout.strip()
            refused = run(deploy + ['promote', second, '--compatible-with', first], cwd=repo, env={**env, 'QT_FIXTURE_CI': 'failure'}, ok=False)
            assert refused.returncode and state()['current_revision'] == first
            assert run(['git', 'rev-parse', 'HEAD'], cwd=repo).stdout.strip() == first
            print('Rehearsal: compatible promotion', flush=True)
            success = run(deploy + ['promote', second, '--compatible-with', first], cwd=repo, env=env)
            assert 'event=promotion_succeeded' in success.stdout
            assert state()['current_revision'] == second and state()['previous_revision'] == first
            if storage_layout:
                assert run(['docker','ps','-q','--filter',f'label=com.docker.compose.project={project}',
                            '--filter','label=com.docker.compose.service=tsdb']).stdout.strip()==database_id
            collect_tags()  # retain this snapshot before the refusal probe replaces it
            if storage_layout:
                assert state()['storage_layout'] == 'ssd-hdd-v1'
                print('Rehearsal: missing overlay rejected without restarting collectors', flush=True)
                before = run(['docker', 'ps', '-q', '--filter', f'label=com.docker.compose.project={project}']).stdout
                refused_layout = run(deploy + ['promote', missing_overlay, '--compatible-with', second], cwd=repo, env=env, ok=False)
                assert refused_layout.returncode and 'not supported by this checkout' in refused_layout.stderr
                assert state()['current_revision'] == second
                assert before == run(['docker', 'ps', '-q', '--filter', f'label=com.docker.compose.project={project}']).stdout
                assert not (root / 'state/promotion.env').exists()
            collect_tags()
            print('Rehearsal: unhealthy candidate and automatic recovery', flush=True)
            failed = run(deploy + ['promote', bad, '--compatible-with', second], cwd=repo, env=env, ok=False)
            collect_tags()
            assert failed.returncode, failed.stdout
            assert 'event=promotion_recovered' in failed.stderr, failed.stderr[-8000:]
            assert state()['current_revision'] == second
            assert not (root / 'state/promotion.env').exists()
            container = run(['docker', 'ps', '-q', '--filter', f'label=com.docker.compose.project={project}', '--filter', 'label=com.docker.compose.service=market-data-collector']).stdout.strip()
            evidence = run(['docker', 'exec', container, 'cat', '/proof/events']).stdout
            assert f'market-data-collector {first} drained' in evidence
            assert evidence.count(f'market-data-collector {second} started') >= 2
            if storage_layout:
                assert evidence.count(f'market-data-collector {second} storage-retained') >= 2
                assert state()['storage_layout'] == 'ssd-hdd-v1'
            assert run(['docker', 'inspect', '--format', '{{.State.Health.Status}}', container]).stdout.strip() == 'healthy'
            print('Rehearsal: interrupted recovery and unavailable local image', flush=True)
            # Simulate interrupted promotion and unavailable recovery artifact.
            # No network may be required to recover: make origin unreachable.
            run(['git', 'remote', 'set-url', 'origin', str(root / 'missing-origin')], cwd=repo)
            marker = root / 'state/promotion.env'
            snapshot = root / 'state/recovery.compose.json'
            original = snapshot.read_text()
            def record_interrupted_recovery():
                digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
                layout = 'ssd-hdd-v1' if storage_layout else ''
                marker.write_text(f'previous_revision={second}\ncandidate_revision={bad}\nactivation_started=true\nstorage_layout={layout}\nrecovery_compose_sha256={digest}\n')
                marker.chmod(0o600)
            record_interrupted_recovery()
            if storage_layout:
                snapshot.write_text('{}')
                altered = run(deploy + ['recover'], cwd=repo, env=env, ok=False)
                assert altered.returncode and 'snapshot changed' in altered.stderr and marker.exists()
                snapshot.write_text(original)
                # These current values cannot replace the frozen recovery mounts.
                env.update(QT_FIXTURE_HISTORY=str(root / 'missing-history'), QT_FIXTURE_WORKING=str(root / 'missing-working'))
            broken = json.loads(original)
            broken['services']['frontend']['image'] = project + '-missing:recovery'
            snapshot.write_text(json.dumps(broken))
            # Handcrafted coherent fixture: focus this attempt on missing cache,
            # after separately proving that unrecorded snapshot changes refuse.
            record_interrupted_recovery()
            unavailable = run(deploy + ['recover'], cwd=repo, env=env, ok=False)
            assert unavailable.returncode and marker.exists()
            assert 'recovery failed' in unavailable.stderr
            snapshot.write_text(original)
            record_interrupted_recovery()
            run(deploy + ['recover'], cwd=repo, env=env)
            assert not marker.exists() and state()['current_revision'] == second
            if storage_layout:
                assert state()['storage_layout'] == 'ssd-hdd-v1'
                # Frozen Compose recovery may recreate a service with the same
                # pinned image/resources. Container identity is not cluster identity.
                recovered_database=run(['docker','ps','-q','--filter',f'label=com.docker.compose.project={project}',
                                        '--filter','label=com.docker.compose.service=tsdb']).stdout.strip()
                assert run(['docker','inspect','--format','{{.Image}}',recovered_database]).stdout.strip()==database_image
                for role in ('history', 'working'):
                    assert (root / role / 'sentinel').read_text() == role + '-retained'
                print('PASS: recorded storage mounts survive promotion and offline recovery; unsupported candidate and altered snapshot refused; current mount overrides ignored during pinned recovery', flush=True)
            print('PASS: CI rejection, compatible rollout, real Docker health failure, pinned-image recovery, synthetic collector drain/restart, durable volume continuity, failed recovery evidence, and offline recovery resume', flush=True)
        except BaseException:
            diagnostic_env = {**env, 'QT_RELEASE_REVISION': first, 'QT_SOURCE_TREE_HASH': 'diagnostic'}
            diagnostic = run(compose + ['logs', '--tail', '30', '--no-color'], cwd=repo, env=diagnostic_env, ok=False)
            print(diagnostic.stdout[-12000:] + diagnostic.stderr[-2000:], flush=True)
            raise
        finally:
            collect_tags()
            clean_env = {**env, 'QT_RELEASE_REVISION': first, 'QT_SOURCE_TREE_HASH': 'cleanup'}
            result = run(compose + ['down', '--volumes', '--remove-orphans'], cwd=repo, env=clean_env, ok=False)
            if result.returncode:
                raise RuntimeError('rehearsal cleanup failed: ' + result.stderr)
            for revision in (first, second, bad):
                tags.update(f'{project}-{service}:{revision}' for service in service_names)
            existing = run(['docker', 'image', 'ls', '--format', '{{.Repository}}:{{.Tag}}']).stdout.splitlines()
            owned = sorted((tags | recovery_tags).intersection(existing))
            if owned:
                run(['docker', 'image', 'rm', *owned])


def require_hosted_routine_runner(environ):
    """The new mode is admitted only on the approved disposable native runner."""
    if (environ.get('GITHUB_ACTIONS') != 'true' or
        environ.get('RUNNER_ENVIRONMENT') != 'github-hosted' or
        not environ.get('RUNNER_TEMP') or
        environ.get('DOCKER_HOST') or environ.get('DOCKER_CONTEXT')):
        raise RuntimeError('routine rehearsal requires GitHub-hosted runner default Docker; no remote context')


def cleanup_owned_operations(operations):
    """Attempt every owned cleanup operation; absence is distinct from daemon failure."""
    errors=[]
    for label, operation, absent in operations:
        try:
            result=operation()
            if result is not None and result.returncode:
                message=(result.stderr or '').strip()
                if not absent or absent not in message:
                    errors.append(label+': '+(message[-500:] or 'command failed'))
        except Exception as exc:
            errors.append(label+': '+type(exc).__name__)
    return errors


def routine_main(evidence_root=None):
    """Actual routine controller/PG/images; synthetic CI metadata, never a release."""
    require_hosted_routine_runner(os.environ)
    if run(['docker','context','show'],timeout=15).stdout.strip() != 'default':
        raise RuntimeError('routine rehearsal refuses non-default Docker context')
    if run(['docker','context','inspect','default','--format','{{.Endpoints.docker.Host}}'],timeout=15).stdout.strip() != 'unix:///var/run/docker.sock':
        raise RuntimeError('routine rehearsal requires local disposable runner socket')
    project='qt-routine-rehearsal-'+uuid.uuid4().hex[:12]
    source=run(['git','rev-parse','HEAD'],cwd=ROOT).stdout.strip()
    deadline=time.monotonic()+600
    phases=[]
    owned_tags=set()
    outcome={'source_revision':source,'project':project,'phases':phases,'status':'running',
             'scope':'synthetic CI metadata; real disposable Docker, PostgreSQL, package and controller; no production qualification'}
    def call(args,**kwargs):
        kwargs['timeout']=min(kwargs.pop('timeout',120),max(1,int(deadline-time.monotonic())))
        return run(args,**kwargs)
    with tempfile.TemporaryDirectory(prefix=project+'-',dir=os.environ['RUNNER_TEMP']) as directory:
        root=Path(directory)
        repo=root/'app';repo.mkdir()
        state_root=root/'state';state_root.mkdir(mode=0o700)
        archive=root/'archive';archive.mkdir()
        bin_root=root/'bin';bin_root.mkdir()
        env={k:v for k,v in os.environ.items() if not k.startswith(('QT_','PG_','POSTGRES_','COMPOSE_'))}
        env.update(QT_SINGLE_NODE_ENV_FILE=str(root/'qt-disposable.env'),QT_SINGLE_NODE_STATE_ROOT=str(state_root),
                   QT_MARKET_DATA_ROOT=str(archive),QT_DEPLOY_WAIT_SECONDS='45',QT_REBUILD_DATABASE_IMAGE='1',
                   PATH=str(bin_root)+':'+os.environ['PATH'])
        compose_path=repo/'docker/docker-compose.server.yml'
        compose=['docker','compose','--project-name',project,'--file',str(compose_path)]
        restore_name=project+'-restore'
        restore_volume=project+'-restore-data'
        docker_calls=root/'docker-calls.jsonl'
        real_docker=shutil.which('docker')
        (bin_root/'docker').write_text('#!/usr/bin/env python3\nimport json,os,sys\n'
            +f'with open({str(docker_calls)!r},"a") as stream: stream.write(json.dumps(sys.argv[1:])+"\\n")\n'
            +f'os.execv({real_docker!r},[{real_docker!r},*sys.argv[1:]])\n')
        (bin_root/'docker').chmod(0o755)
        def trace():
            return [json.loads(line) for line in docker_calls.read_text().splitlines()] if docker_calls.exists() else []
        def no_build_or_pull(start):
            assert not any(set(args)&{'build','pull'} for args in trace()[start:]),'routine activation/recovery rebuilt or pulled'
        def collect_recovery():
            path=state_root/'recovery.compose.json'
            if path.exists():
                owned_tags.update(s['image'] for s in json.loads(path.read_text())['services'].values())
        def revision_state():
            return dict(line.split('=',1) for line in (state_root/'release.env').read_text().splitlines())
        def containers():
            return call(['docker','ps','--all','--quiet','--filter',f'label=com.docker.compose.project={project}']).stdout.splitlines()
        def database():
            ids=call(['docker','ps','--quiet','--filter',f'label=com.docker.compose.project={project}',
                      '--filter','label=com.docker.compose.service=tsdb']).stdout.splitlines()
            assert len(ids)==1
            return ids[0]
        def sql(text_value,container=None):
            return call(['docker','exec','-i',container or database(),'psql','-X','-qAt','-v','ON_ERROR_STOP=1',
                         '-U','qt_fixture','-d','qt_fixture'],input_text=text_value).stdout
        def probe(container=None):
            script=call(['python3','scripts/automation/routine_schema_probe.py','--sql'],cwd=repo).stdout
            raw=sql(script,container)
            return json.loads(call(['python3','scripts/automation/routine_schema_probe.py','--fingerprint'],cwd=repo,input_text=raw).stdout)
        def rows(container=None):
            return sql('SELECT row_to_json(t) FROM qt_routine_evidence t ORDER BY id;',container).strip()
        def qualify_recovery(revision):
            # Real tiny dump -> independent owned PostgreSQL volume -> data read.
            baseline=probe()
            dump=call(['docker','exec',database(),'pg_dump','-U','qt_fixture','-d','qt_fixture',
                       '--no-owner','--no-acl','--table=public.qt_routine_evidence']).stdout
            call(['docker','run','--detach','--name',restore_name,'--network','none',
                  '--label',f'qt.rehearsal.project={project}','--mount',f'type=volume,source={restore_volume},target=/var/lib/postgresql/data',
                  '--env','POSTGRES_HOST_AUTH_METHOD=trust','--env','POSTGRES_USER=qt_fixture','--env','POSTGRES_DB=qt_fixture',
                  'timescale/timescaledb:2.14.2-pg15'])
            ready=False
            for _ in range(60):
                if call(['docker','exec',restore_name,'pg_isready','-h','127.0.0.1','-U','qt_fixture','-d','qt_fixture'],ok=False).returncode==0:
                    ready=True;break
                time.sleep(0.5)
            assert ready,'disposable restore database not ready'
            sql(dump,restore_name)
            assert rows(restore_name)==rows()
            restored=probe(restore_name)
            assert restored['schema_sha256']==baseline['schema_sha256']
            manifest=state_root/'backup-manifest.json'
            manifest.write_text(json.dumps({'scope':'tiny disposable fixture only','dump_sha256':hashlib.sha256(dump.encode()).hexdigest(),
                                            'revision':revision,'source_cluster':baseline['cluster_identifier']}))
            restored_proof=state_root/'restore-receipt.json'
            restored_proof.write_text(json.dumps({'scope':'actual tiny disposable restore; not production backup proof',
                                                  'rows_match':True,'schema_match':True,'restored_cluster':restored['cluster_identifier']}))
            now=datetime.now(timezone.utc)
            receipt={'schema_version':'qt.routine_recovery.v1','revision':revision,'restore_result':'passed',
                     'verified_at':now.isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat(),
                     **{k:baseline[k] for k in ('schema_sha256','cluster_identifier')}}
            for key,path in [('backup_manifest',manifest),('restore_receipt',restored_proof)]:
                path.chmod(0o600);receipt[key]=str(path);receipt[key+'_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
            receipt_path=state_root/'routine-recovery.json';receipt_path.write_text(json.dumps(receipt));receipt_path.chmod(0o600)
            call(['docker','rm','--force',restore_name]);call(['docker','volume','rm',restore_volume])
            return baseline
        try:
            for name in ('server_deploy.sh','check_release_ci.py','pin_deploy_recovery.py','routine_promotion.py','routine_schema_probe.py','routine_configuration.py','release_package.py'):
                target=repo/'scripts/automation'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/'scripts/automation'/name,target)
            for name in ('scripts/ci/ci_impact.py','scripts/provenance/source_tree_hash.py','src/core/storage_mounts.py'):
                target=repo/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/name,target)
            (repo/'docker').mkdir()
            leaf=repo/'portal/frontend/src/features/overview/components/ActivityHeatmap.jsx';leaf.parent.mkdir(parents=True)
            leaf.write_text('healthy')
            (repo/'fixture.py').write_text('''import os,signal,time
from pathlib import Path
name=os.environ['FIXTURE_SERVICE']; revision=os.environ['QT_IMAGE_SOURCE_REVISION']
def record(event):
 with Path('/proof/events').open('a') as stream: stream.write(f'{name} {revision} {event}\\n');stream.flush();os.fsync(stream.fileno())
def stop(*args): record('drained');raise SystemExit(0)
signal.signal(signal.SIGTERM,stop);record('started')
if name=='initialize':raise SystemExit(0)
if Path('/app/failure').read_text().strip()!='unhealthy':Path('/tmp/ready').touch()
while True:time.sleep(.1)
''')
            (repo/'qt').write_text('#!/bin/sh\nexit 0\n')
            (repo/'Dockerfile').write_text('''FROM python:3.12.3-slim
ARG QT_SOURCE_REVISION
ARG QT_SOURCE_TREE_HASH
ENV QT_IMAGE_SOURCE_REVISION=$QT_SOURCE_REVISION QT_IMAGE_SOURCE_TREE_HASH=$QT_SOURCE_TREE_HASH
WORKDIR /app
COPY fixture.py /app/
COPY portal/frontend/src/features/overview/components/ActivityHeatmap.jsx /app/failure
COPY qt /app/scripts/qt
RUN chmod +x /app/scripts/qt
RUN python -c "import json,os;open('/app/.qt-source-attestation.json','w').write(json.dumps({'source_revision':os.environ['QT_IMAGE_SOURCE_REVISION'],'source_tree_hash':os.environ['QT_IMAGE_SOURCE_TREE_HASH']}))"
CMD ["python","/app/fixture.py"]
''')
            (repo/'docker/Dockerfile.database').write_text('FROM timescale/timescaledb:2.14.2-pg15\n')
            services={}
            for service in SERVICES:
                role=service if service in ('frontend','frontend-v2') else 'backend'
                services[service]={'image':f'quanttrad-{role}:${{QT_RELEASE_REVISION}}',
                    'build':{'context':'..','args':{'QT_SOURCE_REVISION':'${QT_RELEASE_REVISION}','QT_SOURCE_TREE_HASH':'${QT_SOURCE_TREE_HASH}'}},
                    'environment':{'FIXTURE_SERVICE':service,'FIXTURE_OPERATOR_SETTING':'${QT_FIXTURE_OPERATOR_SETTING:-baseline}'},'volumes':['proof:/proof'],'stop_grace_period':'5s','init':True}
                if service!='initialize':services[service]['healthcheck']={'test':['CMD','test','-f','/tmp/ready'],'interval':'1s','timeout':'2s','retries':3}
            services['tsdb']={'image':f'{project}-database:${{QT_RELEASE_REVISION}}',
                'build':{'context':'..','dockerfile':'docker/Dockerfile.database'},
                'environment':{'POSTGRES_DB':'qt_fixture','POSTGRES_USER':'qt_fixture','POSTGRES_HOST_AUTH_METHOD':'trust'},
                'volumes':['database:/var/lib/postgresql/data'],'command':['postgres','-c','timescaledb.telemetry_level=off'],
                'healthcheck':{'test':['CMD','pg_isready','-h','127.0.0.1','-U','qt_fixture','-d','qt_fixture'],'interval':'1s','timeout':'2s','retries':40},'stop_grace_period':'10s'}
            services['market-data-collector']['depends_on']={'initialize':{'condition':'service_completed_successfully'}}
            compose_path.write_text(json.dumps({'name':project,'services':services,'volumes':{'proof':{},'database':{}},'networks':{'default':{'internal':True}}}))
            (repo/'.gitignore').write_text('*.pyc\n__pycache__/\n')
            call(['git','init','-b','develop'],cwd=repo);call(['git','config','user.name','QT disposable rehearsal'],cwd=repo)
            call(['git','config','user.email','rehearsal@quanttrad.test'],cwd=repo)
            def commit(message):
                call(['git','add','.'],cwd=repo);call(['git','commit','-m',message],cwd=repo)
                return call(['git','rev-parse','HEAD'],cwd=repo).stdout.strip()
            first=commit('fixture baseline')
            leaf.write_text('healthy presentation update');second=commit('fixture presentation')
            leaf.write_text('unhealthy');bad=commit('fixture unhealthy presentation')
            forbidden=repo/'portal/frontend/src/components/ChartComponent/CredentialsModal.jsx';forbidden.parent.mkdir(parents=True)
            forbidden.write_text('ineligible fixture');ineligible=commit('fixture credential scope refusal')
            origin=root/'origin.git';call(['git','clone','--bare',str(repo),str(origin)])
            call(['git','remote','add','origin',str(origin)],cwd=repo);call(['git','fetch','origin'],cwd=repo)
            call(['git','switch','--detach',first],cwd=repo)
            gh=bin_root/'gh';gh.write_text(GITHUB_API_FIXTURE);gh.chmod(0o755)
            deploy=['bash','scripts/automation/server_deploy.sh']
            call(deploy+['init-env'],cwd=repo,env=env)
            private_env=Path(env['QT_SINGLE_NODE_ENV_FILE'])
            values=dict(line.split('=',1) for line in private_env.read_text().splitlines())
            for key in tuple(values):
                if key.startswith('QT_SINGLE_NODE_') and (key.endswith('BOOTSTRAP_MARKET_DATA') or '_ENABLE_' in key):
                    values[key]='false'
            private_env.write_text(''.join(f'{key}={value}\n' for key,value in values.items()))
            # Generated credentials are inert private fixture values. Never copy a
            # host env file; runtime services have no provider/account enrollment.
            owned_tags.update(f'quanttrad-{name}:{first}' for name in ('backend','frontend','frontend-v2'))
            owned_tags.add(f'{project}-database:{first}')
            call(deploy+['deploy',first],cwd=repo,env=env)
            sql("CREATE TABLE qt_routine_evidence(id integer PRIMARY KEY,value text NOT NULL); INSERT INTO qt_routine_evidence VALUES(1,'retained-fixture');")
            original_rows=rows();baseline=qualify_recovery(first);phases.append('real tiny dump and isolated restore')
            def package(revision):
                call(['git','switch','--detach',revision],cwd=repo)
                tree=call(['python3','scripts/provenance/source_tree_hash.py','--git-revision',revision],cwd=repo).stdout.strip()
                for name in ('backend','frontend','frontend-v2'):
                    tag=f'quanttrad-{name}:{revision}';owned_tags.add(tag)
                    call(['docker','build','--tag',tag,'--build-arg','QT_SOURCE_REVISION='+revision,'--build-arg','QT_SOURCE_TREE_HASH='+tree,str(repo)])
                output_root=root/('package-'+revision)
                package_env={**env,'GITHUB_RUN_ID':'1','GITHUB_RUN_ATTEMPT':'1'}
                for action in ('capture','export'):
                    call(['python3','scripts/automation/release_package.py',action,'--root',str(output_root)],cwd=repo,env=package_env)
                destination=root/(revision+'.zip')
                with zipfile.ZipFile(destination,'w') as bundle:
                    for name in ('manifest.json','images.tar'):bundle.write(output_root/name,name)
                # Force admission to restore the exported tags; cached source
                # builds alone cannot make this package-load test pass.
                for name in ('backend','frontend','frontend-v2'):
                    call(['docker','image','rm',f'quanttrad-{name}:{revision}'])
                call(['git','switch','--detach',first if revision==second else second],cwd=repo)
                return destination
            second_package=package(second)
            package_env={**env,'QT_FIXTURE_PACKAGE':str(second_package)}
            before=containers()
            approved_environment=private_env.read_text()
            recovery_before=(state_root/'recovery.compose.json').read_bytes() if (state_root/'recovery.compose.json').exists() else None
            private_env.write_text(approved_environment+'QT_FIXTURE_OPERATOR_SETTING=unreviewed\n')
            refused=call(deploy+['promote-routine',second,'--package',str(second_package)],cwd=repo,env=package_env,ok=False)
            assert refused.returncode and 'routine configuration admission failed' in refused.stderr and containers()==before
            assert ((state_root/'recovery.compose.json').read_bytes() if (state_root/'recovery.compose.json').exists() else None)==recovery_before
            private_env.write_text(approved_environment)
            refused=call(deploy+['promote-routine',second,'--package',str(second_package)],cwd=repo,
                         env={**package_env,'QT_FIXTURE_OPERATOR_SETTING':'unreviewed-caller'},ok=False)
            assert refused.returncode and 'routine configuration admission failed' in refused.stderr and containers()==before
            assert ((state_root/'recovery.compose.json').read_bytes() if (state_root/'recovery.compose.json').exists() else None)==recovery_before
            phases.append('private environment and caller configuration drift refused before snapshot or replacement')
            hold=state_root/'storage-handoff.json';hold.write_text('{}')
            refused=call(deploy+['promote-routine',second,'--package',str(second_package)],cwd=repo,env=package_env,ok=False)
            assert refused.returncode and 'storage handoff hold' in refused.stderr and containers()==before;hold.unlink()
            refused=call(deploy+['promote-routine',ineligible,'--package',str(second_package)],cwd=repo,env=package_env,ok=False)
            assert refused.returncode and 'compatibility unknown' in refused.stderr and containers()==before
            corrupt=root/'corrupt-package.zip'
            shutil.copyfile(second_package,corrupt)
            with corrupt.open('ab') as stream:stream.write(b'changed-download')
            refused=call(deploy+['promote-routine',second,'--package',str(corrupt)],cwd=repo,env=package_env,ok=False)
            assert refused.returncode and 'artifact digest' in refused.stderr and containers()==before
            restore_record=state_root/'routine-recovery.json';original_record=restore_record.read_text()
            expired=json.loads(original_record);expired['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
            restore_record.write_text(json.dumps(expired))
            refused=call(deploy+['promote-routine',second,'--package',str(second_package)],cwd=repo,env=package_env,ok=False)
            assert refused.returncode and 'recovery qualification' in refused.stderr and containers()==before
            restore_record.write_text(original_record)
            phases.append('real controller hold, credential scope, changed artifact and stale restore refusal before replacement')
            checkpoint=len(trace())
            success=call(deploy+['promote-routine',second,'--package',str(second_package)],cwd=repo,env=package_env)
            collect_recovery();no_build_or_pull(checkpoint)
            assert 'event=promotion_succeeded' in success.stdout and revision_state()['current_revision']==second
            assert rows()==original_rows and probe()==baseline
            phases.append('qualified package load and compatible routine promotion without build/pull')
            qualify_recovery(second)
            bad_package=package(bad);bad_env={**env,'QT_FIXTURE_PACKAGE':str(bad_package)}
            checkpoint=len(trace())
            failed=call(deploy+['promote-routine',bad,'--package',str(bad_package)],cwd=repo,env=bad_env,ok=False)
            collect_recovery();no_build_or_pull(checkpoint)
            assert failed.returncode and 'event=promotion_recovered' in failed.stderr
            assert revision_state()['current_revision']==second and not (state_root/'promotion.env').exists()
            assert rows()==original_rows and probe()==baseline
            phases.append('unhealthy routine rollout and actual pinned-image/schema-checked recovery')
            # Reconstruct an interrupted intent using the snapshot just exercised.
            snapshot=state_root/'recovery.compose.json';schema=state_root/'routine-schema.interrupted'
            schema.write_text(json.dumps(baseline)+'\n');schema.chmod(0o600)
            marker=state_root/'promotion.env'
            marker.write_text(f'previous_revision={second}\ncandidate_revision={bad}\nactivation_started=true\nstorage_layout=\n'
                f'recovery_compose_sha256={hashlib.sha256(snapshot.read_bytes()).hexdigest()}\nroutine_promotion=true\n'
                f'routine_schema_path={schema}\nroutine_schema_sha256={hashlib.sha256(schema.read_bytes()).hexdigest()}\n')
            marker.chmod(0o600)
            before=containers();checkpoint=len(trace());unchanged_checkout=call(['git','rev-parse','HEAD'],cwd=repo).stdout
            private_env.write_text(approved_environment+'QT_FIXTURE_OPERATOR_SETTING=unreviewed-recovery\n')
            refused=call(deploy+['recover'],cwd=repo,env=bad_env,ok=False)
            no_build_or_pull(checkpoint)
            assert refused.returncode and 'routine configuration admission failed' in refused.stderr and marker.exists() and containers()==before
            assert call(['git','rev-parse','HEAD'],cwd=repo).stdout==unchanged_checkout
            private_env.write_text(approved_environment)
            phases.append('interrupted routine recovery refuses edited private configuration before checkout or replacement')
            sql('ALTER TABLE qt_routine_evidence ADD COLUMN unexpected integer;')
            before=containers();checkpoint=len(trace())
            refused=call(deploy+['recover'],cwd=repo,env=bad_env,ok=False)
            no_build_or_pull(checkpoint)
            assert refused.returncode and marker.exists() and 'compatibility is unknown' in refused.stderr and containers()==before
            sql('ALTER TABLE qt_routine_evidence DROP COLUMN unexpected;')
            # No Git fetch or remote package is available during recovery.
            call(['git','remote','set-url','origin',str(root/'missing-origin')],cwd=repo)
            checkpoint=len(trace());call(deploy+['recover'],cwd=repo,env=bad_env);no_build_or_pull(checkpoint)
            assert not marker.exists() and revision_state()['current_revision']==second and rows()==original_rows
            assert probe()==baseline
            phases.append('interrupted routine drift hold, explicit reconciliation and offline recovery')
            outcome['status']='passed'
            print('PASS: actual promote-routine success/refusal/recovery on owned Docker/PostgreSQL; synthetic CI metadata only',flush=True)
        except BaseException as exc:
            outcome.update(status='failed',error=type(exc).__name__+': '+str(exc)[-3000:])
            raise
        finally:
            operations=[('collect recovery tags',lambda: collect_recovery(),None)]
            if compose_path.exists():
                operations.append(('owned compose cleanup',lambda: run(
                    compose+['down','--volumes','--remove-orphans'],cwd=repo,
                    env={**env,'QT_RELEASE_REVISION':'cleanup','QT_SOURCE_TREE_HASH':'cleanup'},ok=False,timeout=90),None))
            operations.extend([
                ('owned restore container cleanup',lambda: run(['docker','rm','--force',restore_name],ok=False,timeout=30),'No such container:'),
                ('owned restore volume cleanup',lambda: run(['docker','volume','rm',restore_volume],ok=False,timeout=30),'no such volume'),
            ])
            # Resolve tags after recovery collection, even if collection itself fails.
            cleanup_errors=cleanup_owned_operations(operations)
            try:
                cleanup_errors.extend(cleanup_owned_operations([
                    ('owned image cleanup: '+tag,
                     lambda tag=tag: run(['docker','image','rm',tag],ok=False,timeout=30),'No such image:')
                    for tag in sorted(owned_tags)
                ]))
            finally:
                outcome['cleanup_errors']=cleanup_errors
                if cleanup_errors:outcome['status']='failed'
                if evidence_root:
                    evidence_root.mkdir(parents=True,exist_ok=True,mode=0o700)
                    (evidence_root/'qualification.json').write_text(json.dumps(outcome,indent=2)+'\n')
            if cleanup_errors:raise RuntimeError('; '.join(cleanup_errors))


if __name__ == '__main__':
    main()
