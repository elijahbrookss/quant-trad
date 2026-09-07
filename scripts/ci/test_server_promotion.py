#!/usr/bin/env python3
"""Real deployment controller with synthetic services, real Docker/Git/volumes.

GitHub API responses are a local fixture. This does not prove QT schema or
provider behavior; the production-image smoke and DB suites cover those seams.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ('tsdb', 'backend', 'initialize', 'market-data-collector', 'docker-stats', 'frontend', 'frontend-v2')


def run(args, *, cwd=None, env=None, ok=True):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, timeout=900)
    if ok and result.returncode:
        raise RuntimeError(f'command failed: {args}\n{result.stdout[-8000:]}\n{result.stderr[-8000:]}')
    return result


def main():
    project = 'qt-promotion-rehearsal-' + uuid.uuid4().hex[:12]
    tags, recovery_tags = set(), set()
    with tempfile.TemporaryDirectory(prefix='qt-promotion-rehearsal-') as directory:
        root = Path(directory)
        repo = root / 'app'
        repo.mkdir()
        for name in ('scripts/automation/server_deploy.sh', 'scripts/automation/check_release_ci.py', 'scripts/automation/pin_deploy_recovery.py', 'scripts/provenance/source_tree_hash.py', 'src/core/storage_mounts.py'):
            target = repo / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        (repo / 'docker').mkdir()
        (root / 'archive').mkdir()
        (root / 'bin').mkdir()
        gh = root / 'bin/gh'
        gh.write_text(r'''#!/usr/bin/env python3
import json, os, sys
from urllib.parse import parse_qs, urlsplit
path = sys.argv[-1]
if 'workflows/' in path:
    sha = parse_qs(urlsplit(path).query)['head_sha'][0]
    print(json.dumps({'workflow_runs': [dict(id=1, head_sha=sha, event='push', head_branch='develop', status='completed', conclusion=os.environ.get('QT_FIXTURE_CI', 'success'), run_attempt=1, html_url='fixture://ci')]}))
else:
    names = ['pr-suite', 'frontend', 'deployment-contract', 'clean-database-bootstrap', 'deployment-rehearsal']
    print(json.dumps({'total_count': len(names), 'jobs': [dict(name=n, status='completed', conclusion='success') for n in names]}))
''')
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
        for service in SERVICES:
            item = dict(image=f'{project}-{service}:${{QT_RELEASE_REVISION}}', build=dict(context='..', args=dict(QT_SOURCE_REVISION='${QT_RELEASE_REVISION}', QT_SOURCE_TREE_HASH='${QT_SOURCE_TREE_HASH}')), environment={'FIXTURE_SERVICE': service}, volumes=['proof:/proof'], stop_grace_period='10s', init=True)
            if service != 'initialize':
                item['healthcheck'] = dict(test=['CMD', 'test', '-f', '/tmp/ready'], interval='1s', timeout='1s', retries=2)
            services[service] = item
        for service in ('initialize', 'market-data-collector', 'docker-stats'):
            services[service]['image'] = services['backend']['image']
        services['market-data-collector']['depends_on'] = {'initialize': {'condition': 'service_completed_successfully'}}
        config = dict(name=project, services=services, volumes={'proof': {}}, networks={'default': {'internal': True}})
        compose_path = repo / 'docker/docker-compose.server.yml'
        compose_path.write_text(json.dumps(config))
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
        (repo / 'failure').write_text('unhealthy')
        bad = commit('fixture unhealthy update')
        origin = root / 'origin.git'
        run(['git', 'clone', '--bare', str(repo), str(origin)])
        run(['git', 'remote', 'add', 'origin', str(origin)], cwd=repo)
        run(['git', 'fetch', 'origin'], cwd=repo)
        run(['git', 'switch', '--detach', first], cwd=repo)
        env = {k: v for k, v in os.environ.items() if not k.startswith(('QT_', 'PG_', 'POSTGRES_', 'COMPOSE_'))}
        env.update(QT_SINGLE_NODE_ENV_FILE=str(root / 'secrets.env'), QT_SINGLE_NODE_STATE_ROOT=str(root / 'state'), QT_MARKET_DATA_ROOT=str(root / 'archive'), QT_DEPLOY_WAIT_SECONDS='90', QT_REBUILD_DATABASE_IMAGE='1', PATH=str(root / 'bin') + ':' + os.environ['PATH'])
        deploy = ['bash', 'scripts/automation/server_deploy.sh']
        run(deploy + ['init-env'], cwd=repo, env=env)
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
            refused = run(deploy + ['promote', second, '--compatible-with', first], cwd=repo, env={**env, 'QT_FIXTURE_CI': 'failure'}, ok=False)
            assert refused.returncode and state()['current_revision'] == first
            assert run(['git', 'rev-parse', 'HEAD'], cwd=repo).stdout.strip() == first
            print('Rehearsal: compatible promotion', flush=True)
            success = run(deploy + ['promote', second, '--compatible-with', first], cwd=repo, env=env)
            assert 'event=promotion_succeeded' in success.stdout
            assert state()['current_revision'] == second and state()['previous_revision'] == first
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
            assert run(['docker', 'inspect', '--format', '{{.State.Health.Status}}', container]).stdout.strip() == 'healthy'
            print('Rehearsal: interrupted recovery and unavailable local image', flush=True)
            # Simulate interrupted promotion and unavailable recovery artifact.
            # No network may be required to recover: make origin unreachable.
            run(['git', 'remote', 'set-url', 'origin', str(root / 'missing-origin')], cwd=repo)
            marker = root / 'state/promotion.env'
            marker.write_text(f'previous_revision={second}\ncandidate_revision={bad}\nactivation_started=true\n')
            marker.chmod(0o600)
            snapshot = root / 'state/recovery.compose.json'
            original = snapshot.read_text()
            broken = json.loads(original)
            broken['services']['frontend']['image'] = project + '-missing:recovery'
            snapshot.write_text(json.dumps(broken))
            unavailable = run(deploy + ['recover'], cwd=repo, env=env, ok=False)
            assert unavailable.returncode and marker.exists()
            assert 'recovery failed' in unavailable.stderr
            snapshot.write_text(original)
            run(deploy + ['recover'], cwd=repo, env=env)
            assert not marker.exists() and state()['current_revision'] == second
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
                tags.update(f'{project}-{service}:{revision}' for service in SERVICES)
            existing = run(['docker', 'image', 'ls', '--format', '{{.Repository}}:{{.Tag}}']).stdout.splitlines()
            owned = sorted((tags | recovery_tags).intersection(existing))
            if owned:
                run(['docker', 'image', 'rm', *owned])


if __name__ == '__main__':
    main()
