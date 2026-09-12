#!/usr/bin/env python3
"""Run built QT core images on isolated storage with provider egress disabled."""
from __future__ import annotations
import argparse
from datetime import datetime
import json
import re
import os
from pathlib import Path
import subprocess
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
SERVICES = ('tsdb', 'backend', 'initialize', 'market-data-collector', 'frontend', 'frontend-v2')


def run(args, *, env, ok=True):
    result = subprocess.run(args, cwd=ROOT, env=env, text=True, capture_output=True, timeout=600)
    if ok and result.returncode:
        raise RuntimeError(f'command failed: {args}\n{result.stdout[-6000:]}\n{result.stderr[-6000:]}')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--revision', help='reuse built images only when their source hash matches HEAD')
    args = parser.parse_args()
    project = 'qt-core-rehearsal-' + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix='qt-core-rehearsal-') as directory:
        root = Path(directory)
        archive = root / 'archive'
        archive.mkdir()
        udev = root / 'udev'
        udev.mkdir()
        env = {k: v for k, v in os.environ.items() if not k.startswith(('QT_', 'PG_', 'POSTGRES_', 'COMPOSE_'))}
        head = run(['git', 'rev-parse', 'HEAD'], env=env).stdout.strip()
        revision = args.revision or head
        if not re.fullmatch(r'[0-9a-f]{40}', revision):
            raise ValueError('image revision must be a full commit SHA')
        tree_hash = run(['python3', 'scripts/provenance/source_tree_hash.py', '--root', str(ROOT), '--git-revision', revision], env=env).stdout.strip()
        if revision != head:
            current_hash = run(['python3', 'scripts/provenance/source_tree_hash.py', '--root', str(ROOT), '--git-revision', head], env=env).stdout.strip()
            if current_hash != tree_hash:
                raise ValueError('built image source hash does not match current HEAD')
        print(f'Core rehearsal: image_revision={revision} source_tree_hash={tree_hash}', flush=True)
        env.update(QT_SINGLE_NODE_ENV_FILE=str(root / 'secrets.env'), QT_SERVER_ENV_FILE=str(root / 'secrets.env'), QT_MARKET_DATA_ROOT=str(archive), QT_COMPOSE_PROJECT_NAME=project, QT_RELEASE_REVISION=revision, QT_SOURCE_TREE_HASH=tree_hash)
        for name in ('BOOTSTRAP_MARKET_DATA', 'ENABLE_SCHEDULED_FACTS', 'ENABLE_STRUCTURED_FACTS', 'ENABLE_TRADE_STREAMS', 'ENABLE_L2_STREAMS'):
            env['QT_SINGLE_NODE_' + name] = 'false'
        run(['bash', 'scripts/automation/server_deploy.sh', 'init-env'], env=env)
        # The env_file reaches app configuration too, so disable enrollment there.
        private = root / 'secrets.env'
        values = dict(line.split('=', 1) for line in private.read_text().splitlines())
        values.update({key: value for key, value in env.items() if key.startswith('QT_SINGLE_NODE_') and value == 'false'})
        values['QT_MARKET_DATA_ROOT'] = str(archive)
        private.write_text(''.join(f'{key}={value}\n' for key, value in values.items()))
        base = ['docker', 'compose', '--env-file', str(private), '--file', 'docker/docker-compose.server.yml']
        config = json.loads(run(base + ['config', '--format', 'json'], env=env).stdout)
        config['services'] = {name: config['services'][name] for name in SERVICES}
        config['networks'] = {'quanttrad': {'name': project + '-network', 'internal': True}}
        config['volumes'] = {'postgres-data': {'name': project + '-postgres'}}
        for name, service in config['services'].items():
            service.pop('build', None)
            service.pop('ports', None)
            service['restart'] = 'no'
            service['pull_policy'] = 'never'
            if 'environment' in service:
                service['environment']['QT_MARKET_DATA_LIFECYCLE_ENABLED'] = 'false'
            volumes = []
            for volume in service.get('volumes', []):
                if volume.get('target') == '/var/run/docker.sock':
                    continue
                if volume.get('target') == '/run/qt-host-udev':
                    volume['source'] = str(udev)
                volumes.append(volume)
            service['volumes'] = volumes
        path = root / 'compose.json'
        path.write_text(json.dumps(config))
        path.chmod(0o600)
        compose = ['docker', 'compose', '--project-name', project, '--file', str(path)]
        def cid(service):
            return run(compose + ['ps', '--all', '--quiet', service], env=env).stdout.strip()
        def database(sql):
            return run(compose + ['exec', '-T', 'tsdb', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'quanttrad', '-d', 'quanttrad', '-Atc', sql], env=env).stdout.strip()
        def worker_state():
            probe = "from portal.backend.workers.market_data_collector_health import live_worker_for_host; import json; row = live_worker_for_host(); print(json.dumps({key: row[key].isoformat() for key in ('started_at', 'heartbeat_at')}))"
            payload = run(compose + ['exec', '-T', 'market-data-collector', 'python', '-c', probe], env=env).stdout.strip().splitlines()[-1]
            return {key: datetime.fromisoformat(value) for key, value in json.loads(payload).items()}
        try:
            run(compose + ['up', '--detach', '--no-build', '--pull', 'never', '--wait', '--wait-timeout', '360'], env=env)
            database('CREATE TABLE public.qt_deployment_rehearsal (value text PRIMARY KEY); INSERT INTO public.qt_deployment_rehearsal VALUES (\'retained\')')
            first_health = worker_state()
            run(compose + ['stop', 'market-data-collector'], env=env)
            old = cid('market-data-collector')
            assert run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', old], env=env).stdout.strip() == '0'
            logs = run(['docker', 'logs', '--tail', '200', old], env=env)
            assert 'market_data_collector_stopped' in logs.stdout + logs.stderr
            run(compose + ['up', '--detach', '--no-build', '--pull', 'never', '--force-recreate', '--wait', '--wait-timeout', '360'], env=env)
            second_health = worker_state()
            assert cid('market-data-collector') != old, 'expected a recreated collector container'
            assert second_health['started_at'] > first_health['started_at']
            assert second_health['heartbeat_at'] >= second_health['started_at']
            assert database('SELECT value FROM public.qt_deployment_rehearsal') == 'retained'
            assert run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', cid('initialize')], env=env).stdout.strip() == '0'
            for service in ('backend', 'market-data-collector', 'frontend', 'frontend-v2'):
                actual = run(compose + ['exec', '-T', service, 'printenv', 'QT_IMAGE_SOURCE_REVISION'], env=env).stdout.strip()
                assert actual == revision, (service, actual, revision)
            run(compose + ['exec', '-T', 'backend', '/app/scripts/qt', 'data', 'collectors', 'fleet'], env=env)
            print('PASS: actual QT clean bootstrap, API/frontends, initializer, collector heartbeat, clean worker stop, recreation, exact image revision, and PostgreSQL data retention; provider enrollment and network egress disabled', flush=True)
        except BaseException:
            diagnostics = run(compose + ['logs', '--tail', '100', '--no-color'], env=env, ok=False)
            print(diagnostics.stdout[-16000:] + diagnostics.stderr[-2000:], flush=True)
            raise
        finally:
            run(compose + ['down', '--volumes', '--remove-orphans'], env=env)


if __name__ == '__main__':
    main()
