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
    parser.add_argument('--storage-layout', action='store_true', help='rehearse the fixed non-root SSD/HDD overlay on disposable volumes')
    args = parser.parse_args()
    project = 'qt-core-rehearsal-' + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix='qt-core-rehearsal-') as directory:
        root = Path(directory)
        archive = root / 'archive'
        archive.mkdir()
        udev = root / 'udev'
        udev.mkdir()
        (udev / 'data').mkdir()
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
        env.update(QT_SINGLE_NODE_STATE_ROOT=str(root / 'state'), QT_SINGLE_NODE_ENV_FILE=str(root / 'secrets.env'), QT_SERVER_ENV_FILE=str(root / 'secrets.env'), QT_MARKET_DATA_ROOT=str(archive), QT_COMPOSE_PROJECT_NAME=project, QT_RELEASE_REVISION=revision, QT_SOURCE_TREE_HASH=tree_hash)
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
        if args.storage_layout:
            hdd = root / 'prepared-hdd'
            (hdd / 'archives').mkdir(parents=True)
            inventory = root / 'inventory.json'
            inventory.write_text(json.dumps({'schema_version': 'qt.storage_inventory.v1', 'targets': [
                dict(target_id='ssd', label='Recent', filesystem_uuid='fixture-ssd',
                     root='/var/lib/postgresql/data', medium='ssd'),
                dict(target_id='hdd', label='History', filesystem_uuid='fixture-hdd',
                     root='/qt-history', medium='hdd')]}))
            inventory.chmod(0o644)
            limits = root / 'maintenance.json'
            limits.write_text(json.dumps({'schema_version': 'qt.storage_maintenance_limits.v1',
                'history': {'wal_bytes': 16*1024**2, 'temporary_bytes': {'ssd': 1024**2, 'hdd': 0},
                    'growth_bytes_per_second': {'ssd': 0, 'hdd': 0},
                    'maintenance_bytes': {'ssd': 1024**2, 'hdd': 1024**2},
                    'movement_timeout_seconds': 60, 'cancellation_grace_seconds': 5},
                'recovery': {'max_bytes': 64*1024**2, 'timeout_seconds': 60,
                             'headroom_bytes': 1024**2, 'max_objects': 100}}))
            limits.chmod(0o644)
            env.update(QT_STORAGE_HDD_ROOT=str(hdd), QT_MARKET_DATA_ROOT=str(hdd / 'archives'),
                QT_MARKET_DATA_EXPECTED_UUID='fixture-hdd', QT_MARKET_DATA_WORKING_ROOT=str(archive),
                QT_MARKET_DATA_WORKING_EXPECTED_UUID='fixture-ssd',
                QT_STORAGE_INVENTORY_HOST_PATH=str(inventory),
                QT_STORAGE_MAINTENANCE_LIMITS_HOST_PATH=str(limits), QT_DOCKER_SOCKET_GID='70')
            base += ['--file', 'docker/docker-compose.storage-server.yml']
        config = json.loads(run(base + ['config', '--format', 'json'], env=env).stdout)
        config['services'] = {name: config['services'][name] for name in SERVICES}
        config['networks'] = {'quanttrad': {'name': project + '-network', 'internal': True}}
        config['volumes'] = {'postgres-data': {'name': project + '-postgres'}}
        if args.storage_layout:
            config['volumes']['storage-history'] = dict(name=project+'-history', driver='local',
                driver_opts=dict(type='tmpfs', device='tmpfs', o='size=268435456,uid=70,gid=70,mode=0700'))
            # Empty disposable cluster only. The real overlay preserves PGDATA.
            config['services']['tsdb']['environment']['PGDATA'] = '/var/lib/postgresql/data/pgdata'
        for name, service in config['services'].items():
            service.pop('build', None)
            service.pop('ports', None)
            service['restart'] = 'no'
            service['pull_policy'] = 'never'
            if 'environment' in service and not args.storage_layout:
                service['environment']['QT_MARKET_DATA_LIFECYCLE_ENABLED'] = 'false'
            volumes = []
            for volume in service.get('volumes', []):
                if volume.get('target') == '/var/run/docker.sock':
                    continue
                if volume.get('target') == '/run/qt-host-udev':
                    volume['source'] = str(udev)
                if args.storage_layout and volume.get('target') == '/qt-history':
                    volume = dict(type='volume', source='storage-history', target='/qt-history')
                if args.storage_layout and volume.get('target') == '/app/logs/market-structure':
                    volume = dict(type='volume', source='postgres-data', target='/app/logs/market-structure',
                                  volume=dict(subpath='working'))
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
        def storage_probe():
            if not args.storage_layout:
                return
            # Both real application services publish private immutable archive
            # objects, then read the other writer's file with the actual store.
            for service in ('backend', 'market-data-collector'):
                code = (
                    "import os, hashlib; from pathlib import Path; "
                    "from market_data.archive import FilesystemRawArchiveObjectStore; "
                    "from scripts.db.fact_header_v2_handoff import commit_handoff, activate_handoff_policy; "
                    "assert os.getuid()==70; "
                    "root=Path('/app/logs/market-structure'); "
                    "assert (root/'legacy-spool-proof').read_text()=='retained-spool'; "
                    "stage=root/'publication-probe'; stage.write_bytes(b'preserved-history'); "
                    "store=FilesystemRawArchiveObjectStore(Path('/qt-history/archives/objects')); "
                    f"store.put_verified(object_key='rehearsal/{service}',source_path=stage,"
                    "expected_sha256=hashlib.sha256(stage.read_bytes()).hexdigest()); "
                    "assert stage.stat().st_dev != store.root.stat().st_dev; "
                    "Path('/app/reports/permission-probe').write_text('owned'); "
                    "print('storage_writer_and_legacy_spool_verified')")
                assert 'storage_writer_and_legacy_spool_verified' in run(
                    compose + ['exec', '-T', service, 'python', '-c', code], env=env).stdout
            for service, other in (('backend', 'market-data-collector'), ('market-data-collector', 'backend')):
                code = ("from pathlib import Path; from market_data.archive import FilesystemRawArchiveObjectStore; "
                        "store=FilesystemRawArchiveObjectStore(Path('/qt-history/archives/objects'),writable=False); "
                        f"assert store.local_path('rehearsal/{other}').read_bytes()==b'preserved-history'")
                run(compose + ['exec', '-T', service, 'python', '-c', code], env=env)
            code = '\n'.join([
                "from pathlib import Path",
                "from portal.backend.db import db",
                "from core.storage_inventory import read_storage_inventory",
                "from portal.backend.service.storage.header_resources import observe_header_resources",
                "from portal.backend.service.storage.maintenance_runtime import storage_maintenance_runners",
                "targets=read_storage_inventory(Path('/run/quanttrad/storage-inventory.json'))",
                "with db.session() as session:",
                "    observed=observe_header_resources(session.connection(),targets,pg_controldata=Path('/usr/lib/postgresql/15/bin/pg_controldata'))",
                "    assert len({v.device_id for v in observed.capacity.values()})==2",
                "runners=storage_maintenance_runners(db,storage_root=Path('/qt-history/archives'),limits_path='/run/quanttrad/storage-maintenance.json')",
                "assert set(runners)=={'service','history_runner','recovery_runner'}",
                "print('postgres_namespace_and_maintenance_wiring_verified')",
            ])
            assert 'postgres_namespace_and_maintenance_wiring_verified' in run(
                compose + ['exec', '-T', 'market-data-collector', 'python', '-c', code], env=env).stdout
        keeper = project + '-history-lifetime'
        keeper_started = False
        try:
            if args.storage_layout:
                run(compose + ['create', '--no-build', '--pull', 'never', 'tsdb'], env=env)
                # A Docker tmpfs volume loses its bytes after its last user
                # stops. Keep this synthetic HDD mounted across service
                # recreation, as a physical filesystem would remain mounted.
                run(['docker', 'run', '--detach', '--name', keeper, '--pull', 'never',
                     '--network', 'none', '--user', '70:70', '--entrypoint', 'python',
                     '--volume', project+'-history:/history', 'quanttrad-backend:'+revision,
                     '-c', 'import time; time.sleep(1800)'], env=env)
                keeper_started = True
                preparation = '\n'.join([
                    'import os,json; from pathlib import Path',
                    "source=Path('/source'); history=Path('/history')",
                    "for path in (source, source/'pgdata', source/'working', history, history/'archives'):",
                    '    path.mkdir(exist_ok=True); os.chown(path,70,70); path.chmod(0o700)',
                    "proof=source/'working/legacy-spool-proof'; proof.write_text('retained-spool'); os.chown(proof,70,70)",
                    "print(json.dumps({name:[os.major(path.stat().st_dev),os.minor(path.stat().st_dev)] for name,path in [('ssd',source),('hdd',history)]}))",
                ])
                devices = json.loads(run(['docker','run','--rm','--pull','never','--network','none',
                    '--user','0:0','--entrypoint','python','--volume',project+'-postgres:/source',
                    '--volume',project+'-history:/history','quanttrad-backend:'+revision,'-c',preparation],env=env).stdout)
                assert devices['ssd'] != devices['hdd']
                for key,(major,minor) in devices.items():
                    (udev/'data'/f'b{major}:{minor}').write_text('E:ID_FS_UUID=fixture-'+key+'\n')
            run(compose + ['up', '--detach', '--no-build', '--pull', 'never', '--wait', '--wait-timeout', '360'], env=env)
            storage_probe()
            database('CREATE TABLE public.qt_deployment_rehearsal (value text PRIMARY KEY); INSERT INTO public.qt_deployment_rehearsal VALUES (\'retained\')')
            first_health = worker_state()
            run(compose + ['stop', 'market-data-collector'], env=env)
            old = cid('market-data-collector')
            assert run(['docker', 'inspect', '--format', '{{.State.ExitCode}}', old], env=env).stdout.strip() == '0'
            logs = run(['docker', 'logs', '--tail', '200', old], env=env)
            assert 'market_data_collector_stopped' in logs.stdout + logs.stderr
            run(compose + ['up', '--detach', '--no-build', '--pull', 'never', '--force-recreate', '--wait', '--wait-timeout', '360'], env=env)
            storage_probe()
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
            if args.storage_layout:
                print('PASS: fixed SSD/HDD UID70 layout, preserved legacy spool bytes, shared archive ownership, packaged operator, PostgreSQL namespace and maintenance configuration before/after recreation; synthetic filesystems and no provider enrollment', flush=True)
            print('PASS: actual QT clean bootstrap, API/frontends, initializer, collector heartbeat, clean worker stop, recreation, exact image revision, and PostgreSQL data retention; provider enrollment and network egress disabled', flush=True)
        except BaseException:
            diagnostics = run(compose + ['logs', '--tail', '100', '--no-color'], env=env, ok=False)
            print(diagnostics.stdout[-16000:] + diagnostics.stderr[-2000:], flush=True)
            raise
        finally:
            if keeper_started:
                run(['docker', 'rm', '--force', keeper], env=env)
            run(compose + ['down', '--volumes', '--remove-orphans'], env=env)


if __name__ == '__main__':
    main()
