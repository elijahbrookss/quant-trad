#!/usr/bin/env python3
"""Transport, run and aggregate the existing bounded deployment proof groups."""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
GROUPS = ('alerts', 'lifecycle', 'restore', 'core', *(f'online-{i}' for i in range(6)))
SOURCE_IMAGES = ('backend', 'frontend', 'frontend-v2', 'terminal', 'storage-test')
ALLOWED_SKIP = 'tests/test_market_data/test_storage_online_controller_db.py::test_controller_prepared_transaction_refuses_without_client'
SKIP_REASON = 'requires disposable max_prepared_transactions > 0'

def read(path):
    return json.loads(Path(path).read_text())

def write(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')

def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def output(*args, timeout=60):
    return subprocess.check_output(args, text=True, timeout=timeout).strip()

def identity():
    revision = output('git', 'rev-parse', 'HEAD')
    if revision != os.environ.get('GITHUB_SHA') or not re.fullmatch(r'[a-f0-9]{40}', revision):
        raise ValueError('proof checkout differs from workflow source')
    return {'revision': revision,
            'source_tree_hash': output('python3', 'scripts/provenance/source_tree_hash.py', '--git-revision', revision),
            'run_id': int(os.environ['GITHUB_RUN_ID']), 'attempt': int(os.environ['GITHUB_RUN_ATTEMPT'])}

def results(log):
    return [json.loads(line.split('QT_PROOF_RESULT ', 1)[1])
            for line in log.splitlines() if line.startswith('QT_PROOF_RESULT ')]

def allocate(nodes):
    if not nodes or len(nodes) != len(set(nodes)):
        raise ValueError('empty or duplicate online collection')
    seeds = read(ROOT / 'scripts/ci/online_proof_allocation.json')['shards']
    if len(seeds) != 6 or len(sum(seeds, [])) != len(set(sum(seeds, []))):
        raise ValueError('invalid scheduling seeds')
    shards = [[n for n in seed if n in nodes] for seed in seeds]
    known = set(sum(shards, []))
    for node in sorted(set(nodes) - known):
        min(shards, key=len).append(node)
    if any(not shard for shard in shards) or Counter(sum(shards, [])) != Counter(nodes):
        raise ValueError('incomplete shard allocation')
    return shards

def validate_result(result, expected):
    if result.get('exitstatus') != 0 or Counter(result.get('nodes', [])) != Counter(expected):
        raise ValueError('proof collection/result mismatch')
    reports = result.get('reports', [])
    if any(r.get('nodeid') not in expected for r in reports):
        raise ValueError('unexpected proof report')
    for node in expected:
        phases = [r for r in reports if r['nodeid'] == node]
        if any(r.get('outcome') not in ('passed', 'skipped') for r in phases):
            raise ValueError('failed proof phase')
        counts = Counter(r.get('phase') for r in phases)
        skips = [r for r in phases if r.get('outcome') == 'skipped']
        if skips:
            if (node != ALLOWED_SKIP or len(skips) != 1 or skips[0].get('reason') != SKIP_REASON
                    or skips[0]['phase'] not in ('setup', 'call')):
                raise ValueError('unexpected skipped proof')
            wanted = {'setup': 1, 'teardown': 1}
            if skips[0]['phase'] == 'call':
                wanted['call'] = 1
        else:
            wanted = {'setup': 1, 'call': 1, 'teardown': 1}
        if counts != Counter(wanted):
            raise ValueError('missing or duplicate proof phases')

def image_snapshot(manifest):
    observed = {}
    for name, row in manifest['images'].items():
        image = json.loads(output('docker', 'image', 'inspect', row['tag']))[0]
        if image['Id'] != row['id']:
            raise ValueError('proof image identity changed: ' + name)
        if name in SOURCE_IMAGES:
            env = dict(e.split('=', 1) for e in image['Config']['Env'])
            if (env.get('QT_IMAGE_SOURCE_REVISION') != manifest['identity']['revision']
                    or env.get('QT_IMAGE_SOURCE_TREE_HASH') != manifest['identity']['source_tree_hash']):
                raise ValueError('proof image source mismatch: ' + name)
        observed[name] = image['Id']
    return observed

def verify_manifest(manifest):
    if manifest.get('schema') != 'qt.deployment_proofs.v1' or manifest.get('identity') != identity():
        raise ValueError('stale or wrong source/run/attempt proof manifest')
    if not re.fullmatch(r'[a-f0-9]{64}', manifest.get('archive_sha256', '')):
        raise ValueError('missing image archive digest')
    nodes = manifest.get('collection_nodes', [])
    if Counter(sum(manifest.get('online_shards', []), [])) != Counter(nodes):
        raise ValueError('shards differ from original complete collection')
    expected_images = {*SOURCE_IMAGES, 'database', 'storage-database'}
    if set(manifest.get('images', {})) != expected_images or any(
            not re.fullmatch(r'sha256:[a-f0-9]{64}', row.get('id', '')) for row in manifest['images'].values()):
        raise ValueError('missing or invalid bound proof image')
    if len(manifest.get('online_shards', [])) != 6 or allocate(nodes) != manifest['online_shards']:
        raise ValueError('wrong or incomplete proof allocation')

def resources(root, minimum_disk):
    available = next(int(line.split()[1]) * 1024 for line in Path('/proc/meminfo').read_text().splitlines()
                     if line.startswith('MemAvailable:'))
    snapshot = {'cpu_count': os.cpu_count(), 'available_memory_bytes': available,
                'free_disk_bytes': shutil.disk_usage(root).free, 'required_disk_bytes': minimum_disk}
    print('ci_proof_resources ' + json.dumps(snapshot), flush=True)
    if available < 4 * 1024**3 or snapshot['free_disk_bytes'] < minimum_disk:
        raise ValueError('insufficient disposable runner memory/disk for bounded proof transport')
    return snapshot


def pack(root):
    root.mkdir(parents=True, exist_ok=False)
    resources(root, 3 * 1024**3)
    source = identity()
    revision = source['revision']
    tags = {name: f'quanttrad-{name}:{revision}' for name in ('backend', 'frontend', 'frontend-v2')}
    tags.update({'terminal': 'qt-terminal-runtime-proof', 'storage-test': 'qt-incremental-runtime-proof',
                 'database': 'quanttrad-postgres:2.14.2-pg15',
                 'storage-database': 'timescale/timescaledb:2.14.2-pg15'})
    manifest = {'schema': 'qt.deployment_proofs.v1', 'identity': source,
                'images': {name: {'tag': tag, 'id': output('docker', 'image', 'inspect', '--format', '{{.Id}}', tag)}
                           for name, tag in tags.items()}}
    image_snapshot(manifest)
    selection = read(ROOT / 'scripts/ci/online_proof_selection.json')
    collected = output('docker', 'run', '--rm', '--network', 'none', '--read-only',
        '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m', '--env', 'QT_DISABLE_DOTENV=1',
        '--env', 'QT_STORAGE_DEMO=1', '--env', 'RUN_DB_TESTS=1', '--env', 'QT_DB_TEST_ISOLATED=1',
        '--env', 'PYTHONPATH=/app/src:/app', '--entrypoint', 'python', tags['storage-test'],
        '-m', 'pytest', '--collect-only', '-q', '-p', 'scripts.ci.proof_pytest', '-m', 'db', *selection,
        timeout=120)
    collection = results(collected)
    if len(collection) != 1 or collection[0]['exitstatus'] != 0:
        raise ValueError('online collection failed')
    manifest['collection_nodes'] = collection[0]['nodes']
    manifest['online_shards'] = allocate(manifest['collection_nodes'])
    subprocess.run(['docker', 'save', '--output', str(root / 'images.tar'), *tags.values()], check=True, timeout=600)
    manifest['archive_sha256'] = sha(root / 'images.tar')
    write(root / 'proof-manifest.json', manifest)
    # Keep the production manifest captured before any proofs; export occurs only
    # after the complete aggregate succeeds with the same loaded image identities.
    package = root / 'release-package'
    subprocess.run(['python3', 'scripts/automation/release_package.py', 'capture', '--root', str(package)], check=True)

def load(root):
    manifest = read(root / 'proof-manifest.json')
    verify_manifest(manifest)
    if sha(root / 'images.tar') != manifest['archive_sha256']:
        raise ValueError('image transport checksum mismatch')
    resources(root, 2 * (root / 'images.tar').stat().st_size + 1024**3)
    subprocess.run(['docker', 'load', '--input', str(root / 'images.tar')], check=True, timeout=600)
    image_snapshot(manifest)
    for suite in ('storage-demo', 'incremental-recovery'):
        (root / ('compose-' + suite + '.yml')).write_text(compose_override(manifest, suite))
    return manifest


def compose_override(manifest, suite):
    if suite not in ('storage-demo', 'incremental-recovery'):
        raise ValueError('prebuilt route is bounded to owned storage proof suites')
    images = {'test': manifest['images']['storage-test']['id'],
              'timescaledb': manifest['images']['database' if suite == 'incremental-recovery' else 'storage-database']['id']}
    if suite == 'incremental-recovery':
        images['restored'] = manifest['images']['database']['id']
    # Explicitly remove inherited build recipes. compose run has no --no-build;
    # an image-only model plus --pull never cannot rebuild/substitute a runtime.
    return 'services:\n' + ''.join(
        '  ' + name + ':\n    image: ' + json.dumps(image) + '\n    build: !reset null\n    pull_policy: never\n'
        for name, image in images.items())


def prebuilt(root, suite):
    manifest = read(root / 'proof-manifest.json')
    verify_manifest(manifest)
    image_snapshot(manifest)
    expected = compose_override(manifest, suite)
    if (root / ('compose-' + suite + '.yml')).read_text() != expected:
        raise ValueError('unexpected prebuilt Compose override')

def validate_compose_model(manifest, suite, model):
    expected = {'test': manifest['images']['storage-test']['id'],
                'timescaledb': manifest['images']['database' if suite == 'incremental-recovery' else 'storage-database']['id']}
    if suite == 'incremental-recovery':
        expected['restored'] = manifest['images']['database']['id']
    if suite not in ('storage-demo', 'incremental-recovery') or set(model.get('services', {})) != set(expected):
        raise ValueError('unexpected proof Compose services')
    for name, image in expected.items():
        service = model['services'][name]
        if (service.get('image') != image or service.get('build') is not None
                or service.get('pull_policy') != 'never' or service.get('ports')):
            raise ValueError('proof Compose model permits image substitution/build/pull or host ports')
    if model.get('networks', {}).get('default', {}).get('internal') is not True:
        raise ValueError('proof Compose network must remain internal')
    print('ci_prebuilt_model_verified suite=' + suite)


def run(root, group, evidence):
    if group not in GROUPS:
        raise ValueError('unknown bounded proof group')
    manifest = read(root / 'proof-manifest.json')
    verify_manifest(manifest)
    before = image_snapshot(manifest)
    evidence.mkdir(parents=True, exist_ok=False)
    if group.startswith('online-'):
        nodes = manifest['online_shards'][int(group.removeprefix('online-'))]
        command = ['bash', 'scripts/ci/run_test_suite.sh', 'storage-demo', *nodes,
                   '-p', 'scripts.ci.proof_pytest', '-s', '-o', 'cache_dir=/tmp/qt-proof-pytest-cache']
    else:
        command = ['bash', 'scripts/ci/proof_' + group + '.sh']
    started = time.time()
    env = {**os.environ, 'QT_CI_PROOF_ROOT': str(root), 'QT_CI_TEST_PHASES': '1'}
    with (evidence / 'proof.log').open('w') as log:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        for line in process.stdout:
            log.write(line); log.flush(); print(line, end='', flush=True)
        status = process.wait()
    receipt = {'identity': manifest['identity'], 'group': group, 'status': status,
               'archive_sha256': manifest['archive_sha256'], 'images_before': before,
               'elapsed_seconds': time.time() - started, 'log_sha256': sha(evidence / 'proof.log')}
    receipt['results'] = results((evidence / 'proof.log').read_text())
    try:
        receipt['images_after'] = image_snapshot(manifest)
        if status != 0:
            raise ValueError('proof command failed')
        if group.startswith('online-'):
            if len(receipt['results']) != 1:
                raise ValueError('missing or duplicate online result')
            validate_result(receipt['results'][0], nodes)
        if group == 'restore':
            if len(receipt['results']) != 2:
                raise ValueError('missing restore proof result')
            for result in receipt['results']:
                if not result.get('nodes'):
                    raise ValueError('empty restore proof')
                validate_result(result, result['nodes'])
        receipt['complete'] = True
    except Exception as exc:
        receipt['complete'] = False
        receipt['error'] = str(exc)
        raise
    finally:
        write(evidence / 'receipt.json', receipt)

def aggregate(manifest, receipts):
    verify_manifest(manifest)
    if Counter(r.get('group') for r in receipts) != Counter(GROUPS):
        raise ValueError('missing, duplicate or unexpected proof group')
    images = {name: row['id'] for name, row in manifest['images'].items()}
    covered = []
    for receipt in receipts:
        if (receipt.get('identity') != manifest['identity'] or receipt.get('complete') is not True
                or receipt.get('status') != 0 or receipt.get('archive_sha256') != manifest['archive_sha256']
                or receipt.get('images_before') != images or receipt.get('images_after') != images):
            raise ValueError('unqualified proof receipt: source/image/attempt/status mismatch')
        if receipt['group'] == 'restore':
            if len(receipt.get('results', [])) != 2:
                raise ValueError('missing restore result')
            for result in receipt['results']:
                if not result.get('nodes'):
                    raise ValueError('empty restore collection')
                validate_result(result, result['nodes'])
        if receipt['group'].startswith('online-'):
            nodes = manifest['online_shards'][int(receipt['group'].removeprefix('online-'))]
            if len(receipt.get('results', [])) != 1:
                raise ValueError('missing online result')
            validate_result(receipt['results'][0], nodes)
            covered.extend(receipt['results'][0]['nodes'])
    if Counter(covered) != Counter(sum(manifest['online_shards'], [])):
        raise ValueError('incomplete aggregate online proof coverage')
    return {'schema': 'qt.deployment_proof_qualification.v1', 'identity': manifest['identity'],
            'images': images, 'archive_sha256': manifest['archive_sha256'],
            'groups': list(GROUPS), 'online_node_count': len(covered), 'complete': True}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=('pack', 'load', 'prebuilt', 'compose-model', 'run', 'aggregate'))
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--group', choices=GROUPS)
    p.add_argument('--suite')
    p.add_argument('--evidence', type=Path)
    args = p.parse_args()
    root = args.root.resolve()
    if args.action == 'pack': pack(root)
    elif args.action == 'load': load(root)
    elif args.action == 'prebuilt': prebuilt(root, args.suite)
    elif args.action == 'compose-model':
        import sys
        manifest = read(root / 'proof-manifest.json')
        verify_manifest(manifest)
        validate_compose_model(manifest, args.suite, json.load(sys.stdin))
    elif args.action == 'run': run(root, args.group, args.evidence)
    else:
        manifest = read(root / 'proof-manifest.json')
        receipts = []
        for path in args.evidence.rglob('receipt.json'):
            receipt = read(path)
            if sha(path.parent / 'proof.log') != receipt.get('log_sha256'):
                raise ValueError('proof diagnostic evidence checksum mismatch')
            receipts.append(receipt)
        qualification = aggregate(manifest, receipts)
        image_snapshot(manifest)
        write(root / 'release-package/qualification.json', qualification)
        print(json.dumps(qualification))

if __name__ == '__main__':
    main()
