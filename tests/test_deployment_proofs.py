from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('deployment_proofs', ROOT / 'scripts/ci/deployment_proofs.py')
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)
IDENTITY = {'revision': 'a'*40, 'source_tree_hash': 'b'*64, 'run_id': 12, 'attempt': 2}


def result(nodes):
    return {'nodes': nodes, 'exitstatus': 0, 'reports': [
        {'nodeid': node, 'phase': phase, 'outcome': 'passed'}
        for node in nodes for phase in ('setup', 'call', 'teardown')]}


def packet(monkeypatch):
    monkeypatch.setattr(proof, 'identity', lambda: IDENTITY)
    seeds = proof.read(ROOT / 'scripts/ci/online_proof_allocation.json')['shards']
    nodes = [shard[0] for shard in seeds] + [proof.ALLOWED_SKIP]
    shards = proof.allocate(nodes)
    manifest = {'schema': 'qt.deployment_proofs.v1', 'identity': IDENTITY,
                'archive_sha256': 'c'*64, 'collection_nodes': nodes, 'online_shards': shards,
                'images': {name: {'tag': name, 'id': 'sha256:' + format(i, '064x')}
                           for i, name in enumerate((*proof.SOURCE_IMAGES, 'database', 'storage-database'))}}
    images = {name: row['id'] for name, row in manifest['images'].items()}
    receipts = [{'identity': IDENTITY, 'group': group, 'complete': True, 'status': 0,
                 'archive_sha256': 'c'*64, 'images_before': images, 'images_after': images,
                 'results': [result(shards[int(group[-1])])] if group.startswith('online-') else
                            [result(['restore-current']), result(['restore-raw'])] if group == 'restore' else []}
                for group in proof.GROUPS]
    return manifest, receipts


def test_all_collected_parameter_cases_are_allocated_once_and_new_nodes_are_included():
    seeds = proof.read(ROOT / 'scripts/ci/online_proof_allocation.json')['shards']
    nodes = sum(seeds, [])
    assert len(nodes) == len(set(nodes)) == 120
    assert proof.allocate(nodes) == seeds
    shards = proof.allocate(nodes + ['tests/new.py::test_extra[variant]'])
    assert sorted(sum(shards, [])) == sorted(nodes + ['tests/new.py::test_extra[variant]'])
    assert any('test_storage_online_controller_db.py' in n for n in shards[0])
    with pytest.raises(ValueError): proof.allocate(nodes + [nodes[0]])
    with pytest.raises(ValueError): proof.allocate([])


def test_complete_exact_attempt_can_qualify_and_only_the_explicit_skip_is_allowed(monkeypatch):
    manifest, receipts = packet(monkeypatch)
    qualified = proof.aggregate(manifest, receipts)
    assert qualified['complete'] and qualified['online_node_count'] == 7
    for receipt in receipts:
        if receipt['group'].startswith('online-'):
            for row in receipt['results'][0]['reports']:
                if row['nodeid'] == proof.ALLOWED_SKIP and row['phase'] == 'call':
                    row.update(outcome='skipped', reason=proof.SKIP_REASON)
    assert proof.aggregate(manifest, receipts)['complete']


@pytest.mark.parametrize('fault', ['missing', 'duplicate', 'extra', 'failed', 'cancelled',
    'source', 'attempt', 'run', 'image-before', 'image-after', 'archive', 'no-result',
    'missing-node', 'duplicate-node', 'extra-node', 'missing-phase', 'duplicate-phase',
    'failed-phase', 'unexpected-skip', 'empty-restore', 'missing-restore'])
def test_aggregate_refuses_incomplete_or_mismatched_proofs(monkeypatch, fault):
    manifest, receipts = packet(monkeypatch)
    receipts = deepcopy(receipts)
    online = next(r for r in receipts if r['group'] == 'online-0')
    if fault == 'missing': receipts.pop()
    elif fault == 'duplicate': receipts.append(deepcopy(receipts[0]))
    elif fault == 'extra': receipts.append({**receipts[0], 'group': 'unreviewed'})
    elif fault == 'failed': receipts[0]['status'] = 1
    elif fault == 'cancelled': receipts[0]['complete'] = False
    elif fault in ('source', 'attempt', 'run'):
        receipts[0]['identity'][{'source':'revision', 'attempt':'attempt', 'run':'run_id'}[fault]] = 'wrong'
    elif fault.startswith('image-'): receipts[0]['images_'+fault.split('-')[1]] = {}
    elif fault == 'archive': receipts[0]['archive_sha256'] = 'wrong'
    elif fault == 'no-result': online['results'] = []
    elif fault == 'missing-node': online['results'][0]['nodes'].pop()
    elif fault == 'duplicate-node': online['results'][0]['nodes'].append(online['results'][0]['nodes'][0])
    elif fault == 'extra-node': online['results'][0]['nodes'].append('unexpected')
    elif fault == 'missing-phase': online['results'][0]['reports'].pop()
    elif fault == 'duplicate-phase': online['results'][0]['reports'].append(online['results'][0]['reports'][0])
    elif fault == 'failed-phase': online['results'][0]['reports'][0]['outcome'] = 'failed'
    elif fault == 'unexpected-skip': online['results'][0]['reports'][0].update(outcome='skipped', reason='wrong')
    elif fault == 'empty-restore': next(r for r in receipts if r['group']=='restore')['results'][0] = result([])
    elif fault == 'missing-restore': next(r for r in receipts if r['group']=='restore')['results'].pop()
    with pytest.raises(ValueError): proof.aggregate(manifest, receipts)


@pytest.mark.parametrize('field,value', [('identity', {}), ('schema', 'unknown'), ('archive_sha256', ''),
                                      ('images', {}), ('collection_nodes', [])])
def test_manifest_refuses_missing_source_image_attempt_or_original_collection(monkeypatch, field, value):
    manifest, _ = packet(monkeypatch)
    manifest[field] = value
    with pytest.raises(ValueError): proof.verify_manifest(manifest)


def test_plugin_records_real_collection_and_setup_call_teardown(tmp_path):
    test = tmp_path / 'test_synthetic.py'
    test.write_text('def test_ok(): assert True\n')
    run = subprocess.run([sys.executable, '-m', 'pytest', '-q', '-p', 'scripts.ci.proof_pytest', str(test)],
                         cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert run.returncode == 0, run.stdout + run.stderr
    records = proof.results(run.stdout)
    assert len(records) == 1
    proof.validate_result(records[0], records[0]['nodes'])


def test_workflow_keeps_protected_runs_recovery_and_complete_aggregate():
    workflow = yaml.safe_load((ROOT / '.github/workflows/test.yaml').read_text())
    cancel = workflow['concurrency']['cancel-in-progress']
    assert "github.event_name == 'pull_request'" in cancel
    assert 'github.head_ref' in cancel and "'feature/'" in cancel and "'hotfix/'" in cancel
    assert "'refs/heads/develop'" not in cancel and "'refs/heads/main'" not in cancel
    assert workflow['permissions'] == {'contents':'read'}
    jobs = workflow['jobs']
    assert jobs['deployment-proof-online']['strategy'] == {
        'fail-fast': False, 'max-parallel': 4, 'matrix': {'shard': list(range(6))}}
    assert set(jobs['deployment-contract']['needs']) == {'changes', 'deployment-proof-build',
        'deployment-proof-alerts', 'deployment-proof-lifecycle', 'deployment-proof-restore',
        'deployment-proof-core', 'deployment-proof-online'}
    assert 'always()' in jobs['deployment-contract']['if']
    assert jobs['committed-recovery']['strategy']['fail-fast'] is False
    aggregate = jobs['deployment-contract']['steps']
    assert any(s.get('name')=='Attest complete deployment proof coverage' for s in aggregate)
    assert not any(s.get('continue-on-error') for job in jobs.values() for s in job['steps'])


def test_prebuilt_only_allows_exact_owned_storage_overrides(monkeypatch, tmp_path):
    manifest, _ = packet(monkeypatch)
    proof.write(tmp_path / 'proof-manifest.json', manifest)
    monkeypatch.setattr(proof, 'image_snapshot', lambda m: None)
    expected = {'services': {'test': {'image': manifest['images']['storage-test']['id']},
                            'timescaledb': {'image': manifest['images']['storage-database']['id']}}}
    proof.write(tmp_path / 'compose-storage-demo.json', expected)
    proof.prebuilt(tmp_path, 'storage-demo')
    expected['services']['test']['volumes'] = ['/host:/host']
    proof.write(tmp_path / 'compose-storage-demo.json', expected)
    with pytest.raises(ValueError, match='unexpected prebuilt'): proof.prebuilt(tmp_path, 'storage-demo')
    with pytest.raises(ValueError, match='bounded'): proof.prebuilt(tmp_path, 'db')


def test_image_identity_and_source_both_must_match(monkeypatch):
    manifest, _ = packet(monkeypatch)
    def inspect(*args):
        tag = args[-1]
        row = next(row for row in manifest['images'].values() if row['tag'] == tag)
        return json.dumps([{'Id': row['id'], 'Config': {'Env': [
            'QT_IMAGE_SOURCE_REVISION=' + IDENTITY['revision'],
            'QT_IMAGE_SOURCE_TREE_HASH=' + IDENTITY['source_tree_hash']]}}])
    monkeypatch.setattr(proof, 'output', inspect)
    assert len(proof.image_snapshot(manifest)) == 7
    monkeypatch.setattr(proof, 'output', lambda *a: inspect(*a).replace(IDENTITY['source_tree_hash'], 'd'*64))
    with pytest.raises(ValueError, match='source mismatch'): proof.image_snapshot(manifest)
    monkeypatch.setattr(proof, 'output', lambda *a: inspect(*a).replace('sha256:', 'wrong:'))
    with pytest.raises(ValueError, match='identity changed'): proof.image_snapshot(manifest)


def test_wrong_transport_checksum_is_rejected_before_docker_load(monkeypatch, tmp_path):
    manifest, _ = packet(monkeypatch)
    proof.write(tmp_path / 'proof-manifest.json', manifest)
    (tmp_path / 'images.tar').write_bytes(b'wrong archive')
    monkeypatch.setattr(proof.subprocess, 'run', lambda *a, **k: pytest.fail('must not load mismatched bytes'))
    with pytest.raises(ValueError, match='checksum mismatch'): proof.load(tmp_path)
