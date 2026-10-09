"""Only a proven pre-preparation failure admits explicit committed recovery."""
from contextlib import nullcontext
import hashlib
import json
from types import SimpleNamespace
import time

import pytest

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_recovery as recovery
from scripts.automation import storage_online_repositories as repositories
from scripts.automation import storage_online_operation as operation


def test_database_archiver_probe_uses_database_uid_and_no_keys(monkeypatch):
    calls = []
    def docker(*args, **kwargs):
        calls.append((args, kwargs))
        return "pgBackRest 2.59.1\n"
    monkeypatch.setattr(host, 'docker', docker)
    assert recovery.inspect_database_archiver('database') == {
        'pgbackrest_version': 'pgBackRest 2.59.1'}
    assert calls == [(('exec', '--user', '70:70', 'database',
                      '/usr/local/bin/pgbackrest', 'version'), {'timeout': 10})]


@pytest.mark.parametrize('version', ['pgBackRest 2.58.0', '', 'unconfirmed'])
def test_database_archiver_probe_rejects_wrong_or_missing_version(monkeypatch, version):
    monkeypatch.setattr(host, 'docker', lambda *a, **kw: version)
    with pytest.raises(RuntimeError, match='archiver_version_mismatch'):
        recovery.inspect_database_archiver('database')


def test_missing_database_archiver_refuses_preflight_before_other_preparation(tmp_path, monkeypatch):
    def missing(*args, **kwargs):
        raise RuntimeError('storage_pause_docker_failed: operation=exec exit=127')
    monkeypatch.setattr(host, 'docker', missing)
    monkeypatch.setattr(operation.launch, 'inspect_candidate_image',
                        lambda *a: pytest.fail('continued after missing database tool'))
    source = tmp_path/'source'
    with pytest.raises(RuntimeError, match='archiver_unavailable.*before pausing collection'):
        operation.inspect_operation_configuration(tmp_path, project='fixture',
            source_revision='owned', source_image='owned', image='candidate', request={},
            inventory_path=tmp_path, keys_root=tmp_path, socket_volume='socket',
            spool_destination=tmp_path/'spool', roots={str(source): [], str(source/'objects'): []},
            rows={'tsdb': {'id': 'database'}}, recipe_sha256='owned', deadline=time.monotonic()+20)


@pytest.fixture
def request_file(tmp_path):
    path = tmp_path/'continuation.json'
    value = dict(schema_version='qt.storage_repository_continuation.v1',
        operation_sha256='a'*64, final_sha256='b'*64, duration_seconds=120)
    host.save_receipt(path, value, initial=True)
    return path, value


@pytest.mark.parametrize('change', [dict(duration_seconds=True), dict(duration_seconds=0),
    dict(duration_seconds=601), dict(final_sha256='bad'), dict(extra='ignored'),
    dict(schema_version='future')])
def test_request_refuses_unbounded_or_ambiguous_inputs(request_file, change):
    path, value = request_file
    host.save_receipt(path, {**value, **change}, initial=False)
    with pytest.raises(ValueError, match='request_invalid'):
        recovery._continuation_request(path)


@pytest.fixture
def failed_preparer(tmp_path, monkeypatch):
    helper = 'a'*64
    command = ['-c', 'owned fixed command', json.dumps(dict(
        incremental_config='/run/quanttrad/recovery/incremental-config.json'))]
    recipe = dict(services=dict(prepare=dict(command=command)))
    host.save_receipt(tmp_path/repositories.RECIPE, recipe, initial=True)
    saved = dict(phase='recovery_repository_preparing', repositories=dict(
        completed=['logins', 'create'], inflight='prepare', report=None,
        helper_retirement=None, finished_at=None, helper_id=helper,
        recipe_sha256=host.digest(recipe), helper_contract='owned'))
    status = dict(Status='exited', ExitCode=1, Pid=0, Running=False, Paused=False,
        Restarting=False, Dead=False, OOMKilled=False)
    logs = "FileNotFoundError: [Errno 2] No such file or directory: '/run/quanttrad/recovery/incremental-config.json'\n"
    state = dict(status=status, logs=logs)
    monkeypatch.setattr(host, 'database_details', lambda identity: dict(config=dict(Cmd=command)))
    monkeypatch.setattr(repositories, '_preparer_contract', lambda *a: 'owned')
    monkeypatch.setattr(host, 'docker', lambda *a: json.dumps(state['status']))
    def read_logs(args, **kwargs):
        assert args == ['docker', 'logs', '--tail', '60', helper]
        assert kwargs['stderr'] == recovery.subprocess.STDOUT
        assert kwargs['timeout'] <= 10
        return SimpleNamespace(stdout=state['logs'])
    monkeypatch.setattr(recovery.subprocess, 'run', read_logs)
    return tmp_path, saved, state


def test_confirmed_preparation_failure_has_no_mutating_call(failed_preparer):
    path, saved, state = failed_preparer
    with host.docker_deadline(time.monotonic()+30):
        observed = recovery._failed_preparer(path, saved, {})
    assert observed['helper_state_sha256'] == host.digest(state['status'])
    assert observed['helper_logs_sha256'] == hashlib.sha256(state['logs'].encode()).hexdigest()


@pytest.mark.parametrize('change', [dict(ExitCode=0), dict(OOMKilled=True),
    dict(Pid=17), dict(Running=True), dict(Status='created')])
def test_uncertain_or_successful_helper_is_never_retried(failed_preparer, change):
    path, saved, state = failed_preparer
    state['status'].update(change)
    with host.docker_deadline(time.monotonic()+30), pytest.raises(RuntimeError, match='outcome_uncertain'):
        recovery._failed_preparer(path, saved, {})


@pytest.mark.parametrize('fault', ['unknown_exception', 'later_failure', 'recipe_changed', 'oversized_logs'])
def test_only_missing_input_before_repository_effects_is_supported(failed_preparer, fault):
    path, saved, state = failed_preparer
    if fault == 'unknown_exception':
        state['logs'] = 'TimeoutError: repository result uncertain'
    elif fault == 'later_failure':
        saved['repositories']['completed'].append('prepare')
        saved['repositories']['inflight'] = 'settings'
    elif fault == 'recipe_changed':
        saved['repositories']['recipe_sha256'] = 'b'*64
    else:
        state['logs'] = 'x'*16385 + state['logs']
    with host.docker_deadline(time.monotonic()+30), pytest.raises(RuntimeError):
        recovery._failed_preparer(path, saved, {})


def test_missing_live_process_requires_scoped_continuation(monkeypatch):
    worker = dict(container_id='a'*64, binding={}, contract='owned', deadline=time.time()-1)
    saved = dict(binding=dict(worker_started_at='original-start'))
    check = lambda: None
    status = dict(Status='exited', Pid=0, StartedAt='original-start', Running=False,
                  Paused=False, Restarting=False, Dead=False)
    monkeypatch.setattr(recovery.launch, '_admit', lambda *a: None)
    monkeypatch.setattr(host, 'docker', lambda *a: json.dumps(status))
    with pytest.raises(RuntimeError, match='deadline_expired'):
        recovery.admit_retired_reader(None, worker, saved, check, time.monotonic()+30)
    token = recovery._CONTINUATION.set(check)
    try:
        recovery.admit_retired_reader(None, worker, saved, check, time.monotonic()+30)
        with pytest.raises(RuntimeError, match='deadline_expired'):
            recovery.admit_retired_reader(None, worker, saved, lambda: None, time.monotonic()+30)
        with pytest.raises(RuntimeError, match='deadline_expired'):
            recovery.admit_retired_reader(None, worker, saved, check, time.monotonic()-1)
    finally:
        recovery._CONTINUATION.reset(token)


def test_inspection_is_read_only_and_any_existing_intent_refuses_replay(tmp_path, monkeypatch):
    operation_file = tmp_path/'operation.json'
    host.save_receipt(operation_file, {}, initial=True)
    request = tmp_path/'request.json'
    host.save_receipt(request, dict(schema_version='qt.storage_repository_continuation.v1',
        operation_sha256=hashlib.sha256(operation_file.read_bytes()).hexdigest(),
        final_sha256='b'*64, duration_seconds=120), initial=True)
    monkeypatch.setattr(operation, 'load_operation_plan', lambda path: dict(state_root=str(tmp_path)))
    monkeypatch.setattr(host, 'deployment_lock', lambda path: nullcontext())
    monkeypatch.setattr(recovery, '_inspect_continuation', lambda *a: (
        dict(deadline=1), {}, {}, tmp_path, dict(plan_id='confirmed')))
    original = {p.name:p.read_bytes() for p in tmp_path.iterdir()}
    result = recovery.continue_repositories(operation_file, request_file=request)
    assert result['storage_mutations_performed'] is False
    assert {p.name:p.read_bytes() for p in tmp_path.iterdir()} == original
    host.save_receipt(tmp_path/recovery.CONTINUATION_STATE, dict(phase='preparing'), initial=True)
    with pytest.raises(RuntimeError, match='intent_exists_reconcile_required'):
        recovery.continue_repositories(operation_file, request_file=request, execute=True)


def test_execution_uses_recovery_allowance_after_inspection_scope_ends(tmp_path, monkeypatch):
    from scripts.automation import storage_online_final as final
    operation_file = tmp_path/'operation.json'
    host.save_receipt(operation_file, {}, initial=True)
    wall, boot, monotonic = time.time(), final._boot_seconds(), time.monotonic()
    saved = dict(started_at=wall-200, started_boot=boot-200, duration_seconds=60,
        deadline=wall-140, deadline_boot=boot-140, repositories={},
        switch=dict(deadline_monotonic=monotonic-140))
    host.save_receipt(tmp_path/final.STATE, saved, initial=True)
    host.save_receipt(tmp_path/repositories.RECIPE, {}, initial=True)
    request = tmp_path/'request.json'
    host.save_receipt(request, dict(schema_version='qt.storage_repository_continuation.v1',
        operation_sha256=hashlib.sha256(operation_file.read_bytes()).hexdigest(),
        final_sha256=hashlib.sha256((tmp_path/final.STATE).read_bytes()).hexdigest(),
        duration_seconds=120), initial=True)
    plan = dict(state_root=str(tmp_path), project='fixture', source_image='owned',
        limits=dict(preparation_seconds=30, final_seconds=120, recovery_seconds=30,
            runtime_seconds=60, spool_max_bytes=100, spool_max_entries=10,
            spool_reserve_bytes=10, repository_max_bytes=100,
            repository_reserve_bytes=10, recent_free_bytes=10))
    monkeypatch.setattr(operation, 'load_operation_plan', lambda path: plan)
    monkeypatch.setattr(host, 'deployment_lock', lambda path: nullcontext())
    inspection_deadlines = []
    def inspect(*args):
        inspection_deadlines.append(host.current_docker_deadline()-time.monotonic())
        return saved, {}, {}, tmp_path, dict(plan_id='confirmed', helper_id='a'*64)
    monkeypatch.setattr(recovery, '_inspect_continuation', inspect)
    monkeypatch.setattr(recovery, '_held_continuation', lambda *a, **kw: nullcontext(lambda: None))
    monkeypatch.setattr(host, 'docker', lambda *args: '')
    class ReachedRepositoryPreparation(Exception):
        pass
    def prepare(*args, **kwargs):
        assert 110 < host.current_docker_deadline()-time.monotonic() <= 120
        raise ReachedRepositoryPreparation
    monkeypatch.setattr(repositories, 'prepare_repositories', prepare)
    with pytest.raises(ReachedRepositoryPreparation):
        recovery.continue_repositories(operation_file, request_file=request, execute=True)
    assert 0 < inspection_deadlines[0] <= 60
    assert 110 < inspection_deadlines[1] <= 120


@pytest.mark.parametrize('other', ['extend_attempt_seconds', 'capacity_file',
    'replacement_package_file', 'cancel_attempt_file', 'forward_package_file',
    'prepare_forward_keys_file', 'place_forward_lookups_file', 'reschedule_forward_file',
    'prepare_forward_only'])
def test_continuation_cannot_be_combined_with_other_operations(other):
    with pytest.raises(ValueError, match='must_be_separate'):
        operation.run_operation_plan('unused', recover_repositories_file='request',
            **{other: True if other == 'prepare_forward_only' else 'unused'})


def test_cli_routes_explicit_continuation_without_opening_api(monkeypatch, capsys):
    from cli import main
    monkeypatch.setattr(main, '_client', lambda *a: pytest.fail('HTTP client opened'))
    calls = []
    def continuation(path, **kwargs):
        calls.append((path, kwargs))
        return dict(phase='inspected')
    monkeypatch.setattr(recovery, 'continue_repositories', continuation)
    parser = main.build_parser()
    for tail in ([], ['--execute']):
        args = parser.parse_args(['storage', 'migrate', '--operation-file', 'plan',
            '--recover-repositories-file', 'request', *tail])
        assert args.func(args) == 0
    assert calls == [('plan', dict(request_file='request', execute=False)),
                     ('plan', dict(request_file='request', execute=True))]
    assert 'inspected' in capsys.readouterr().out


def test_online_preflight_requires_incremental_settings_before_source_stop(monkeypatch):
    from portal.backend.service.storage import maintenance_runtime
    from scripts.automation import storage_handoff_pause as preserving
    import sys
    history = {key:dict(ssd=1, hdd=1) for key in
               ('temporary_bytes', 'growth_bytes_per_second', 'maintenance_bytes')}
    monkeypatch.setattr(maintenance_runtime, 'read_storage_maintenance_limits',
        lambda path: (history, dict(headroom_bytes=dict(ssd=1,hdd=1)), None))
    monkeypatch.setattr(sys, 'argv', ['probe', 'private-config', '["ssd","hdd"]', 'require-incremental'])
    with pytest.raises(ValueError, match='incremental_configuration_required'):
        exec(preserving._RUNTIME_MAINTENANCE_PROBE, {})


@pytest.mark.parametrize('change', [{}, {'database_image': 'mutable:latest'},
    {'previous_continuation_sha256': 'unknown'}, {'duration_seconds': 601}])
def test_archiver_request_requires_exact_prior_attempt_and_immutable_image(request_file, change):
    path, value = request_file
    value.update(schema_version='qt.storage_repository_continuation.v2',
        previous_continuation_sha256='c'*64, database_image='sha256:'+'d'*64)
    value.update(change)
    host.save_receipt(path, value, initial=False)
    if change:
        with pytest.raises(ValueError, match='request_invalid'):
            recovery._continuation_request(path)
    else:
        assert recovery._continuation_request(path) == value


@pytest.mark.parametrize('fault', [None, 'binaries', 'configuration', 'tool'])
def test_replacement_requires_same_database_binaries_and_image_configuration(monkeypatch, fault):
    old, new = 'sha256:'+'a'*64, 'sha256:'+'b'*64
    binaries = 'postgres (PostgreSQL) 15.6\na postgres\nb extension.so\nc extension.control\n'
    calls = []
    def docker(*args, **kwargs):
        calls.append(args)
        if args[:2] == ('image', 'inspect'):
            return json.dumps(dict(Env=['same'], Labels={'maintainer': 'same',
                **({'com.docker.compose.project': 'old'} if args[-1] == old else {})},
                Cmd=['changed' if fault == 'configuration' and args[-1] == new else 'postgres']))
        if args[0] == 'exec':
            assert args[1:4] == ('--user', '70:70', 'database')
            return binaries
        assert args[0] == 'run' and '--mount' not in args and '--volume' not in args
        assert args[args.index('--network')+1] == 'none' and '--read-only' in args
        return ('wrong\n' if fault == 'tool' else 'pgBackRest 2.59.1\n') + (
            binaries+'changed\n' if fault == 'binaries' else binaries)
    monkeypatch.setattr(host, 'docker', docker)
    if fault:
        with pytest.raises(RuntimeError, match='replacement_.*changed'):
            recovery.inspect_archiver_replacement(dict(image=old, id='database'), new)
    else:
        result = recovery.inspect_archiver_replacement(dict(image=old, id='database'), new)
        assert result['original'] == old and result['candidate'] == new
        assert result['database_binaries_sha256'] == hashlib.sha256(binaries.encode()).hexdigest()


@pytest.mark.parametrize('fault', [None, 'inflight', 'incomplete', 'retirement', 'config', 'wal'])
def test_archiver_recovery_requires_completed_preparation_without_replaying_it(tmp_path, monkeypatch, fault):
    recipe = dict(services=dict(prepare=dict(command=['owned'])))
    host.save_receipt(tmp_path/repositories.RECIPE, recipe, initial=True)
    status = dict(Status='exited', ExitCode=0, Pid=0, Running=False, Paused=False,
                  Restarting=False, Dead=False, OOMKilled=False)
    saved = dict(phase='recovery_repository_preparing', repositories=dict(
        completed=list(repositories._ACTIONS), inflight=None, finished_at=None,
        report=dict(repositories_initialized=True, backup_created=False, policy_enabled=False,
                    archiver_config_sha256='a'*64), helper_id='helper', helper_contract='owned',
        helper_retirement=host.digest(status), recipe_sha256=host.digest(recipe)))
    if fault == 'inflight': saved['repositories']['inflight'] = 'wal_switch'
    if fault == 'incomplete': saved['repositories']['completed'].pop()
    if fault == 'retirement': status['Running'] = True
    calls = []
    def docker(*args):
        calls.append(args)
        if args[0] == 'inspect': return json.dumps(status)
        assert args[:4] == ('exec', '--user', '70:70', 'database')
        if args[4] == 'sha256sum':
            return ('b' if fault == 'config' else 'a')*64+'  /run/quanttrad/recovery/pgbackrest.conf\n'
        assert args[4:6] == ('sh', '-ec') and args[6].startswith('test ! -e')
        return ''
    monkeypatch.setattr(host, 'docker', docker)
    monkeypatch.setattr(host, 'database_details', lambda *a: dict(config=dict(Cmd=['owned'])))
    monkeypatch.setattr(host, 'database_contract', lambda *a: 'contract')
    monkeypatch.setattr(repositories, '_preparer_contract', lambda *a: 'owned')
    def query(identity, sql):
        assert sql.startswith('SELECT ')
        return json.dumps(dict(mode='on', command=repositories._ARCHIVE_COMMAND, library='',
                               archived_count=1 if fault == 'wal' else 0, pending_restart=False))
    monkeypatch.setattr(host, 'maintenance_query', query)
    if fault:
        with pytest.raises(RuntimeError, match='storage_archiver_recovery_'):
            recovery._prepared_repository_failure(tmp_path, saved, dict(id='database'))
    else:
        assert recovery._prepared_repository_failure(tmp_path, saved, dict(id='database'))['helper_id'] == 'helper'


@pytest.mark.parametrize('fault', [None, 'hash', 'live', 'clock', 'second_attempt'])
def test_archiver_correction_binds_expired_first_attempt(tmp_path, monkeypatch, fault):
    prior = dict(schema_version='qt.storage_repository_continuation.v1', phase='preparing',
        finished_at=None, deadline=time.time()-10, deadline_boot=10, deadline_monotonic=20)
    if fault == 'live': prior['deadline'] = time.time()+10
    if fault == 'second_attempt': prior['schema_version'] = 'qt.storage_repository_continuation.v2'
    path = tmp_path/recovery.CONTINUATION_STATE
    host.save_receipt(path, prior, initial=True)
    raw = path.read_bytes()
    package = dict(previous_continuation_sha256='x' if fault == 'hash' else hashlib.sha256(raw).hexdigest())
    saved = dict(deadline=prior['deadline'], deadline_boot=11 if fault == 'clock' else 10,
                 switch=dict(deadline_monotonic=20))
    monkeypatch.setattr(recovery, '_inspect_continuation', lambda *a: (saved, {}, {}, tmp_path, {}))
    if fault:
        with pytest.raises(RuntimeError, match='storage_archiver_recovery_previous_'):
            recovery._inspect_previous_archiver_attempt(tmp_path, {}, package)
    else:
        assert recovery._inspect_previous_archiver_attempt(tmp_path, {}, package)[-1] == raw
    assert path.read_bytes() == raw


@pytest.fixture
def archiver_replacement(tmp_path, monkeypatch):
    from copy import deepcopy
    from scripts.automation import storage_online_final as final, storage_online_runtime as runtime
    old, new, worker_id = 'a'*64, 'b'*64, 'c'*64
    old_image, new_image = 'sha256:'+'d'*64, 'sha256:'+'e'*64
    model = dict(services=dict(tsdb=dict(image=old_image)))
    host.save_receipt(tmp_path/recovery.RECIPE, model, initial=True)
    host.save_receipt(tmp_path/runtime.RUNTIME_RECIPE, model, initial=True)
    saved = dict(binding=dict(project='fixture'), phase='recovery_repository_preparing',
        recovery=dict(replacement_id=old, recipe_sha256=host.digest(model)),
        repositories=dict(completed=list(repositories._ACTIONS), finished_at=None))
    host.save_receipt(tmp_path/final.STATE, saved, initial=True)
    audit = dict(request=dict(database_image=new_image), deadline_monotonic=time.monotonic()+120)
    host.save_receipt(tmp_path/recovery.CONTINUATION_STATE, audit, initial=True)
    rows = {name: dict(id=str(i+1)*64, running=False) for i,name in enumerate(host.STOP)}
    rows['tsdb'] = dict(id=old, running=True, status='running', pid=10, exit_code=0)
    state = dict(actions=[], fault=None)
    mounts = [dict(Destination='/var/lib/postgresql/data', Type='volume', Name='owned-pgdata')]
    def details(identity):
        return dict(image=old_image if identity == old else new_image,
            mounts=mounts if state['fault'] != 'mounts' or identity == old else [],
            contract='changed' if state['fault'] == 'contract' and identity == new else 'same')
    monkeypatch.setattr(host, 'database_details', details)
    monkeypatch.setattr(host, 'database_contract', lambda value: value['contract'])
    monkeypatch.setattr(host, 'database_networks', lambda *a: {})
    monkeypatch.setattr(host, 'same_database_networks', lambda *a: True)
    monkeypatch.setattr(recovery.initial, '_admit_clients', lambda *a: None)
    monkeypatch.setattr(final, '_admit_mount_writers', lambda *a, **kw: None)
    def inventory(*args, **kwargs):
        journal = audit.get('database_replacement', {})
        if journal.get('inflight') == 'remove':
            assert kwargs['removing_database_id'] == old
        return deepcopy(rows)
    monkeypatch.setattr(host, 'inventory', inventory)
    def check():
        assert host.load_receipt(tmp_path/recovery.CONTINUATION_STATE) == audit
    def dispatch(args, *, deadline, check):
        check()
        journal = host.load_receipt(tmp_path/recovery.CONTINUATION_STATE)['database_replacement']
        action = journal['inflight']
        assert action == recovery._ACTIONS[len(state['actions'])]
        assert '-v' not in args and '--volumes' not in args
        state['actions'].append(action)
        if action == 'stop': rows['tsdb'].update(running=False, status='exited', pid=0)
        elif action == 'remove': rows.pop('tsdb')
        elif action == 'create': rows['tsdb'] = dict(id=new, running=False, status='created', pid=0, exit_code=0)
        else: rows['tsdb'].update(running=True, status='running', pid=12)
        if state['fault'] == action: raise EOFError('lost daemon response')
        check()
    monkeypatch.setattr(host, 'supervised_source_action', dispatch)
    monkeypatch.setattr(host, 'cluster_identifier', lambda *a, **kw: 'wrong' if state['fault'] == 'cluster' else 'original')
    monkeypatch.setattr(recovery, 'inspect_database_archiver', lambda *a: {'pgbackrest_version':'pgBackRest 2.59.1'})
    def query(identity, sql):
        assert identity == new and sql.startswith('SELECT ')
        return json.dumps(dict(mode='on', command='wrong' if state['fault'] == 'wal' else repositories._ARCHIVE_COMMAND,
                              archived_count=1))
    monkeypatch.setattr(host, 'maintenance_query', query)
    def run():
        return recovery._replace_archiver_database(tmp_path, saved=saved,
            worker=dict(container_id=worker_id), preparation=dict(clients={}, cluster='original'), audit=audit, check=check)
    return tmp_path, state, saved, audit, run


def test_archiver_replacement_preserves_preparation_and_updates_only_image_binding(archiver_replacement):
    from scripts.automation import storage_online_final as final, storage_online_runtime as runtime
    root, state, saved, audit, run = archiver_replacement
    run()
    assert state['actions'] == list(recovery._ACTIONS)
    assert audit['database_replacement']['completed'] == [*recovery._ACTIONS, 'publish']
    assert saved['repositories']['completed'] == list(repositories._ACTIONS)
    assert saved['phase'] == 'recovery_wal_ready'
    assert host.load_receipt(root/final.STATE) == saved
    assert host.load_receipt(root/recovery.RECIPE) == host.load_receipt(root/runtime.RUNTIME_RECIPE)
    assert saved['recovery']['replacement_id'] == 'b'*64


@pytest.mark.parametrize('fault', ['stop', 'remove', 'create', 'start', 'contract', 'mounts', 'cluster', 'wal'])
def test_archiver_replacement_uncertainty_never_publishes_success(archiver_replacement, fault):
    from scripts.automation import storage_online_final as final, storage_online_runtime as runtime
    root, state, saved, audit, run = archiver_replacement
    state['fault'] = fault
    original = {name: (root/name).read_bytes() for name in (final.STATE, recovery.RECIPE, runtime.RUNTIME_RECIPE)}
    with pytest.raises((EOFError, RuntimeError)):
        run()
    assert {name: (root/name).read_bytes() for name in original} == original
    assert 'publish' not in audit['database_replacement']['completed']
    assert audit['database_replacement']['finished_at'] is None
    if fault in recovery._ACTIONS:
        assert audit['database_replacement']['inflight'] == fault


def test_archiver_publication_failure_retains_uncertain_intent(archiver_replacement, monkeypatch):
    from scripts.automation import storage_online_final as final, storage_online_runtime as runtime
    root, state, saved, audit, run = archiver_replacement
    original_final = (root/final.STATE).read_bytes()
    persist = host.save_receipt
    def fail_runtime_recipe(path, value, **kwargs):
        if path == root/runtime.RUNTIME_RECIPE:
            raise OSError('interrupted recipe publication')
        return persist(path, value, **kwargs)
    monkeypatch.setattr(host, 'save_receipt', fail_runtime_recipe)
    with pytest.raises(OSError, match='interrupted recipe'):
        run()
    assert (root/final.STATE).read_bytes() == original_final
    assert host.load_receipt(root/recovery.CONTINUATION_STATE)['database_replacement']['inflight'] == 'publish'
    assert audit['database_replacement']['finished_at'] is None
