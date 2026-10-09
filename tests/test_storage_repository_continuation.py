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
