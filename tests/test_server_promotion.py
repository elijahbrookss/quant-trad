from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/automation/server_deploy.sh"


def shell(tmp_path, body, *, env=None):
    # Exercise actual shell functions. Substitute only external boundaries.
    code = SCRIPT.read_text().split('action="${1:-}"')[0]
    script = tmp_path / "probe.sh"
    script.write_text(code + "\n" + body)
    return subprocess.run(["bash", str(script)], env={**os.environ, "QT_SINGLE_NODE_STATE_ROOT": str(tmp_path / "state"), **(env or {})}, capture_output=True, text=True, timeout=20)


@pytest.mark.parametrize("failure", ["compose", "initializer", "attestation", "fleet"])
def test_failed_gate_never_records_a_success(tmp_path, failure):
    result = shell(tmp_path, r'''
require_runtime() { :; }
select_release() { QT_RELEASE_REVISION=fixture; QT_SOURCE_TREE_HASH=fixture; }
build_release_images() { :; }
validate_storage_root() { :; }
compose() {
  if [[ "$1" = up && "$FAILURE" = compose ]]; then return 19; fi
  if [[ "$1" = exec && "$FAILURE" = fleet ]]; then return 20; fi
}
verify_initializer() { test "$FAILURE" != initializer; }
verify_release_image() { test "$FAILURE" != attestation; }
record_release() { echo INCORRECT_SUCCESS; }
(deploy_release fixture) &
child=$!
if wait "$child"; then exit 90; else echo FAILURE_PRESERVED; fi
''', env={"FAILURE": failure})
    assert result.returncode == 0, result.stderr
    assert "FAILURE_PRESERVED" in result.stdout
    assert "INCORRECT_SUCCESS" not in result.stdout


def test_lock_refuses_overlapping_operations(tmp_path):
    import fcntl
    state = tmp_path / "state"
    state.mkdir()
    with (state / "deployment.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = shell(tmp_path, "acquire_deployment_lock")
    assert result.returncode != 0
    assert "another server operation" in result.stderr


def test_unresolved_promotion_blocks_direct_deploy(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "promotion.env").write_text("previous_revision=fixture\n")
    result = shell(tmp_path, "deploy_release fixture")
    assert result.returncode != 0
    assert "unfinished promotion" in result.stderr


def test_recovery_failure_keeps_evidence_and_does_not_report_success(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "promotion.env").write_text(f"previous_revision={'a' * 40}\ncandidate_revision={'b' * 40}\nactivation_started=true\n")
    (state / "recovery.compose.json").write_text("{}")
    result = shell(tmp_path, "select_release() { :; }; deploy_release() { false; echo INCORRECT_SUCCESS; }; recover_promotion")
    assert result.returncode != 0
    assert "recovery failed" in result.stderr
    assert "INCORRECT_SUCCESS" not in result.stdout
    assert (state / "promotion.env").exists()


def test_pre_activation_failure_restores_checkout_without_recreating_services(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "promotion.env").write_text(f"previous_revision={'a' * 40}\ncandidate_revision={'b' * 40}\nactivation_started=false\n")
    (state / "recovery.compose.json").write_text("{}")
    result = shell(tmp_path, "select_release() { echo CHECKOUT_RESTORED; }; deploy_release() { echo INCORRECT_RESTART; exit 1; }; recover_promotion")
    assert result.returncode == 0, result.stderr
    assert "CHECKOUT_RESTORED" in result.stdout
    assert "INCORRECT_RESTART" not in result.stdout
    assert not (state / "promotion.env").exists()


def test_release_reports_unfinished_promotion_separately_from_last_success(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "release.env").write_text(f"current_revision={'a' * 40}\n")
    (state / "promotion.env").write_text(f"previous_revision={'a' * 40}\ncandidate_revision={'b' * 40}\nactivation_started=true\n")
    result = shell(tmp_path, "show_release")
    assert result.returncode == 0, result.stderr
    assert "current revision: " + "a" * 40 in result.stdout
    assert "unfinished promotion candidate: " + "b" * 40 in result.stdout
    assert "activation started: true" in result.stdout


@pytest.mark.parametrize("entry", ["deploy_release fixture", "recover_promotion", "promote_release fixture"])
@pytest.mark.parametrize("hold", ["valid", "corrupt", "symlink"])
def test_storage_hold_blocks_deploy_and_old_image_recovery_before_side_effects(tmp_path, entry, hold):
    state = tmp_path / "state"
    state.mkdir()
    marker = state / "storage-handoff.json"
    if hold == "symlink":
        marker.symlink_to(state / "missing")
    else:
        marker.write_text('{}' if hold == "valid" else '{')
    result = shell(tmp_path, "require_runtime() { echo INCORRECT_RUNTIME; }; select_release() { echo INCORRECT_CHECKOUT; }; " + entry)
    assert result.returncode != 0
    assert "storage handoff hold" in result.stderr
    assert "INCORRECT" not in result.stdout
    assert os.path.lexists(marker)


@pytest.mark.parametrize("action", ["init-env", "deploy", "rollback", "promote", "recover", "apply-alerts", "preview-alerts", "restore-alerts", "stop", "qt", "credentials-coinbase"])
def test_every_mutating_action_refuses_storage_hold_at_dispatch(tmp_path, action):
    state = tmp_path / "state"
    state.mkdir()
    (state / "storage-handoff.json").write_text("{")
    result = subprocess.run(["bash", str(SCRIPT), action], capture_output=True, text=True, timeout=10,
                            env={**os.environ, "QT_SINGLE_NODE_STATE_ROOT": str(state),
                                 "QT_SINGLE_NODE_ENV_FILE": str(tmp_path / "nonexistent-synthetic.env")})
    assert result.returncode != 0
    assert "storage handoff hold" in result.stderr


def test_release_reports_hold_even_without_successful_release(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "storage-handoff.json").write_text("{")
    result = shell(tmp_path, "show_release")
    assert result.returncode == 0
    assert "Storage handoff hold: active" in result.stdout
    assert "No successful release" in result.stdout


def fixed_state(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "release.env").write_text(
        f"current_revision={'a' * 40}\nstorage_layout=ssd-hdd-v1\n"
    )
    return state


def test_recorded_layout_survives_release_record_and_environment_override(tmp_path):
    state = fixed_state(tmp_path)
    result = shell(tmp_path, r'''
QT_RELEASE_REVISION=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
QT_SOURCE_TREE_HASH=fixture
record_release
show_release
''', env={"QT_STORAGE_LAYOUT": "", "QT_SINGLE_NODE_ENV_FILE": str(tmp_path / "unused.env")})
    assert result.returncode == 0, result.stderr
    assert "storage layout: ssd-hdd-v1" in result.stdout
    assert "storage_layout=ssd-hdd-v1" in (state / "release.env").read_text()


@pytest.mark.parametrize("invoke", ["compose config", "if compose config; then echo INCORRECT_SUCCESS; fi"])
def test_fixed_layout_refuses_missing_candidate_overlay_even_in_conditional(tmp_path, invoke):
    fixed_state(tmp_path)
    result = shell(tmp_path, 'repo_root="$FIXTURE_ROOT"; docker() { echo INCORRECT_DOCKER; }; ' + invoke,
                   env={"FIXTURE_ROOT": str(tmp_path)})
    assert "not supported by this checkout" in result.stderr
    assert "INCORRECT" not in result.stdout


@pytest.mark.parametrize("inherited_recovery", ["false", "true"])
def test_normal_compose_selects_recorded_storage_overlay(tmp_path, inherited_recovery):
    fixed_state(tmp_path)
    docker = tmp_path / "docker"
    docker.mkdir()
    overlay = docker / "docker-compose.storage-server.yml"
    overlay.write_text("services: {}\n")
    result = shell(tmp_path, 'repo_root="$FIXTURE_ROOT"; docker() { printf "%s\\n" "$@"; }; compose config',
                   env={"FIXTURE_ROOT": str(tmp_path), "recovery_config_frozen": inherited_recovery})
    assert result.returncode == 0, result.stderr
    assert str(overlay) in result.stdout.splitlines()


@pytest.mark.parametrize("fault", ["old_layout", "missing_hash", "changed_snapshot"])
def test_fixed_recovery_refuses_incompatible_or_changed_snapshot_before_checkout(tmp_path, fault):
    state = fixed_state(tmp_path)
    snapshot = state / "recovery.compose.json"
    snapshot.write_text('{"services": {}}')
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    marker = f"previous_revision={'a' * 40}\ncandidate_revision={'b' * 40}\nactivation_started=true\n"
    if fault != "old_layout":
        marker += "storage_layout=ssd-hdd-v1\n"
    if fault != "missing_hash":
        marker += f"recovery_compose_sha256={digest}\n"
    (state / "promotion.env").write_text(marker)
    if fault == "changed_snapshot":
        snapshot.write_text("{}")
    result = shell(tmp_path, "select_release() { echo INCORRECT_CHECKOUT; }; recover_promotion")
    assert result.returncode != 0
    assert "INCORRECT" not in result.stdout
    assert (state / "promotion.env").exists()
    assert "recovery" in result.stderr


def test_fixed_recovery_uses_only_pinned_snapshot_despite_current_overrides(tmp_path):
    state = fixed_state(tmp_path)
    snapshot = state / "recovery.compose.json"
    snapshot.write_text('{"services": {}}')
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    (state / "promotion.env").write_text(
        f"previous_revision={'a' * 40}\ncandidate_revision={'b' * 40}\nactivation_started=true\n"
        f"storage_layout=ssd-hdd-v1\nrecovery_compose_sha256={digest}\n"
    )
    # No storage overlay exists in this synthetic checkout. Recovery must not
    # load one or merge today's alert/storage values into the saved model.
    result = shell(tmp_path, r'''
alerts_enabled=true
select_release() { :; }
docker() { printf '%s\n' "$@"; }
deploy_release() { compose config; }
recover_promotion
''')
    assert result.returncode == 0, result.stderr
    assert str(snapshot) in result.stdout.splitlines()
    assert "docker-compose.storage-server" not in result.stdout
    assert "docker-compose.alert-email" not in result.stdout
    assert not (state / "promotion.env").exists()
