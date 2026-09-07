from __future__ import annotations

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
