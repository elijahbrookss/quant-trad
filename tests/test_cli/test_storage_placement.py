import json
import signal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cli import main as cli
from cli import storage_placement as operator
from core.storage_targets import StoragePolicy


def request():
    return {"schema_version": operator.placement.OPERATION, "source_revision": "a"*40,
        "request_id": "raw-test", "handoff_sha256": "b"*64,
        "policy": StoragePolicy(("ssd",), ("hdd",), ("hdd",), ("hdd",)).to_dict(),
        "resource_limits": {"wal_bytes": 1, "temporary_bytes": {"ssd": 0, "hdd": 0},
            "maintenance_bytes": {"ssd": 0, "hdd": 0}, "growth_bytes_per_second": {"ssd": 0, "hdd": 0},
            "movement_timeout_seconds": 60, "cancellation_grace_seconds": 1}}


@pytest.mark.parametrize("contents", [b"x"*65537, b'{"a":1,"a":2}', b"[]",
    json.dumps({**request(), "extra": True}).encode(),
    json.dumps({**request(), "source_revision": "develop"}).encode()],
    ids=["oversized", "duplicate", "array", "extra_field", "mutable_revision"])
def test_invalid_request_cannot_reach_runtime(tmp_path, monkeypatch, contents):
    path = tmp_path/"request.json"
    path.write_bytes(contents)
    monkeypatch.setattr(operator, "_runtime", lambda *_: pytest.fail("invalid request reached runtime"))
    with pytest.raises(ValueError):
        operator.run_local_placement(path, execute=True)


def test_api_or_local_user_cannot_execute_placement(monkeypatch):
    import core.settings
    monkeypatch.setattr(operator.os, "geteuid", lambda: 1000, raising=False)
    monkeypatch.setattr(core.settings, "get_settings", lambda: SimpleNamespace(storage=SimpleNamespace(
        maintenance_owner="dedicated", archive_shared_group_id=70)))
    with pytest.raises(RuntimeError, match="existing_maintenance_runtime_required"):
        operator._runtime(request())


def test_changed_image_refuses_before_mount_or_database_access(monkeypatch):
    import core.settings
    import core.storage_mounts
    from portal.backend.service import provenance
    monkeypatch.setattr(operator.os, "geteuid", lambda: 70, raising=False)
    monkeypatch.setattr(core.settings, "get_settings", lambda: SimpleNamespace(storage=SimpleNamespace(
        maintenance_owner="dedicated", archive_shared_group_id=70)))
    monkeypatch.setattr(provenance, "_verified_image_source_revision", lambda *a, **kw: "c"*40)
    monkeypatch.setattr(core.storage_mounts, "require_configured_archive_mount",
        lambda: pytest.fail("changed image reached the mount check"))
    with pytest.raises(RuntimeError, match="operator_revision_changed"):
        operator._runtime(request())


@pytest.mark.parametrize("action", ["inspect", "execute", "cancel"])
def test_cli_defaults_to_inspection_and_never_routes_through_api(tmp_path, monkeypatch, capsys, action):
    path = tmp_path/"request.json"
    value = request()
    path.write_text(json.dumps(value))
    monkeypatch.setenv("PG_DSN", "postgresql+psycopg2://unused:unused@invalid/unused")
    monkeypatch.setattr(operator, "_runtime", lambda _: {"worker_id": "maintenance-owned"})
    monkeypatch.setattr(cli, "_client", lambda _: pytest.fail("physical operation reached API"))
    import sqlalchemy
    engine = Mock()
    create = Mock(return_value=engine)
    monkeypatch.setattr(sqlalchemy, "create_engine", create)
    operations = {}
    for name in ("inspect_retained_raw_history", "move_retained_raw_history", "cancel_retained_raw_history"):
        operations[name] = Mock(return_value={"state": "observed", "recovery_verified": False})
        monkeypatch.setattr(operator.placement, name, operations[name])
    signals = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    args = ["--no-audit-log", "storage", "place-retained-raw", "--request-file", str(path)]
    if action != "inspect":
        args += ["--"+action]
    assert cli.main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["recovery_verified"] is False and result["service_changes_performed"] is False
    selected = {"inspect": "inspect_retained_raw_history", "execute": "move_retained_raw_history",
                "cancel": "cancel_retained_raw_history"}[action]
    for name, call in operations.items():
        assert call.call_count == int(name == selected)
    actual = operations[selected].call_args.kwargs
    assert actual["request_id"] == value["request_id"] and actual["handoff_sha256"] == value["handoff_sha256"]
    if action == "execute":
        assert actual["resource_limits"] == value["resource_limits"] and not actual["cancelled"]()
    assert {sig: signal.getsignal(sig) for sig in signals} == signals
    engine.dispose.assert_called_once_with()


def test_driver_error_is_redacted_and_not_retried(tmp_path, monkeypatch):
    path = tmp_path/"request.json"
    path.write_text(json.dumps(request()))
    monkeypatch.setenv("PG_DSN", "postgresql+psycopg2://unused:unused@invalid/unused")
    monkeypatch.setattr(operator, "_runtime", lambda _: {"worker_id": "maintenance-owned"})
    import sqlalchemy
    engine = Mock()
    monkeypatch.setattr(sqlalchemy, "create_engine", lambda *a, **kw: engine)
    move = Mock(side_effect=RuntimeError("driver failed password=private payload=private"))
    monkeypatch.setattr(operator.placement, "move_retained_raw_history", move)
    with pytest.raises(RuntimeError, match="inspect_same_request_before_retry") as error:
        operator.run_local_placement(path, execute=True)
    assert "private" not in str(error.value) and move.call_count == 1
    engine.dispose.assert_called_once_with()


def test_cli_rejects_combined_actions():
    with pytest.raises(SystemExit) as error:
        cli.build_parser().parse_args(["storage", "place-retained-raw", "--request-file", "-", "--execute", "--cancel"])
    assert error.value.code == 2


@pytest.mark.parametrize("fails", [False, True])
def test_signal_requests_cooperative_cancel_and_restores_handlers(fails):
    from psycopg2 import extensions
    from psycopg2.extras import wait_select

    before = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    previous_wait = extensions.get_wait_callback()
    sentinel = Mock()
    extensions.set_wait_callback(sentinel)
    try:
        try:
            with operator._cancellation() as cancelled:
                assert extensions.get_wait_callback() is wait_select
                assert not cancelled()
                signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                assert cancelled()
                if fails:
                    raise RuntimeError("fixture move failed")
        except RuntimeError as exc:
            assert fails and str(exc) == "fixture move failed"
        assert extensions.get_wait_callback() is sentinel
    finally:
        extensions.set_wait_callback(previous_wait)
    assert {sig: signal.getsignal(sig) for sig in before} == before
