from pathlib import Path
from types import SimpleNamespace

from scripts.automation import canonical_retention_action as operator


def _settings(tmp_path):
    return SimpleNamespace(
        storage=SimpleNamespace(
            maintenance_owner="dedicated", archive_shared_group_id=70,
            maintenance_limits_path=str(tmp_path / "limits.json"),
        ),
        workers=SimpleNamespace(
            collectors=SimpleNamespace(db_wait_timeout_seconds=3),
        ),
        market_data_lifecycle=object(),
    )


def test_operator_runs_only_exact_private_canonical_action_and_writes_receipt(
        tmp_path, monkeypatch):
    calls = []
    result = {"status": "completed", "failure_count": 0, "outcomes": [
        {"status": "page_acknowledged"},
    ]}
    service = SimpleNamespace(
        run_canonical_action=lambda **kwargs: calls.append(kwargs) or result,
    )
    settings = _settings(tmp_path)
    receipts = []
    monkeypatch.setattr(operator, "get_settings", lambda: settings)
    monkeypatch.setattr(operator.os, "geteuid", lambda: 70)
    monkeypatch.setattr(operator, "require_configured_archive_mount", lambda: None)
    monkeypatch.setattr(operator, "require_configured_working_mount", lambda: None)
    monkeypatch.setattr(operator, "FilesystemRawArchiveObjectStore", lambda *_: None)
    monkeypatch.setattr(operator, "wait_for_database_ready", lambda **_: True)
    monkeypatch.setattr(
        operator, "storage_maintenance_runners",
        lambda *_, **__: {"service": service, "history_runner": object(),
                           "recovery_runner": object()},
    )
    monkeypatch.setattr(operator, "write_receipt", lambda path, value: receipts.append((path, value)))
    receipt = tmp_path / "receipt.json"

    code = operator.main([
        "--storage-day", "2026-09-03", "--expected-action", "stage_page",
        "--receipt", str(receipt), "--execute",
    ])

    assert code == 0 and receipts == [(receipt, result)]
    assert calls == [{
        "policy": settings.market_data_lifecycle,
        "storage_day": operator.date(2026, 9, 3),
        "expected_action": "stage_page",
        "storage_root": operator.DEFAULT_STORAGE_ROOT,
        "execute": True,
    }]


def test_operator_returns_nonzero_for_one_attempt_failure(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    result = {"status": "failed", "failure_count": 1, "outcomes": [
        {"status": "failed", "error": "proof refused"},
    ]}
    monkeypatch.setattr(operator, "get_settings", lambda: settings)
    monkeypatch.setattr(operator.os, "geteuid", lambda: 70)
    monkeypatch.setattr(operator, "require_configured_archive_mount", lambda: None)
    monkeypatch.setattr(operator, "require_configured_working_mount", lambda: None)
    monkeypatch.setattr(operator, "FilesystemRawArchiveObjectStore", lambda *_: None)
    monkeypatch.setattr(operator, "wait_for_database_ready", lambda **_: True)
    monkeypatch.setattr(
        operator, "storage_maintenance_runners",
        lambda *_, **__: {"service": SimpleNamespace(
            run_canonical_action=lambda **_: result,
        )},
    )
    monkeypatch.setattr(operator, "write_receipt", lambda *_: None)

    assert operator.main([
        "--storage-day", "2026-09-03", "--expected-action", "reclaim_partition",
        "--receipt", str(tmp_path / "receipt.json"), "--execute",
    ]) == 1
