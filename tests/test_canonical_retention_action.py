from datetime import date
from types import SimpleNamespace

import pytest

from scripts.automation import canonical_retention_action as cli


def test_cli_passes_explicit_verification_count_to_exact_action(monkeypatch, tmp_path):
    lifecycle = object()
    settings = SimpleNamespace(
        storage=SimpleNamespace(maintenance_owner="dedicated", archive_shared_group_id=70,
                                maintenance_limits_path="/run/test-limits"),
        market_data_lifecycle=lifecycle,
        workers=SimpleNamespace(collectors=SimpleNamespace(db_wait_timeout_seconds=10)),
    )
    observed = []
    service = SimpleNamespace(run_canonical_action=lambda **kwargs: observed.append(kwargs) or {"status": "completed"})
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli.os, "geteuid", lambda: 70, raising=False)
    monkeypatch.setattr(cli, "require_configured_archive_mount", lambda: None)
    monkeypatch.setattr(cli, "require_configured_working_mount", lambda: None)
    monkeypatch.setattr(cli, "FilesystemRawArchiveObjectStore", lambda _path: None)
    monkeypatch.setattr(cli, "storage_maintenance_runners", lambda *_args, **_kwargs: {"service": service})
    monkeypatch.setattr(cli, "wait_for_database_ready", lambda **_kwargs: True)
    receipt = tmp_path / "receipt.json"
    assert cli.main(["--storage-day", "2026-09-24", "--expected-action", "verify_partition",
                     "--verification-pages", "1043", "--execute", "--receipt", str(receipt)]) == 0
    assert observed == [{"policy": lifecycle, "storage_day": date(2026, 9, 24),
                         "expected_action": "verify_partition", "storage_root": cli.DEFAULT_STORAGE_ROOT,
                         "execute": True, "verification_pages": 1043}]
    assert receipt.is_file()


@pytest.mark.parametrize("action,pages", [("stage_page", "1043"), ("reclaim_partition", "1043"),
                                          ("verify_partition", "0"), ("verify_partition", "100001")])
def test_cli_rejects_out_of_scope_count_before_runtime_admission(monkeypatch, tmp_path, action, pages):
    monkeypatch.setattr(cli, "get_settings", lambda: pytest.fail("unexpected runtime admission"))
    with pytest.raises(SystemExit) as error:
        cli.main(["--storage-day", "2026-09-24", "--expected-action", action,
                  "--verification-pages", pages, "--receipt", str(tmp_path / "unused")])
    assert error.value.code == 2


def test_cli_requires_complete_exact_page_pin_before_runtime_admission(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "get_settings", lambda: pytest.fail("unexpected runtime admission"))
    common = ["--storage-day", "2026-09-24", "--receipt", str(tmp_path / "unused")]
    page = ["--page-ordinal", "631", "--manifest-id", "cfa_" + "a" * 64,
            "--manifest-hash", "b" * 64, "--generation", "1"]
    for args in (["--expected-action", "verify_page", *page], ["--expected-action", "stage_page", *page]):
        with pytest.raises(SystemExit) as error:
            cli.main(common + args)
        assert error.value.code == 2
