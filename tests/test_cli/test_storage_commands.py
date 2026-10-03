import json
from unittest.mock import Mock

from cli.api import ApiError
import cli.main as cli


def test_storage_review_preserves_revision_and_request_identity(tmp_path, monkeypatch, capsys):
    policy_path = tmp_path / "policy.json"
    policy = {"recent": ["ssd"], "history": ["hdd"], "archives": ["hdd"], "backups": ["hdd"]}
    policy_path.write_text(json.dumps(policy))
    client = Mock()
    client.request_json.return_value = {"id": "plan-1", "state": "planned"}
    monkeypatch.setattr(cli, "_client", lambda args: client)
    result = cli.main(["--no-audit-log", "storage", "review", "--policy-file", str(policy_path),
                       "--base-revision", "7", "--request-id", "request-1"])
    assert result == 0
    client.request_json.assert_called_once_with("POST", "/api/storage/plans", payload={
        "policy": policy, "base_revision": 7, "request_id": "request-1",
    })
    assert json.loads(capsys.readouterr().out)["state"] == "planned"


def test_storage_apply_returns_error_when_execution_is_blocked(monkeypatch, capsys):
    client = Mock()
    client.request_json.side_effect = ApiError("storage_execution_unavailable", status=409)
    monkeypatch.setattr(cli, "_client", lambda args: client)
    assert cli.main(["--no-audit-log", "storage", "apply", "plan-1", "--policy-hash", "hash"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == 409
    client.request_json.assert_called_once_with("POST", "/api/storage/plans/plan-1/apply",
                                               payload={"policy_hash": "hash"})
