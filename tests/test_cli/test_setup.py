from __future__ import annotations

import json
import urllib.parse
import urllib.request

from cli import setup as qt_setup
from cli.main import main


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_ensure_operator_env_generates_local_values(tmp_path):
    (tmp_path / "secrets.env.example").write_text(
        "\n".join(
            [
                "POSTGRES_DB=quanttrad",
                "POSTGRES_USER=quanttrad",
                "POSTGRES_PASSWORD=replace-with-a-local-db-password",
                "PG_DSN=postgresql+psycopg2://quanttrad:replace-with-a-local-db-password@localhost:15432/quanttrad",
                "PGADMIN_DEFAULT_PASSWORD=replace-with-a-local-pgadmin-password",
                "GF_SECURITY_ADMIN_PASSWORD=replace-with-a-local-grafana-password",
                "QT_SECURITY_PROVIDER_CREDENTIAL_KEY=replace-with-fernet-key",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = qt_setup.ensure_operator_env(tmp_path)
    values = qt_setup._env_values(tmp_path / "secrets.env")

    assert result["created"] is True
    assert result["credential_key_valid"] is True
    assert "replace-with" not in values["POSTGRES_PASSWORD"]
    assert "replace-with" not in values["PG_DSN"]
    assert qt_setup.validate_fernet_key(values["QT_SECURITY_PROVIDER_CREDENTIAL_KEY"])[0] is True


def test_ensure_operator_env_does_not_rotate_invalid_existing_key(tmp_path):
    (tmp_path / "secrets.env").write_text(
        "\n".join(
            [
                "POSTGRES_DB=quanttrad",
                "POSTGRES_USER=quanttrad",
                "POSTGRES_PASSWORD=local-password",
                "PG_DSN=postgresql+psycopg2://quanttrad:local-password@localhost:15432/quanttrad",
                "QT_SECURITY_PROVIDER_CREDENTIAL_KEY=not-a-fernet-key",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = qt_setup.ensure_operator_env(tmp_path)
    values = qt_setup._env_values(tmp_path / "secrets.env")

    assert result["credential_key_valid"] is False
    assert values["QT_SECURITY_PROVIDER_CREDENTIAL_KEY"] == "not-a-fernet-key"


def test_ensure_operator_env_url_encodes_generated_pg_dsn(tmp_path):
    (tmp_path / "secrets.env").write_text(
        "\n".join(
            [
                "POSTGRES_DB=quant/trad",
                "POSTGRES_USER=quant@user",
                "POSTGRES_PASSWORD=p@ss:word/with#hash",
                "PG_DSN=postgresql+psycopg2://quanttrad:replace-with-password@localhost:15432/quanttrad",
                f"QT_SECURITY_PROVIDER_CREDENTIAL_KEY={qt_setup.generate_fernet_key()}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    qt_setup.ensure_operator_env(tmp_path)
    values = qt_setup._env_values(tmp_path / "secrets.env")

    assert values["PG_DSN"] == (
        "postgresql+psycopg2://"
        "quant%40user:p%40ss%3Aword%2Fwith%23hash@localhost:15432/quant%2Ftrad"
    )


def test_ensure_operator_env_repairs_stale_canonical_local_pg_dsn(tmp_path) -> None:
    (tmp_path / "secrets.env").write_text(
        "\n".join(
            [
                "POSTGRES_DB=quanttrad",
                "POSTGRES_USER=quanttrad",
                "POSTGRES_PASSWORD=current",
                "PG_DSN=postgresql+psycopg2://quanttrad:stale@localhost:15432/quanttrad",
                f"QT_SECURITY_PROVIDER_CREDENTIAL_KEY={qt_setup.generate_fernet_key()}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = qt_setup.ensure_operator_env(tmp_path)
    values = qt_setup._env_values(tmp_path / "secrets.env")

    assert "PG_DSN" in result["changed"]
    assert values["PG_DSN"] == (
        "postgresql+psycopg2://quanttrad:current@localhost:15432/quanttrad"
    )


def test_ensure_operator_env_preserves_nonlocal_pg_dsn(tmp_path) -> None:
    remote_dsn = "postgresql+psycopg2://remote:user@db.example.test:5432/market"
    (tmp_path / "secrets.env").write_text(
        "\n".join(
            [
                "POSTGRES_DB=quanttrad",
                "POSTGRES_USER=quanttrad",
                "POSTGRES_PASSWORD=current",
                f"PG_DSN={remote_dsn}",
                f"QT_SECURITY_PROVIDER_CREDENTIAL_KEY={qt_setup.generate_fernet_key()}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = qt_setup.ensure_operator_env(tmp_path)
    values = qt_setup._env_values(tmp_path / "secrets.env")

    assert "PG_DSN" not in result["changed"]
    assert values["PG_DSN"] == remote_dsn


def test_ensure_operator_env_preserves_noncanonical_local_pg_dsn(tmp_path) -> None:
    local_dsn = "postgresql+psycopg2://quanttrad:stale@localhost:5432/quanttrad"
    (tmp_path / "secrets.env").write_text(
        "\n".join(
            [
                "POSTGRES_DB=quanttrad",
                "POSTGRES_USER=quanttrad",
                "POSTGRES_PASSWORD=current",
                f"PG_DSN={local_dsn}",
                f"QT_SECURITY_PROVIDER_CREDENTIAL_KEY={qt_setup.generate_fernet_key()}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = qt_setup.ensure_operator_env(tmp_path)
    values = qt_setup._env_values(tmp_path / "secrets.env")

    assert "PG_DSN" not in result["changed"]
    assert values["PG_DSN"] == local_dsn


def test_pg_dsn_check_rejects_stale_local_credentials_without_exposing_values(
    monkeypatch,
) -> None:
    monkeypatch.delenv("PG_DSN", raising=False)
    check = qt_setup._pg_dsn_check(
        {
            "PG_DSN": "postgresql+psycopg2://quanttrad:stale@localhost:15432/quanttrad",
            "POSTGRES_USER": "quanttrad",
            "POSTGRES_PASSWORD": "current",
            "POSTGRES_DB": "quanttrad",
        }
    )

    assert check.status == "failed"
    assert "stale" not in check.detail
    assert "current" not in check.detail
    assert check.remediation == "Run `qt setup env` to align the single local PG_DSN."


def test_pg_dsn_check_accepts_url_encoded_matching_local_credentials(monkeypatch) -> None:
    monkeypatch.delenv("PG_DSN", raising=False)
    check = qt_setup._pg_dsn_check(
        {
            "PG_DSN": (
                "postgresql+psycopg2://quant%40user:p%40ss%3Aword%2Fwith%23hash"
                "@localhost:15432/quant%2Ftrad"
            ),
            "POSTGRES_USER": "quant@user",
            "POSTGRES_PASSWORD": "p@ss:word/with#hash",
            "POSTGRES_DB": "quant/trad",
        }
    )

    assert check.status == "ok"


def test_setup_provider_coinbase_uses_canonical_credentials_api(monkeypatch, capsys):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_urlopen(request, timeout):
        _ = timeout
        path = urllib.parse.urlparse(request.full_url).path
        body = json.loads(request.data.decode("utf-8")) if request.data else None
        calls.append((request.get_method(), path, body))
        if path == "/api/providers/credentials/schema":
            payload = {
                "provider_id": "COINBASE",
                "venue_id": "COINBASE_DIRECT",
                "environment": "paper",
                "default_credential_ref": "coinbase-coinbase_direct-paper",
                "required": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
                "optional": [],
                "accepted": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
                "secrets_are_returned": False,
            }
        elif path == "/api/providers/credentials":
            assert body == {
                "provider_id": "COINBASE",
                "venue_id": "COINBASE_DIRECT",
                "credential_ref": "coinbase-coinbase_direct-paper",
                "environment": "paper",
                "display_name": None,
                "credentials": {
                    "COINBASE_API_KEY": "key",
                    "COINBASE_API_SECRET": "secret",
                },
            }
            payload = {
                "credential": {
                    "credential_ref": "coinbase-coinbase_direct-paper",
                    "provider_id": "COINBASE",
                    "venue_id": "COINBASE_DIRECT",
                    "environment": "paper",
                    "status": "active",
                },
                "secrets_are_returned": False,
            }
        elif path == "/api/providers/credentials/coinbase-coinbase_direct-paper/validate":
            payload = {
                "credential": {
                    "credential_ref": "coinbase-coinbase_direct-paper",
                    "status": "active",
                },
                "secrets_are_returned": False,
            }
        else:
            raise AssertionError(f"unexpected path: {path}")
        return _Response(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    exit_code = main(
        [
            "--no-audit-log",
            "setup",
            "provider",
            "coinbase",
            "--secrets-json",
            '{"COINBASE_API_KEY":"key","COINBASE_API_SECRET":"secret"}',
            "--no-input",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["schema_version"] == "qt_setup_provider.v1"
    assert payload["status"] == "ok"
    assert payload["credential_ref"] == "coinbase-coinbase_direct-paper"
    assert payload["secrets_are_returned"] is False
    assert [call[:2] for call in calls] == [
        ("GET", "/api/providers/credentials/schema"),
        ("POST", "/api/providers/credentials"),
        ("POST", "/api/providers/credentials/coinbase-coinbase_direct-paper/validate"),
    ]
