from types import SimpleNamespace
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.db.inspect_fact_header_cutover_v2 import inspect_preflight, main


@pytest.mark.parametrize("timeout", [0, 61, True, 1.5, "15"])
def test_preflight_rejects_unbounded_timeout_before_connecting(timeout):
    engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"),
                             connect=lambda: pytest.fail("must reject before connection"))
    with pytest.raises(ValueError, match="timeout_out_of_range"):
        inspect_preflight(engine, statement_timeout_seconds=timeout)


def test_preflight_rejects_non_postgres_before_connecting():
    engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"),
                             connect=lambda: pytest.fail("must not create another database"))
    with pytest.raises(ValueError, match="requires_postgresql"):
        inspect_preflight(engine)


def test_preflight_does_not_load_dotenv_or_create_sqlite(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PG_DSN", raising=False)
    target = tmp_path / "not-created.db"
    (tmp_path / ".env").write_text("PG_DSN=sqlite:///" + str(target))
    with pytest.raises(SystemExit) as error:
        main([])
    assert error.value.code == 2
    assert not target.exists()
    monkeypatch.setenv("PG_DSN", "sqlite:///" + str(target))
    with pytest.raises(SystemExit) as error:
        main([])
    assert error.value.code == 2
    assert not target.exists()


def test_preflight_exposes_no_execute_option():
    with pytest.raises(SystemExit) as error:
        main(["--execute"])
    assert error.value.code == 2


def test_preflight_import_has_no_application_settings_side_effects():
    root = Path(__file__).resolve().parents[2]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(root)
    result = subprocess.run([
        sys.executable, "-c",
        "import sys; import scripts.db.inspect_fact_header_cutover_v2; "
        "assert 'core.settings' not in sys.modules; "
        "assert 'portal.backend.db' not in sys.modules",
    ], cwd=root, env=environment, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
