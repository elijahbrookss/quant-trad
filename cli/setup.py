from __future__ import annotations

import base64
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VENV = ".venv"
REQUIRED_PYTHON = (3, 12)
SETUP_SCHEMA_VERSION = "qt_setup.v1"
PLACEHOLDER_RE = re.compile(r"^(replace-with-.*|<.+>|)$")
ENV_ASSIGNMENT_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


@dataclass(frozen=True)
class SetupCheck:
    name: str
    status: str
    detail: str
    required: bool = True
    remediation: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "required": self.required,
            "detail": self.detail,
        }
        if self.remediation:
            payload["remediation"] = self.remediation
        return payload


def required_python_label() -> str:
    return f"{REQUIRED_PYTHON[0]}.{REQUIRED_PYTHON[1]}+"


def _version_tuple(version: str) -> tuple[int, int, int] | None:
    match = re.search(r"Python\s+(\d+)\.(\d+)\.(\d+)", version)
    if not match:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def _version_label(version: tuple[int, int, int] | None) -> str:
    if not version:
        return "unknown"
    return ".".join(str(part) for part in version)


def _python_ok(version: tuple[int, int, int] | None) -> bool:
    if not version:
        return False
    return version[:2] >= REQUIRED_PYTHON


def _command_output(command: list[str], *, cwd: Path, timeout: float = 10.0) -> tuple[int, str]:
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    return int(proc.returncode), (proc.stdout or "").strip()


def _env_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = ENV_ASSIGNMENT_RE.match(stripped)
        if not match:
            continue
        values[match.group(1)] = match.group(2).strip().strip('"').strip("'")
    return values


def _placeholder(value: str | None) -> bool:
    return PLACEHOLDER_RE.match(str(value or "").strip()) is not None


def generate_fernet_key() -> str:
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


def validate_fernet_key(value: str | None) -> tuple[bool, str]:
    text = str(value or "").strip()
    if not text:
        return False, "missing"
    if _placeholder(text):
        return False, "placeholder"
    try:
        decoded = base64.urlsafe_b64decode(text.encode("ascii"))
    except Exception:
        return False, "not valid urlsafe base64"
    if len(decoded) != 32:
        return False, f"decoded key must be 32 bytes, got {len(decoded)}"
    return True, "valid"


def _safe_password() -> str:
    return secrets.token_urlsafe(24)


def _set_env_line(lines: list[str], key: str, value: str) -> bool:
    prefix = f"{key}="
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if stripped.startswith(prefix):
            new_line = f"{key}={value}"
            if line != new_line:
                lines[index] = new_line
                return True
            return False
    if lines and lines[-1].strip():
        lines.append("")
    lines.append(f"{key}={value}")
    return True


def _postgres_dsn(user: str, password: str, db: str) -> str:
    encoded_user = urllib.parse.quote(str(user), safe="")
    encoded_password = urllib.parse.quote(str(password), safe="")
    encoded_db = urllib.parse.quote(str(db), safe="")
    return f"postgresql+psycopg2://{encoded_user}:{encoded_password}@localhost:15432/{encoded_db}"


def _local_pg_dsn_consistency(
    pg_dsn: str,
    *,
    user: str,
    password: str,
    db: str,
) -> bool | None:
    if not user or not password or not db:
        return None
    try:
        parsed = urllib.parse.urlsplit(pg_dsn)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in {"postgresql", "postgresql+psycopg2"}:
        return None
    if str(parsed.hostname or "").lower() not in {"localhost", "127.0.0.1", "::1"}:
        return None
    if port != 15432:
        return None
    return (
        urllib.parse.unquote(parsed.username or ""),
        urllib.parse.unquote(parsed.password or ""),
        urllib.parse.unquote(parsed.path.lstrip("/")),
    ) == (user, password, db)


def ensure_operator_env(repo_root: str | Path = REPO_ROOT) -> dict[str, Any]:
    root = Path(repo_root)
    path = root / "secrets.env"
    template = root / "secrets.env.example"
    created = False
    changes: list[str] = []

    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    elif template.exists():
        lines = template.read_text(encoding="utf-8").splitlines()
        created = True
    else:
        lines = []
        created = True

    values = _env_values(path) if path.exists() else _env_values(template)

    db = values.get("POSTGRES_DB") if not _placeholder(values.get("POSTGRES_DB")) else "quanttrad"
    user = values.get("POSTGRES_USER") if not _placeholder(values.get("POSTGRES_USER")) else "quanttrad"
    password = values.get("POSTGRES_PASSWORD")
    if _placeholder(password):
        password = _safe_password()
        if _set_env_line(lines, "POSTGRES_PASSWORD", password):
            changes.append("POSTGRES_PASSWORD")

    if _placeholder(values.get("POSTGRES_DB")) and _set_env_line(lines, "POSTGRES_DB", db):
        changes.append("POSTGRES_DB")
    if _placeholder(values.get("POSTGRES_USER")) and _set_env_line(lines, "POSTGRES_USER", user):
        changes.append("POSTGRES_USER")

    pg_dsn = values.get("PG_DSN")
    if _placeholder(pg_dsn) or "replace-with" in str(pg_dsn or "") or "<" in str(pg_dsn or ""):
        pg_dsn = _postgres_dsn(user or "quanttrad", password or _safe_password(), db or "quanttrad")
        if _set_env_line(lines, "PG_DSN", pg_dsn):
            changes.append("PG_DSN")
    elif _local_pg_dsn_consistency(
        str(pg_dsn or ""),
        user=str(user or ""),
        password=str(password or ""),
        db=str(db or ""),
    ) is False:
        pg_dsn = _postgres_dsn(str(user), str(password), str(db))
        if _set_env_line(lines, "PG_DSN", pg_dsn):
            changes.append("PG_DSN")

    for key in ("PGADMIN_DEFAULT_PASSWORD", "GF_SECURITY_ADMIN_PASSWORD"):
        if _placeholder(values.get(key)) and _set_env_line(lines, key, _safe_password()):
            changes.append(key)

    credential_key = values.get("QT_SECURITY_PROVIDER_CREDENTIAL_KEY")
    key_valid, key_detail = validate_fernet_key(credential_key)
    if not key_valid and key_detail in {"missing", "placeholder"}:
        credential_key = generate_fernet_key()
        if _set_env_line(lines, "QT_SECURITY_PROVIDER_CREDENTIAL_KEY", credential_key):
            changes.append("QT_SECURITY_PROVIDER_CREDENTIAL_KEY")

    if created or changes:
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    final_values = _env_values(path)
    final_key_valid, final_key_detail = validate_fernet_key(final_values.get("QT_SECURITY_PROVIDER_CREDENTIAL_KEY"))
    return {
        "path": str(path),
        "created": created,
        "changed": sorted(set(changes)),
        "credential_key_valid": final_key_valid,
        "credential_key_detail": final_key_detail,
    }


def _python_version(executable: str, *, cwd: Path) -> tuple[tuple[int, int, int] | None, str]:
    code, output = _command_output([executable, "--version"], cwd=cwd)
    if code != 0:
        return None, output or f"{executable} not found"
    return _version_tuple(output), output.strip()


def find_python(candidates: list[str] | None = None, *, cwd: Path = REPO_ROOT) -> dict[str, Any]:
    checked: list[dict[str, Any]] = []
    names = candidates or [
        os.environ.get("QT_SETUP_PYTHON", ""),
        "python3.12",
        "python3",
        "python",
    ]
    for raw_name in names:
        name = str(raw_name or "").strip()
        if not name:
            continue
        path = shutil.which(name)
        if path is None and os.sep in name:
            path = name
        if path is None:
            checked.append({"executable": name, "status": "missing"})
            continue
        version, raw = _python_version(path, cwd=cwd)
        status = "ok" if _python_ok(version) else "unsupported"
        item = {
            "executable": path,
            "status": status,
            "version": _version_label(version),
            "raw": raw,
        }
        checked.append(item)
        if status == "ok":
            return {"selected": item, "checked": checked}
    return {"selected": None, "checked": checked}


def _venv_python(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _venv_qt(venv: Path) -> Path:
    return venv / ("Scripts/qt.exe" if os.name == "nt" else "bin/qt")


def _venv_check(repo_root: Path, venv: Path) -> SetupCheck:
    python_path = _venv_python(venv)
    if not python_path.exists():
        return SetupCheck(
            "venv",
            "failed",
            f"{venv} does not contain a Python interpreter",
            remediation="Run `make deps` after installing Python 3.12+.",
        )
    version, raw = _python_version(str(python_path), cwd=repo_root)
    if not _python_ok(version):
        return SetupCheck(
            "venv",
            "failed",
            f"{python_path} is {_version_label(version)} ({raw}); {required_python_label()} is required",
            remediation="Move the old venv aside and run `make deps PY=python3.12`.",
        )
    return SetupCheck("venv", "ok", f"{python_path} is {_version_label(version)}")


def _editable_install_check(repo_root: Path, venv: Path) -> SetupCheck:
    qt_path = _venv_qt(venv)
    if not qt_path.exists():
        return SetupCheck(
            "editable_install",
            "failed",
            f"{qt_path} is missing",
            remediation="Run `make deps`.",
        )
    python_path = _venv_python(venv)
    command = [
        str(python_path),
        "-c",
        "import importlib.metadata as m; print(m.version('quant-trad'))",
    ]
    code, output = _command_output(command, cwd=repo_root)
    if code != 0:
        return SetupCheck(
            "editable_install",
            "failed",
            output or "quant-trad package metadata is unavailable",
            remediation="Run `make deps`.",
        )
    return SetupCheck("editable_install", "ok", f"quant-trad {output.strip()} installed")


def _pg_dsn_check(values: Mapping[str, str]) -> SetupCheck:
    pg_dsn = str(os.environ.get("PG_DSN") or values.get("PG_DSN") or "").strip()
    if not pg_dsn:
        return SetupCheck(
            "pg_dsn",
            "failed",
            "PG_DSN is missing",
            remediation="Run `qt setup env` or set PG_DSN in secrets.env.",
        )
    if "replace-with" in pg_dsn or "<" in pg_dsn:
        return SetupCheck(
            "pg_dsn",
            "failed",
            "PG_DSN still contains placeholder text",
            remediation="Run `qt setup env` or edit secrets.env with local database values.",
        )
    if not pg_dsn.startswith("postgresql"):
        return SetupCheck(
            "pg_dsn",
            "failed",
            "PG_DSN must be a PostgreSQL SQLAlchemy DSN",
            remediation="Use the single PG_DSN persistence boundary.",
        )
    try:
        _ = urllib.parse.urlsplit(pg_dsn).port
    except ValueError:
        return SetupCheck(
            "pg_dsn",
            "failed",
            "PG_DSN is not a valid PostgreSQL URL",
            remediation="Use the single PG_DSN persistence boundary.",
        )
    expected_user = str(values.get("POSTGRES_USER") or "").strip()
    expected_password = str(values.get("POSTGRES_PASSWORD") or "")
    expected_db = str(values.get("POSTGRES_DB") or "").strip()
    consistency = _local_pg_dsn_consistency(
        pg_dsn,
        user=expected_user,
        password=expected_password,
        db=expected_db,
    )
    if consistency is False:
        return SetupCheck(
            "pg_dsn",
            "failed",
            "Local PG_DSN credentials do not match POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB",
            remediation="Run `qt setup env` to align the single local PG_DSN.",
        )
    return SetupCheck("pg_dsn", "ok", "PG_DSN configured")


def _credential_key_check(values: Mapping[str, str]) -> SetupCheck:
    value = str(os.environ.get("QT_SECURITY_PROVIDER_CREDENTIAL_KEY") or values.get("QT_SECURITY_PROVIDER_CREDENTIAL_KEY") or "").strip()
    valid, detail = validate_fernet_key(value)
    if not valid:
        return SetupCheck(
            "provider_credential_key",
            "failed",
            f"QT_SECURITY_PROVIDER_CREDENTIAL_KEY is {detail}",
            remediation="Run `qt setup env`; if credentials already exist, re-save them after fixing the key.",
        )
    return SetupCheck("provider_credential_key", "ok", "credential encryption key configured")


def _backend_check(api_url: str, timeout: float) -> SetupCheck:
    url = str(api_url).rstrip("/") + "/api/health"
    request = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            payload = json.loads(body) if body else {}
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return SetupCheck(
            "backend",
            "warning",
            f"backend unavailable at {url}: {exc}",
            required=False,
            remediation="Start core services with `make up BUILD=1 STACK_PROFILES=core`.",
        )
    status = str(payload.get("status") or "").lower()
    if status != "ok":
        return SetupCheck(
            "backend",
            "warning",
            f"backend responded without ok status: {payload}",
            required=False,
            remediation="Check backend logs with `make logs SERVICE=backend`.",
        )
    return SetupCheck("backend", "ok", f"{url} returned ok", required=False)


def setup_doctor_payload(
    *,
    repo_root: str | Path = REPO_ROOT,
    venv: str | Path = DEFAULT_VENV,
    api_url: str = "http://127.0.0.1:8000",
    timeout: float = 2.0,
    include_backend: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root)
    venv_path = Path(venv)
    if not venv_path.is_absolute():
        venv_path = root / venv_path
    checks: list[SetupCheck] = []

    current_version = (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    if _python_ok(current_version):
        checks.append(SetupCheck("current_python", "ok", f"{sys.executable} is {_version_label(current_version)}"))
    else:
        checks.append(
            SetupCheck(
                "current_python",
                "warning",
                f"{sys.executable} is {_version_label(current_version)}; setup target is {required_python_label()}",
                required=False,
                remediation="Use `make deps PY=python3.12` for bootstrap.",
            )
        )

    selected = find_python(cwd=root).get("selected")
    if selected:
        checks.append(SetupCheck("target_python", "ok", f"{selected['executable']} is {selected['version']}"))
    else:
        checks.append(
            SetupCheck(
                "target_python",
                "failed",
                f"No Python {required_python_label()} interpreter found on PATH",
                remediation="Install Python 3.12+ and run `make deps`.",
            )
        )

    env_path = root / "secrets.env"
    if env_path.exists():
        checks.append(SetupCheck("operator_env", "ok", str(env_path)))
    else:
        checks.append(
            SetupCheck(
                "operator_env",
                "failed",
                "secrets.env is missing",
                remediation="Run `qt setup env`.",
            )
        )
    values = _env_values(env_path)
    checks.append(_pg_dsn_check(values))
    checks.append(_credential_key_check(values))
    checks.append(_venv_check(root, venv_path))
    if checks[-1].status == "ok":
        checks.append(_editable_install_check(root, venv_path))
    else:
        checks.append(
            SetupCheck(
                "editable_install",
                "failed",
                "editable install cannot be checked until the venv is valid",
                remediation="Run `make deps`.",
            )
        )
    if include_backend:
        checks.append(_backend_check(api_url, timeout))

    status = "ok"
    if any(check.status == "failed" and check.required for check in checks):
        status = "needs_attention"
    elif any(check.status in {"failed", "warning"} for check in checks):
        status = "degraded"
    return {
        "schema_version": SETUP_SCHEMA_VERSION,
        "operation": "doctor",
        "status": status,
        "repo_root": str(root),
        "venv": str(venv_path),
        "checks": [check.to_dict() for check in checks],
        "next_steps": setup_next_steps(status),
    }


def setup_next_steps(status: str) -> list[str]:
    if status == "ok":
        return [
            "Run `make up BUILD=1 STACK_PROFILES=core` when you need the local stack.",
            "Run `./scripts/qt setup provider coinbase` when Coinbase credentials are needed.",
        ]
    if status == "degraded":
        return [
            "Start core services with `make up BUILD=1 STACK_PROFILES=core` when backend access is needed.",
            "Run `./scripts/qt setup provider coinbase` after the backend is available when Coinbase credentials are needed.",
        ]
    return [
        f"Install Python {required_python_label()} if `target_python` failed.",
        "Run `make deps PY=python3.12` from the repo root.",
        "Run `./scripts/qt setup env` to create or repair local operator env values.",
        "Run `make up BUILD=1 STACK_PROFILES=core` after local setup succeeds.",
    ]


def setup_env_payload(*, repo_root: str | Path = REPO_ROOT) -> tuple[int, dict[str, Any]]:
    try:
        result = ensure_operator_env(repo_root)
        payload = {
            "schema_version": SETUP_SCHEMA_VERSION,
            "operation": "env",
            "status": "ok" if result["credential_key_valid"] else "needs_attention",
            **result,
        }
        if not result["credential_key_valid"]:
            payload["error"] = "QT_SECURITY_PROVIDER_CREDENTIAL_KEY is invalid and was not rotated automatically."
            payload["remediation"] = "Fix the key manually, then re-save provider credentials."
        return (0 if payload["status"] == "ok" else 1), payload
    except Exception as exc:
        return 1, {
            "schema_version": SETUP_SCHEMA_VERSION,
            "operation": "env",
            "status": "failed",
            "error": str(exc),
        }
