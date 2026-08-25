"""Run one assurance child with an in-container process-group timeout."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys


TIMEOUT_MARKER = b"qt_assurance_process_guard:timeout_child_group_terminated\n"
CHILD_ENV_ALLOWLIST = {
    "HOME",
    "LANG",
    "NO_COLOR",
    "PATH",
    "PG_DSN",
    "PYTHONHASHSEED",
    "PYTHONDONTWRITEBYTECODE",
    "PYTEST_ADDOPTS",
    "PYTEST_PLUGINS",
    "PYTHONPATH",
    "QT_ASSURANCE_MODE",
    "QT_DB_TEST_ISOLATED",
    "QT_EXTERNAL_ORDER_SUBMISSION_ENABLED",
    "RUN_DB_TESTS",
    "TMPDIR",
    "TZ",
}


def child_environment(source: dict[str, str] | None = None) -> dict[str, str]:
    """Return the exact proof-child environment, excluding image-baked extras."""

    source = dict(os.environ if source is None else source)
    result = {
        key: source[key]
        for key in sorted(CHILD_ENV_ALLOWLIST)
        if key in source
    }
    for key, value in result.items():
        if any(character in value for character in "\x00\r\n"):
            raise ValueError(f"unsafe child environment value:{key}")
    if result.get("QT_EXTERNAL_ORDER_SUBMISSION_ENABLED") != "0":
        raise ValueError("external order submission must be disabled")
    if result.get("QT_ASSURANCE_MODE") != "1":
        raise ValueError("assurance mode must be enabled")
    if not result.get("PATH"):
        raise ValueError("exact child PATH required")
    return result


def run(argv: list[str], timeout_seconds: int) -> int:
    if not argv:
        raise ValueError("child argv required")
    process = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=child_environment(),
        shell=False,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        sys.stdout.buffer.write(stdout)
        sys.stderr.buffer.write(stderr)
        sys.stderr.buffer.write(TIMEOUT_MARKER)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.flush()
        return 124
    sys.stdout.buffer.write(stdout)
    sys.stderr.buffer.write(stderr)
    sys.stdout.buffer.flush()
    sys.stderr.buffer.flush()
    return int(process.returncode)


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, required=True)
    parser.add_argument("argv", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    if not argv:
        parser.error("child argv required after --")
    return run(argv, args.timeout)


if __name__ == "__main__":
    raise SystemExit(_cli())
