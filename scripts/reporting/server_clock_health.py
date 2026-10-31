"""Read-only host chrony status -> bounded, non-secret operational JSONL.

No clock corrections, network configuration, research admission, or log access.
The uncertainty bound assumes the selected upstream clock is correct.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess

MAX_REFERENCE_AGE = 1800.0
MAX_UNCERTAINTY = 0.100
MAX_FILE_BYTES = 1024 * 1024
COMMANDS = tuple(("/usr/bin/chronyc", *args) for args in (
    ("tracking",), ("activity",), ("-n", "sources"),
))


def assess(tracking: str, activity: str, sources: str, now: datetime) -> dict:
    """Reject unavailable/malformed evidence rather than infer a healthy clock."""
    result = {"schema_version": 1, "sample_kind": "clock_health",
              "resource_id": "host-clock", "observed_at": now.isoformat(),
              "available": 0, "healthy": 0, "reasons": []}
    try:
        fields = dict(line.split(" : ", 1) for line in tracking.splitlines() if " : " in line)
        fields = {key.strip(): value.strip() for key, value in fields.items()}
        def seconds(key: str, signed: bool = False) -> float:
            pattern = r"([+-]?[0-9.eE]+) seconds" + (r" (?:fast|slow) of NTP time" if signed else "")
            match = re.fullmatch(pattern, fields[key])
            if not match:
                raise ValueError(key)
            value = float(match[1])
            if not math.isfinite(value) or value < 0:
                raise ValueError(key)
            return value
        offset = seconds("System time", True)
        delay = seconds("Root delay")
        dispersion = seconds("Root dispersion")
        reference = datetime.strptime(fields["Ref time (UTC)"], "%a %b %d %H:%M:%S %Y").replace(tzinfo=timezone.utc)
        age = (now - reference).total_seconds()
        online_match = re.search(r"^([0-9]+) sources online$", activity, re.M)
        if not online_match:
            raise ValueError("online sources")
        online = int(online_match[1])
        selected = 0
        for line in sources.splitlines():
            parts = line.split()
            if parts and parts[0] in ("^*", "=*"):
                if len(parts) < 7 or not 1 <= int(parts[2]) <= 15 or not 0 < int(parts[4], 8) <= 255:
                    raise ValueError("selected source")
                selected += 1
        if selected > 1:
            raise ValueError("multiple selected sources")
        uncertainty = offset + delay / 2 + dispersion
        if not math.isfinite(uncertainty):
            raise ValueError("uncertainty")
        result.update(available=1, online_sources=online, selected_sources=selected,
                      reference_age_seconds=age, uncertainty_seconds=uncertainty)
        reasons = result["reasons"]
        if fields["Leap status"] not in ("Normal", "Insert second", "Delete second"):
            reasons.append("unsynchronised")
        if not online or not selected:
            reasons.append("no_selected_online_source")
        if age < -1:
            reasons.append("future_reference")
        if age > MAX_REFERENCE_AGE:
            reasons.append("stale_reference")
        if uncertainty > MAX_UNCERTAINTY:
            reasons.append("excessive_uncertainty")
        result["healthy"] = int(not reasons)
    except (KeyError, ValueError, OverflowError):
        result.update(available=0, healthy=0, reasons=["invalid_status"])
    return result


def sample() -> dict:
    outputs = []
    try:
        for command in COMMANDS:
            completed = subprocess.run(command, capture_output=True, text=True,
                                       check=True, timeout=3, env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"})
            outputs.append(completed.stdout)
    except (OSError, subprocess.SubprocessError) as exc:
        # Do not export source addresses, raw command output, or host internals.
        return {"schema_version": 1, "sample_kind": "clock_health", "resource_id": "host-clock",
                "observed_at": datetime.now(timezone.utc).isoformat(), "available": 0,
                "healthy": 0, "reasons": ["probe_timeout" if isinstance(exc, subprocess.TimeoutExpired) else "probe_failed"]}
    return assess(*outputs, datetime.now(timezone.utc))


def append_sample(directory: Path, record: dict) -> None:
    """One systemd oneshot writer; two bounded regular files in an owned directory."""
    metadata = directory.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise ValueError("clock_export_requires_owned_directory")
    if metadata.st_mode & 0o022:
        raise ValueError("clock_export_directory_is_writable_by_others")
    path = directory / "clock-health.jsonl"
    backup = directory / "clock-health.jsonl.1"
    for candidate in (path, backup):
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o022:
            raise ValueError("clock_export_requires_owned_regular_files")
    line = json.dumps(record, allow_nan=False, separators=(",", ":")) + "\n"
    if path.exists() and path.stat().st_size + len(line.encode()) > MAX_FILE_BYTES:
        path.replace(backup)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o644)
    with os.fdopen(fd, "a") as output:
        output.write(line)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path)
    args = parser.parse_args()
    record = sample()
    if args.output_directory:
        append_sample(args.output_directory, record)
    print(json.dumps(record, allow_nan=False))
    return 0 if record["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
