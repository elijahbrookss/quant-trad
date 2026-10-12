#!/usr/bin/env python3
"""Read-only block-device identity and signature audit before storage setup.

Run on the host with permission to read the explicitly named block device.
This command never formats, mounts, repairs, wipes, or changes a device.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import subprocess


def run_json(command: list[str]) -> dict:
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
    return json.loads(result.stdout)


def audit(device: Path, expected_serial: str) -> dict:
    requested = str(device)
    if not device.is_absolute() or not str(device).startswith("/dev/disk/by-id/"):
        raise ValueError("storage_audit_invalid: stable /dev/disk/by-id path required")
    resolved = device.resolve(strict=True)
    if not str(resolved).startswith("/dev/") or not stat.S_ISBLK(resolved.stat().st_mode):
        raise ValueError("storage_audit_invalid: target is not a block device")
    topology = run_json(["lsblk", "--json", "--bytes", "--paths", "--output",
                         "NAME,TYPE,SIZE,MODEL,SERIAL,WWN,FSTYPE,UUID,MOUNTPOINTS", str(resolved)])
    devices = topology.get("blockdevices", [])
    if len(devices) != 1 or devices[0].get("type") != "disk":
        raise ValueError("storage_audit_invalid: expected exactly one whole disk")
    observed_serial = str(devices[0].get("serial") or "").strip()
    if not expected_serial or observed_serial != expected_serial:
        raise ValueError("storage_audit_identity_mismatch")
    signatures = run_json(["wipefs", "--no-act", "--json", str(resolved)])
    return {"schema_version": "qt.storage_device_audit.v1", "requested_device": requested,
            "resolved_device": str(resolved), "topology": topology, "signatures": signatures,
            "destructive_changes_performed": False,
            "formatting_authorized": False,
            "note": "No detected signature is not proof that a disk contains no valuable data."}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", type=Path, required=True)
    parser.add_argument("--expected-serial", required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("host administrator authentication is required for the read-only device audit")
    try:
        result = audit(args.device, args.expected_serial)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"storage_audit_failed: {exc}\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
