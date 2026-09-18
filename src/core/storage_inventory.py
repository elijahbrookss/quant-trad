"""Read administrator-prepared storage targets without probing block devices.

The inventory is host topology, not a second policy authority. Database state
owns enrollment and role assignment. This module never creates directories,
formats disks, mounts filesystems, or accepts paths from browser requests.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .storage_targets import StorageTarget

INVENTORY_VERSION = "qt.storage_inventory.v1"
MAX_INVENTORY_BYTES = 128 * 1024


def read_storage_inventory(path: Path) -> tuple[StorageTarget, ...]:
    try:
        with Path(path).open("rb") as handle:
            raw = handle.read(MAX_INVENTORY_BYTES + 1)
    except FileNotFoundError:
        return ()
    if len(raw) > MAX_INVENTORY_BYTES:
        raise ValueError("storage_inventory_invalid: inventory too large")
    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict) or set(payload) != {"schema_version", "targets"}:
            raise ValueError("invalid fields")
        if payload["schema_version"] != INVENTORY_VERSION:
            raise ValueError("unsupported schema_version")
        rows = payload["targets"]
        if not isinstance(rows, list) or len(rows) > 32:
            raise ValueError("targets must contain at most 32 entries")
        targets = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("target must be an object")
            values: dict[str, Any] = dict(row)
            values["roles"] = tuple(values.get("roles", ("recent", "history", "archives", "backups")))
            target = StorageTarget(**values)
            if target.state != "active":
                raise ValueError("host inventory entries must be active")
            targets.append(target)
        for attr in ("target_id", "filesystem_uuid", "root"):
            if len({getattr(t, attr) for t in targets}) != len(targets):
                raise ValueError(f"duplicate {attr}")
        return tuple(targets)
    except (TypeError, KeyError, ValueError) as exc:
        raise ValueError(f"storage_inventory_invalid: {exc}") from exc
