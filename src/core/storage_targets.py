"""Physical storage targets and policy, independent of presentation and SQL.

Target identity and an object's recorded location outlive allocation policy.
A policy change never changes where an existing object is read.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from .storage_mounts import FilesystemEvidence, StorageMountError, inspect_filesystem

STORAGE_POLICY_VERSION = "qt.storage_policy.v1"
STORAGE_ROLES = ("recent", "history", "archives", "backups")
_TARGET_ID = re.compile(r"[a-z][a-z0-9_-]{0,47}")
_UUID = re.compile(r"[A-Za-z0-9-]{4,128}")


def _integer(value: Any, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"storage_policy_invalid: {name} must be {minimum}..{maximum}")
    return value


@dataclass(frozen=True)
class StorageTarget:
    target_id: str
    label: str
    filesystem_uuid: str
    root: str
    medium: str
    roles: tuple[str, ...] = STORAGE_ROLES
    state: str = "active"

    def __post_init__(self) -> None:
        if not isinstance(self.target_id, str) or not _TARGET_ID.fullmatch(self.target_id):
            raise ValueError("storage_target_invalid: invalid target_id")
        if not isinstance(self.label, str) or not self.label.strip() or len(self.label) > 80:
            raise ValueError("storage_target_invalid: label must be 1..80 characters")
        if not isinstance(self.filesystem_uuid, str) or not _UUID.fullmatch(self.filesystem_uuid):
            raise ValueError("storage_target_invalid: filesystem UUID required")
        if not isinstance(self.root, str) or "\x00" in self.root or "\\" in self.root:
            raise ValueError("storage_target_invalid: dedicated absolute root required")
        path = PurePosixPath(self.root)
        if not path.is_absolute() or path == PurePosixPath("/") or ".." in path.parts:
            raise ValueError("storage_target_invalid: dedicated absolute root required")
        if not isinstance(self.medium, str) or self.medium not in {"ssd", "hdd"}:
            raise ValueError("storage_target_invalid: medium must be ssd or hdd")
        if (not isinstance(self.roles, tuple) or not self.roles
                or any(not isinstance(role, str) or role not in STORAGE_ROLES for role in self.roles)
                or len(set(self.roles)) != len(self.roles)):
            raise ValueError("storage_target_invalid: invalid roles")
        if not isinstance(self.state, str) or self.state not in {"active", "draining", "disabled"}:
            raise ValueError("storage_target_invalid: invalid state")

    def inspect(self, *, require_writable: bool = False, udev_root: Path = Path("/run/udev/data")) -> FilesystemEvidence:
        return inspect_filesystem(
            Path(self.root), expected_uuid=self.filesystem_uuid,
            require_writable=require_writable, udev_root=udev_root,
        )


@dataclass(frozen=True)
class StoragePolicy:
    recent: tuple[str, ...]
    history: tuple[str, ...]
    archives: tuple[str, ...]
    backups: tuple[str, ...]
    recent_days: int = 30
    reserve_percent: int = 20
    backup_interval_hours: int = 24
    backup_copies: int = 2
    movement_enabled: bool = False
    backup_enabled: bool = False

    def __post_init__(self) -> None:
        for role in STORAGE_ROLES:
            values = getattr(self, role)
            if not isinstance(values, tuple) or not 1 <= len(values) <= 32:
                raise ValueError(f"storage_policy_invalid: {role} requires 1..32 targets")
            if any(not isinstance(v, str) or not _TARGET_ID.fullmatch(v) for v in values) or len(set(values)) != len(values):
                raise ValueError(f"storage_policy_invalid: {role} has invalid or duplicate targets")
        if len(self.recent) != 1:
            raise ValueError("storage_policy_invalid: recent requires exactly one target")
        _integer(self.recent_days, "recent_days", 1, 3650)
        _integer(self.reserve_percent, "reserve_percent", 10, 80)
        _integer(self.backup_interval_hours, "backup_interval_hours", 1, 168)
        _integer(self.backup_copies, "backup_copies", 1, 30)
        if type(self.movement_enabled) is not bool or type(self.backup_enabled) is not bool:
            raise ValueError("storage_policy_invalid: execution flags must be boolean")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> StoragePolicy:
        values = dict(payload)
        if values.pop("schema_version", STORAGE_POLICY_VERSION) != STORAGE_POLICY_VERSION:
            raise ValueError("storage_policy_invalid: unsupported schema_version")
        unknown = values.keys() - cls.__dataclass_fields__.keys()
        if unknown:
            raise ValueError(f"storage_policy_invalid: unknown fields={sorted(unknown)}")
        for role in STORAGE_ROLES:
            if not isinstance(values.get(role), (list, tuple)):
                raise ValueError(f"storage_policy_invalid: {role} must be a target list")
            values[role] = tuple(values[role])
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for role in STORAGE_ROLES:
            payload[role] = list(payload[role])
        return {"schema_version": STORAGE_POLICY_VERSION, **payload}

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def validate_targets(self, targets: Sequence[StorageTarget]) -> None:
        by_id = {target.target_id: target for target in targets}
        if len(by_id) != len(targets):
            raise ValueError("storage_policy_invalid: duplicate target identities")
        uuids = [target.filesystem_uuid for target in targets]
        if len(uuids) != len(set(uuids)):
            raise ValueError("storage_policy_invalid: one filesystem cannot be counted as two drives")
        for role in STORAGE_ROLES:
            for target_id in getattr(self, role):
                target = by_id.get(target_id)
                if target is None or role not in target.roles or target.state != "active":
                    raise ValueError(f"storage_policy_invalid: target={target_id} cannot receive role={role}")


@dataclass(frozen=True)
class StorageLocation:
    target_id: str
    object_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.target_id, str) or not _TARGET_ID.fullmatch(self.target_id):
            raise ValueError("storage_location_invalid: invalid target_id")
        if not isinstance(self.object_key, str):
            raise ValueError("storage_location_invalid: relative object key required")
        key = PurePosixPath(self.object_key)
        if (not self.object_key
                or "\\" in self.object_key or "\x00" in self.object_key
                or key.is_absolute() or any(part in {"", ".", ".."} for part in self.object_key.split("/"))):
            raise ValueError("storage_location_invalid: relative object key required")

    def resolve(self, target: StorageTarget, *, require_writable: bool = False,
                udev_root: Path = Path("/run/udev/data")) -> Path:
        if self.target_id != target.target_id:
            raise StorageMountError("storage_location_target_mismatch")
        evidence = target.inspect(require_writable=require_writable, udev_root=udev_root)
        root = Path(evidence.path)
        path = (root / self.object_key).resolve()
        if not path.is_relative_to(root):
            raise StorageMountError("storage_location_outside_target")
        ancestor = path
        while not ancestor.exists():
            ancestor = ancestor.parent
        if ancestor.stat().st_dev != root.stat().st_dev:
            raise StorageMountError("storage_location_wrong_filesystem")
        return path


def allocate_target(*, policy: StoragePolicy, role: str, targets: Sequence[StorageTarget],
                    capacity: Mapping[str, FilesystemEvidence], required_bytes: int,
                    reserved_bytes: Mapping[str, int] | None = None) -> StorageTarget:
    """Choose a destination for NEW work; never use this to locate old data.

    Caller holds the registry lock and durably reserves the chosen bytes in the
    same transaction. Capacity excludes each filesystem's OS-reserved blocks.
    """
    if role not in STORAGE_ROLES:
        raise ValueError("storage_allocation_invalid: unknown role")
    if type(required_bytes) is not int or required_bytes <= 0:
        raise ValueError("storage_allocation_invalid: required_bytes must be positive")
    reserved = reserved_bytes or {}
    policy.validate_targets(targets)
    by_id = {target.target_id: target for target in targets}
    eligible = []
    for target_id in getattr(policy, role):
        target = by_id[target_id]
        evidence = capacity.get(target_id)
        if evidence is None or evidence.read_only or evidence.filesystem_uuid != target.filesystem_uuid:
            continue
        reservation = reserved.get(target_id, 0)
        if type(reservation) is not int or reservation < 0:
            raise ValueError("storage_allocation_invalid: invalid reservation")
        headroom = evidence.available_bytes - (evidence.total_bytes * policy.reserve_percent + 99) // 100 - reservation
        if headroom >= required_bytes:
            eligible.append((headroom, target_id, target))
    if not eligible:
        raise StorageMountError(f"storage_capacity_blocked: no verified {role} target has reserved headroom")
    return sorted(eligible, key=lambda item: (-item[0], item[1]))[0][2]
