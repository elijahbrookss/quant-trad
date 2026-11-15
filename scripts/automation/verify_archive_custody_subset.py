"""Verify exact accepted archive objects on a separate, read-only local device."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from tempfile import NamedTemporaryFile
from time import monotonic

from core.storage_mounts import inspect_filesystem
from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation.stage_encrypted_recovery_handoff import _encode as _json_line, _file, _stamp
from scripts.automation.storage_host_boundary import load_receipt
from scripts.automation.verify_canonical_recovery_archive import _path, _sha256
from scripts.automation.verify_encrypted_recovery_handoff import _directory
from scripts.db.canonical_cold_recovery_suffix import _encode, prepare_replayed_archive_baseline, read_archive_catalog_inventory
from scripts.db.canonical_cold_replayed_checkpoint import _lsn


_VERSION = "qt.archive_custody_subset.v1"
_PAUSED_ARCHIVE = "qt.paused_archive_suffix.v1"
_DEVICE = re.compile(r"(?:0|[1-9][0-9]{0,9}):(?:0|[1-9][0-9]{0,9})")


def _device_file(path):
    with path.open("r", encoding="ascii") as stream:
        value = stream.read(24).strip()
    if not _DEVICE.fullmatch(value):
        raise ValueError("archive_custody_block_device_metadata_invalid")
    return value


def _physical_devices(sysfs, device):
    """Collapse partitions and device-mapper leaves to their physical Linux paths."""
    paths = sysfs / "devices"
    pin = sysfs / "dev" / "block" / device
    try:
        start = pin.resolve(strict=True)
    except FileNotFoundError:
        raise ValueError("archive_custody_block_device_unavailable") from None
    if not start.is_relative_to(paths) or _device_file(start / "dev") != device:
        raise ValueError("archive_custody_block_device_unavailable")
    pending = [start]
    visited = set()
    leaves = {}
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        if len(visited) > 64 or not current.is_relative_to(paths):
            raise ValueError("archive_custody_block_ancestry_outside_budget")
        native = _device_file(current / "dev")
        if (sysfs / "dev" / "block" / native).resolve(strict=True) != current:
            raise ValueError("archive_custody_block_ancestry_changed")
        if (current / "partition").exists():
            pending.append(current.parent)
            continue
        with os.scandir(current / "slaves") as children:
            downstream = []
            for child in children:
                if len(downstream) >= 64:
                    raise ValueError("archive_custody_block_ancestry_outside_budget")
                downstream.append(Path(child.path).resolve(strict=True))
        if downstream:
            pending.extend(downstream)
            continue
        if (current.is_relative_to(paths / "virtual") or any(part.startswith("virtio") for part in current.parts)
                or not (current / "device").exists()):
            raise ValueError("archive_custody_physical_device_unresolved")
        # Multiple namespaces can share one physical NVMe controller.
        unit = (current.parent if re.fullmatch(r"nvme[0-9]+n[0-9]+", current.name)
                and re.fullmatch(r"nvme[0-9]+", current.parent.name) else current)
        leaves[native] = (current.name, hashlib.sha256(os.fsencode(unit.relative_to(paths))).hexdigest())
    if not leaves:
        raise ValueError("archive_custody_physical_device_unresolved")
    return [{"device_id": native, "block_device": name, "physical_unit_sha256": digest}
            for native, (name, digest) in sorted(leaves.items())]


def _device_evidence(sysfs, source_devices, destination):
    source = {device: _physical_devices(sysfs, device) for device in sorted(source_devices)}
    receiver = _physical_devices(sysfs, destination)
    units = {leaf["physical_unit_sha256"] for leaves in source.values() for leaf in leaves}
    shared = [leaf["block_device"] for leaf in receiver if leaf["physical_unit_sha256"] in units]
    if shared:
        raise ValueError(f"archive_custody_shared_physical_device: destination={destination} shared={','.join(shared)}")
    return source, receiver


def _object_file(root, device, key):
    candidate = root
    parts = Path(key).parts
    for index, part in enumerate(parts):
        candidate /= part
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            return None
        leaf = index + 1 == len(parts)
        expected_type = stat.S_ISREG if leaf else stat.S_ISDIR
        if info.st_dev != device or not expected_type(info.st_mode) or (leaf and info.st_nlink != 1):
            raise ValueError(f"archive_custody_unsafe_object_path: {key}")
    return _path(root, key, absolute=False), info


def _paused_catalog(inventory, receipt, pin, *, source_device, source_inode, max_objects, check):
    body = {key: value for key, value in receipt.items() if key != "verification_sha256"}
    layout = body.get("storage_layout")
    evidence_paths = [body.get(field) for field in ("catalog_inventory_path", "archive_root", "suffix_root", "source_archive")]
    if (body.get("schema") != _PAUSED_ARCHIVE or archive_evidence_hash(body) != pin
            or body.get("materialize_baseline") is not True
            or not all(_sha256(body.get(field)) for field in ("pair_receipt_sha256", "catalog_inventory_sha256"))
            or not isinstance(body.get("database_identity"), str)
            or not re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", body["database_identity"])
            or not isinstance(layout, dict) or set(layout) != {"layout_version", "certificate_sha256"}
            or not isinstance(layout["layout_version"], str) or not layout["layout_version"]
            or not _sha256(layout["certificate_sha256"])
            or not all(isinstance(value, str) and PurePosixPath(value).root == "/"
                       and str(PurePosixPath(value)) == value and ".." not in PurePosixPath(value).parts
                       for value in evidence_paths)):
        raise ValueError("archive_custody_paused_receipt_invalid")
    catalog, archive, objects, _ = map(PurePosixPath, evidence_paths)
    numeric = ("timeline", "accepted_commit_seq", "catalog_objects", "catalog_bytes", "materialized_objects",
               "materialized_bytes", "baseline_objects_used", "baseline_bytes_used", "suffix_objects", "suffix_bytes")
    if (catalog != archive / "catalog.jsonl" or objects != archive / "objects"
            or any(type(body.get(field)) is not int or body[field] < 0 for field in numeric)
            or not 1 <= body["timeline"] <= 2**32 - 1
            or not 0 < _lsn(body.get("target_lsn")) <= _lsn(body.get("replay_lsn"))
            or (body["materialized_objects"], body["materialized_bytes"], body.get("materialized_inventory_sha256"))
            != (body["catalog_objects"], body["catalog_bytes"], body["catalog_inventory_sha256"])
            or body["baseline_objects_used"] + body["suffix_objects"] != body["catalog_objects"]
            or body["baseline_bytes_used"] + body["suffix_bytes"] != body["catalog_bytes"]):
        raise ValueError("archive_custody_paused_materialized_view_incomplete")
    # The older same-HDD copier used device_id for both its readonly input and
    # private output. The newer independent receiver records the input separately.
    if (body.get("source_device_id", body.get("device_id")) != source_device
            or type(body.get("source_inode")) is not int or body["source_inode"] != source_inode):
        raise ValueError("archive_custody_paused_source_scope_changed")
    return read_archive_catalog_inventory(inventory, inventory_sha256=body["catalog_inventory_sha256"],
        count=body["catalog_objects"], total=body["catalog_bytes"], max_objects=max_objects, check=check)


def verify_archive_custody_subset(*, inventory, destination, destination_inode,
                                  destination_uuid, source_root, source_uuid, source_inode,
                                  max_objects, max_verify_bytes, timeout_seconds, source_devices=(),
                                  checkpoint=None, checkpoint_sha256=None, paused_archive_receipt=None,
                                  paused_archive_receipt_sha256=None, record_gap=None,
                                  sysfs=Path("/sys"), udev=Path("/run/udev/data")):
    """Compare a pinned canonical catalog with independently mounted objects.

    Missing or different bytes are coverage gaps. Mount aliases, unsafe object
    paths, changed evidence and exhausted budgets fail the verification. A
    caller collecting gap rows must publish them only after this returns.
    """
    proof = checkpoint if checkpoint is not None else paused_archive_receipt
    pin = checkpoint_sha256 if checkpoint is not None else paused_archive_receipt_sha256
    if ((checkpoint is None) == (paused_archive_receipt is None)
            or (checkpoint is None and checkpoint_sha256 is not None)
            or (paused_archive_receipt is None and paused_archive_receipt_sha256 is not None)
            or not isinstance(proof, dict) or not _sha256(pin) or proof.get("verification_sha256") != pin
            or any(not isinstance(uuid, str) or not re.fullmatch(r"[A-Za-z0-9-]{4,128}", uuid)
                   for uuid in (source_uuid, destination_uuid))
            or not isinstance(source_devices, (tuple, list)) or len(source_devices) > 15
            or not all(isinstance(value, str) and _DEVICE.fullmatch(value) for value in source_devices)
            or len(set(source_devices)) != len(source_devices)
            or (record_gap is not None and not callable(record_gap))
            or any(type(value) is not int or not minimum <= value <= maximum for value, minimum, maximum in (
                (source_inode, 1, 2**64 - 1), (destination_inode, 1, 2**64 - 1), (max_objects, 1, 10_000_000),
                (max_verify_bytes, 1, 2**63 - 1), (timeout_seconds, 1, 86_400),
            ))):
        raise ValueError("archive_custody_scope_invalid")
    destination = _directory(destination)
    source_root = _directory(source_root)
    sysfs = _directory(sysfs)
    if source_root.is_relative_to(destination) or destination.is_relative_to(source_root):
        raise ValueError("archive_custody_source_destination_overlap")
    started = monotonic()
    deadline = started + timeout_seconds
    next_scope = started

    def observe(root, uuid, inode, *, readonly):
        info = root.lstat()
        mount = inspect_filesystem(root, expected_uuid=uuid, udev_root=udev, require_writable=False)
        if (info.st_ino != inode or mount.device_id != f"{os.major(info.st_dev)}:{os.minor(info.st_dev)}"
                or (readonly and not mount.read_only)):
            raise ValueError(f"archive_custody_mount_pin_changed: {root}")
        return (info.st_dev, info.st_ino), mount.device_id

    identity, destination_device = observe(destination, destination_uuid, destination_inode, readonly=True)
    source_identity, source_device = observe(source_root, source_uuid, source_inode, readonly=False)
    excluded = set(source_devices) | {source_device}
    if destination_device in excluded:
        raise ValueError(f"archive_custody_shared_physical_device: destination={destination_device}")
    backing = _device_evidence(sysfs, excluded, destination_device)

    def check():
        nonlocal next_scope
        now = monotonic()
        if now >= deadline:
            raise TimeoutError("archive_custody_deadline_exceeded")
        if now >= next_scope:
            next_scope = now + 0.25
            current = destination.lstat()
            if (current.st_dev, current.st_ino) != identity or not os.statvfs(destination).f_flag & os.ST_RDONLY:
                raise RuntimeError("archive_custody_destination_changed")
            original = source_root.lstat()
            if (original.st_dev, original.st_ino) != source_identity:
                raise RuntimeError("archive_custody_source_changed")

    if checkpoint is not None:
        objects = prepare_replayed_archive_baseline(inventory, checkpoint=checkpoint, max_objects=max_objects, check=check).objects
    else:
        objects = _paused_catalog(inventory, proof, pin, source_device=source_device, source_inode=source_identity[1],
                                  max_objects=max_objects, check=check)
    labels = ("verified", "missing", "mismatched")
    counts = {label: {"objects": 0, "bytes": 0} for label in labels}
    digests = {label: hashlib.sha256() for label in labels}
    gaps = hashlib.sha256()
    bytes_read = 0
    with archive_namespace(destination, exclusive=True) as held:
        for key, descriptor in objects.items():
            check()
            found = _object_file(destination, identity[0], key)
            if found is None:
                status, reason = "missing", "absent"
            elif found[1].st_size != descriptor[1]:
                status, reason = "mismatched", "size"
            else:
                bytes_read += found[1].st_size
                if bytes_read > max_verify_bytes:
                    raise ValueError("archive_custody_verification_byte_budget_exceeded")
                actual = _file(found[0], _stamp(found[1]), check)
                status, reason = ("verified", None) if actual["sha256"] == descriptor[0] else ("mismatched", "sha256")
            counts[status]["objects"] += 1
            counts[status]["bytes"] += descriptor[1]
            digests[status].update(_encode(key, descriptor))
            if status != "verified":
                gap = {"object_key": key, "sha256": descriptor[0], "bytes": descriptor[1], "status": status, "reason": reason}
                gaps.update(_json_line(gap))
                if record_gap is not None:
                    record_gap(gap)
        check()
        held()
        if (observe(destination, destination_uuid, destination_inode, readonly=True) != (identity, destination_device)
                or observe(source_root, source_uuid, source_inode, readonly=False) != (source_identity, source_device)
                or _device_evidence(sysfs, excluded, destination_device) != backing):
            raise RuntimeError("archive_custody_mount_evidence_changed")
    proof_pin = {"checkpoint_sha256": pin} if checkpoint is not None else {"paused_archive_receipt_sha256": pin}
    report = {"schema": _VERSION, "scope": "archive_object_bytes", "database_identity": proof["database_identity"],
              **proof_pin, "inventory_proof_schema": proof["schema"], "pair_receipt_sha256": proof["pair_receipt_sha256"],
              "required_inventory_sha256": proof["catalog_inventory_sha256"],
              "required_objects": proof["catalog_objects"], "required_bytes": proof["catalog_bytes"],
              "source": {"path": str(source_root), "device_id": source_device, "inode": source_inode,
                         "filesystem_uuid": source_uuid},
              "source_block_ancestry": backing[0],
              "destination": {"path": str(destination), "device_id": destination_device, "inode": destination_inode,
                              "filesystem_uuid": destination_uuid, "block_ancestry": backing[1]},
              "coverage": {label: counts[label] | {"inventory_sha256": digests[label].hexdigest()} for label in labels},
              "gap_report_sha256": gaps.hexdigest(), "verified_file_bytes_read": bytes_read}
    return report | {"verification_sha256": archive_evidence_hash(report)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("inventory", "destination", "source-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    evidence = parser.add_mutually_exclusive_group(required=True)
    evidence.add_argument("--checkpoint", type=Path)
    evidence.add_argument("--paused-archive-receipt", type=Path)
    parser.add_argument("--checkpoint-sha256")
    parser.add_argument("--paused-archive-receipt-sha256")
    parser.add_argument("--destination-inode", type=int, required=True)
    parser.add_argument("--destination-uuid", required=True)
    parser.add_argument("--source-inode", type=int, required=True)
    parser.add_argument("--source-uuid", required=True)
    parser.add_argument("--source-device", action="append", default=[], help="Additional source Linux major:minor (repeatable).")
    parser.add_argument("--max-objects", type=int, required=True)
    parser.add_argument("--max-verify-bytes", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, required=True)
    parser.add_argument("--gap-report", type=Path, help="Write verified missing and mismatched object descriptors as JSONL.")
    args = parser.parse_args(argv)
    staging = None
    try:
        proof_path = args.checkpoint or args.paused_archive_receipt
        if not proof_path.is_absolute() or proof_path.resolve(strict=True) != proof_path:
            raise ValueError("archive_custody_inventory_proof_path_invalid")
        proof = load_receipt(proof_path, max_bytes=65_536)
        if args.gap_report is not None:
            folder = _directory(args.gap_report.parent)
            if (folder != args.gap_report.parent or folder.is_relative_to(args.destination)
                    or folder.is_relative_to(args.source_root) or os.path.lexists(args.gap_report)):
                raise ValueError("archive_custody_invalid_gap_report_destination")
        output = NamedTemporaryFile(mode="wb", dir=folder, prefix=".archive-gaps-", delete=False) if args.gap_report else nullcontext()
        with output as stream:
            staging = Path(stream.name) if stream else None
            report = verify_archive_custody_subset(
                inventory=args.inventory, checkpoint=proof if args.checkpoint else None,
                checkpoint_sha256=args.checkpoint_sha256, paused_archive_receipt=proof if args.paused_archive_receipt else None,
                paused_archive_receipt_sha256=args.paused_archive_receipt_sha256,
                destination=args.destination, destination_inode=args.destination_inode, destination_uuid=args.destination_uuid,
                source_root=args.source_root, source_uuid=args.source_uuid, source_inode=args.source_inode,
                source_devices=args.source_device, max_objects=args.max_objects, max_verify_bytes=args.max_verify_bytes,
                timeout_seconds=args.timeout_seconds, record_gap=(lambda row: stream.write(_json_line(row))) if stream else None,
            )
            if stream is not None:
                stream.flush()
                os.fsync(stream.fileno())
        if staging is not None:
            os.link(staging, args.gap_report)
            staging.unlink()
            _sync(args.gap_report.parent)
        print(json.dumps(report, sort_keys=True, indent=2, allow_nan=False))
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    finally:
        if staging is not None and staging.exists():
            staging.unlink()


if __name__ == "__main__":
    main()
