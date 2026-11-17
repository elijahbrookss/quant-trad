"""Seed a separate private candidate archive from the verified rehearsal view.

Mount the reviewed clone and suffix receipts and the entire source archive
read-only. Bind a separate empty destination directory writable on the same
HDD. This copies the catalogued bytes once and resumes only the same scope.
Run it before publishing any candidate pages.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from time import monotonic

from market_data.archive_namespace import archive_namespace
from market_data.fact_archive import archive_evidence_hash
from scripts.automation.canonical_cold_receiver_archive import _check_materialized_objects
from scripts.automation.stage_encrypted_recovery_handoff import _private_root, _stamp
from scripts.ci.materialize_paused_archive_suffix import _CATALOG_INVENTORY, _place_object, _scope_directory
from scripts.ci.measure_same_hdd_rehearsal import _require_isolated_mounts
from scripts.ci.verify_benchmark_clone_boundary import _read_receipt
from scripts.db.canonical_cold_recovery_suffix import read_archive_catalog_inventory


_VERSION = "qt.private_candidate_archive_seed.v1"


def _boundaries(args):
    proof = _read_receipt(args.clone_proof, args.clone_proof_sha256)
    suffix = _read_receipt(args.suffix_receipt, args.suffix_receipt_sha256)
    if (proof["schema"] != "qt.benchmark_clone_boundary.v1"
            or suffix["schema"] != "qt.paused_archive_suffix.v1"
            or proof["suffix_receipt_sha256"] != args.suffix_receipt_sha256
            or suffix["materialize_baseline"] is not True
            or any(proof[key] != suffix[key] for key in (
                "pair_receipt_sha256", "database_identity", "storage_layout", "device_id", "timeline",
                "target_lsn", "replay_lsn", "accepted_commit_seq",
            ))):
        raise ValueError("private_candidate_seed_recovery_boundary_changed")
    expected = proof["archive"]
    catalog = expected["inventory_sha256"], expected["objects"], expected["bytes"]
    if (catalog != (suffix["catalog_inventory_sha256"], suffix["catalog_objects"], suffix["catalog_bytes"])
            or catalog != (suffix["materialized_inventory_sha256"], suffix["materialized_objects"],
                           suffix["materialized_bytes"])
            or not 1 <= expected["objects"] <= args.max_objects
            or not 1 <= expected["bytes"] <= args.max_copy_bytes):
        raise ValueError("private_candidate_seed_catalog_changed_or_exceeds_budget")
    return proof, suffix


def _mounts(proof, suffix, *, source, source_objects, destination):
    paths = source, source_objects, destination
    identities = tuple(_private_root(path)[1] for path in paths)
    device = proof["device_id"]
    _require_isolated_mounts(source, destination, device)
    if (any(f"{os.major(actual)}:{os.minor(actual)}" != device for actual, _ in identities)
            or len(set(identities)) != len(identities)
            or identities[0][1] != proof["archive_inode"]
            or identities[0][1] != suffix["suffix_inode"]
            or identities[1][1] != proof["archive_objects_inode"]
            or not all(os.statvfs(path).f_flag & os.ST_RDONLY for path in (source, source_objects))
            or os.statvfs(destination).f_flag & os.ST_RDONLY):
        raise ValueError("private_candidate_seed_mount_identity_changed")
    return identities


def _free(root, reserve_bytes, *, additional_bytes=0, entries=0):
    fs = os.statvfs(root)
    available = fs.f_bavail * fs.f_frsize
    if available < reserve_bytes + additional_bytes or fs.f_favail < entries:
        raise RuntimeError("private_candidate_seed_insufficient_filesystem_headroom")
    return available


def seed(args):
    for key, minimum, maximum in (
        ("max_objects", 1, 10_000_000), ("max_copy_bytes", 1, 2**63 - 1),
        ("reserve_bytes", 0, 2**63 - 1), ("timeout_seconds", 1, 36 * 3600),
    ):
        value = getattr(args, key)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"private_candidate_seed_invalid_limit: {key}")
    started = monotonic()
    deadline = started + args.timeout_seconds

    def check_time():
        if monotonic() >= deadline:
            raise TimeoutError("private_candidate_seed_deadline")

    source = Path(args.source_archive)
    destination = Path(args.destination_archive)
    if source.is_relative_to(destination) or destination.is_relative_to(source):
        raise ValueError("private_candidate_seed_overlapping_paths")
    source_objects = source / "objects"
    catalog_file = source / _CATALOG_INVENTORY
    proof, suffix = _boundaries(args)
    mounts = _mounts(proof, suffix, source=source, source_objects=source_objects, destination=destination)
    inventory_digest = proof["archive"]["inventory_sha256"]
    count, total = proof["archive"]["objects"], proof["archive"]["bytes"]
    scope = {
        "schema": _VERSION, "clone_proof_sha256": args.clone_proof_sha256,
        "suffix_receipt_sha256": args.suffix_receipt_sha256, "database_identity": proof["database_identity"],
        "accepted_commit_seq": proof["accepted_commit_seq"], "device_id": proof["device_id"],
        "source_path": str(source), "source_archive_inode": mounts[0][1], "source_objects_inode": mounts[1][1],
        "archive_path": str(destination), "archive_inode": mounts[2][1],
        "catalog_inventory_sha256": inventory_digest, "catalog_objects": count, "catalog_bytes": total,
    }
    with archive_namespace(source, exclusive=True) as check_source, \
         archive_namespace(source_objects, exclusive=True) as check_source_objects, \
         archive_namespace(destination, exclusive=True) as check_destination:
        def check():
            check_time()
            check_source()
            check_source_objects()
            check_destination()

        inventory_stamp = _stamp(catalog_file.lstat())
        catalog = read_archive_catalog_inventory(
            catalog_file, inventory_sha256=inventory_digest, count=count, total=total,
            max_objects=args.max_objects, check=check,
        )
        block = os.statvfs(destination).f_frsize
        required_bytes, required_entries = 8 * block, 4
        for key, (_, size) in catalog.items():
            check_time()
            depth = len(Path(key).parts)
            required_entries += depth
            required_bytes += ((size + block - 1) // block + depth - 1) * block
        if required_bytes > args.max_copy_bytes:
            raise ValueError("private_candidate_seed_allocation_exceeds_copy_budget")
        new_scope = not os.path.lexists(destination / "scope.json")
        free_before = _free(destination, args.reserve_bytes,
                            additional_bytes=required_bytes if new_scope else 0,
                            entries=required_entries if new_scope else 0)
        objects, work = _scope_directory(destination, scope)
        copied = allocated = 0
        with archive_namespace(objects, exclusive=True) as check_objects:
            def check_copy():
                check()
                check_objects()

            for key, descriptor in catalog.items():
                check_copy()
                filename = objects / key
                if not os.path.lexists(filename):
                    _free(destination, args.reserve_bytes, entries=len(Path(key).parts) + 1)
                copied += _place_object(key, descriptor, source=source_objects, objects=objects, work=work,
                                        reserve_bytes=args.reserve_bytes, check=check_copy)
                allocated += filename.lstat().st_blocks * 512
            _check_materialized_objects(objects, device=mounts[2][0], count=count, check=check_copy)
            if (allocated > args.max_copy_bytes or _stamp(catalog_file.lstat()) != inventory_stamp
                    or _boundaries(args) != (proof, suffix)
                    or _mounts(proof, suffix, source=source, source_objects=source_objects,
                               destination=destination) != mounts):
                raise RuntimeError("private_candidate_seed_final_boundary_changed")
            free_after = _free(destination, args.reserve_bytes)
            check_copy()
            objects_inode = objects.lstat().st_ino

    result = scope | {
        "archive_objects_inode": objects_inode,
        "copied_objects": copied, "reused_objects": count - copied,
        "object_file_allocated_bytes": allocated,
        "planned_allocation_bound_bytes": required_bytes,
        "planned_file_and_directory_entries": required_entries,
        "filesystem_free_before_bytes": free_before, "filesystem_free_after_bytes": free_after,
        "elapsed_seconds": round(monotonic() - started, 3),
    }
    return result | {"verification_sha256": archive_evidence_hash(result)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-archive", "destination-archive", "clone-proof", "suffix-receipt"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("clone-proof-sha256", "suffix-receipt-sha256"):
        parser.add_argument(f"--{name}", required=True)
    for name in ("max-objects", "max-copy-bytes", "reserve-bytes", "timeout-seconds"):
        parser.add_argument(f"--{name}", type=int, required=True)
    try:
        receipt = seed(parser.parse_args(argv))
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"{type(error).__name__}: {error}\n")
    print(json.dumps(receipt, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
