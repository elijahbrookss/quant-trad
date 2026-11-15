"""Bind the original history registration to a promoted rehearsal copy.

Run only in the network-disabled rehearsal capsule. Bind the private copy as
the parent of pgdata and history, its original Unix socket, and the separately
seeded candidate archive. Use the clone and seed receipts reviewed for this
copy. The operation changes two registration rows on the private database and
does not move PostgreSQL files or change the serving storage policy.
"""

import argparse
import json
import os
from pathlib import Path
import re
import socket
import stat

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from core.storage_mounts import inspect_filesystem
from market_data.fact_archive import archive_evidence_hash
from portal.backend.service.storage.repos.market_lifecycle import _LIFECYCLE_LOCK_NAME
from scripts.automation.stage_encrypted_recovery_handoff import _private_root
from scripts.automation.verify_canonical_recovery_archive import _sha256
from scripts.ci.seed_private_candidate_archive import _VERSION as _SEED_VERSION
from scripts.ci.verify_benchmark_clone_boundary import _read_receipt
from scripts.db.canonical_cold_custody_placement import _target
from scripts.db.canonical_cold_private_seal import assert_promoted_clone, load_private_clone


SCHEMA = "qt.private_rehearsal_placement.v1"


def _clone_root(clone):
    directory = clone.socket.parent
    if (directory.name, directory.parent.name) != ("socket", "control"):
        raise ValueError("private_placement_requires_native_clone_socket_path")
    return directory.parent.parent


def _mount_backing(root, archive, device):
    with Path("/proc/self/mountinfo").open("rb") as stream:
        data = stream.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise RuntimeError("private_placement_mountinfo_limit")
    paths = {path: [] for path in (root, archive, root / "pgdata", root / "history")}
    guarded = root / "pgdata", root / "history", archive
    for line in os.fsdecode(data).splitlines():
        fields = line.split(" ")
        backing, path = (Path(re.sub(r"\\([0-7]{3})", lambda value: chr(int(value[1], 8)), part))
                         for part in fields[3:5])
        if path in paths:
            paths[path].append((fields[2], backing))
        elif any(path.is_relative_to(parent) for parent in guarded):
            raise ValueError("private_placement_unexpected_nested_mount")
    if any(len(paths[path]) != 1 or paths[path][0][0] != device for path in (root, archive)):
        raise ValueError("private_placement_requires_explicit_same_device_root_and_archive_mounts")
    base, archive_source = (paths[path][0][1] for path in (root, archive))
    if archive_source.is_relative_to(base) or base.is_relative_to(archive_source):
        raise ValueError("private_placement_archive_backing_overlaps_clone")
    for child in (root / "pgdata", root / "history"):
        if paths[child] and paths[child] != [(device, base / child.name)]:
            raise ValueError("private_placement_database_child_mount_alias")
    return {"root": str(base), "archive": str(archive_source)}


def _paths(clone, archive, filesystem_uuid, *, require_writable):
    root = _clone_root(clone)
    if (archive.is_relative_to(root) or root.is_relative_to(archive)):
        raise ValueError("private_placement_archive_overlaps_clone")
    roles = {"root": root, "pgdata": root / "pgdata", "history": root / "history",
             "wal": root / "pgdata" / "pg_wal", "archive": archive, "objects": archive / "objects"}
    identities = {name: _private_root(path)[1] for name, path in roles.items()}
    proof, device = clone.proof, clone.proof["device_id"]
    if (len(set(identities.values())) != len(roles)
            or any(f"{os.major(dev)}:{os.minor(dev)}" != device for dev, _ in identities.values())
            or (identities["pgdata"][1], identities["history"][1]) != (proof["pgdata_inode"], proof["tablespace_inode"])
            or ({identities["archive"][1], identities["objects"][1]} &
                {proof["archive_inode"], proof["archive_objects_inode"]})):
        raise ValueError("private_placement_directory_identity_changed")
    if require_writable and any(os.statvfs(roles[name]).f_flag & os.ST_RDONLY
                                for name in ("root", "pgdata", "history", "wal")):
        raise ValueError("private_placement_native_directories_require_writable_mounts")
    udev = Path(os.environ.get("QT_STORAGE_UDEV_ROOT", "/run/udev/data"))
    for path in (root, archive):
        observed = inspect_filesystem(path, expected_uuid=filesystem_uuid, udev_root=udev, require_writable=False)
        if (observed.device_id, observed.filesystem_uuid) != (device, filesystem_uuid):
            raise ValueError("private_placement_filesystem_identity_changed")
    links = roles["pgdata"] / "pg_tblspc"
    _private_root(links)
    link = links / str(proof["tablespace_oid"])
    if (not link.is_symlink() or link.readlink() != roles["history"]
            or link.resolve(strict=True) != roles["history"]):
        raise ValueError("private_placement_tablespace_link_changed")
    return {"directories": {name: {"path": str(path), "inode": identities[name][1]} for name, path in roles.items()},
            "backing": _mount_backing(root, archive, device)}


def assert_postmaster_file(conn, pgdata):
    """Check that SQL is connected to the exact mounted PostgreSQL data directory."""
    path = Path(pgdata) / "postmaster.pid"
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("private_placement_invalid_postmaster_file")
        local = stream.read(4097)
    observed = conn.scalar(text("SELECT pg_read_binary_file('postmaster.pid',0,4096)"))
    if not 0 < len(local) <= 4096 or local != bytes(observed):
        raise ValueError("private_placement_postmaster_file_changed")


def _registry(conn, clone, history_target_id):
    row = conn.execute(text("""
        SELECT t.root,t.filesystem_uuid,r.binding,r.tablespace_oid, s.spcname AS tablespace_name,
               pg_tablespace_location(s.oid) AS location, c.catalog_version_no AS catalog_version
        FROM public.portal_storage_targets AS t JOIN public.portal_storage_header_tablespaces AS r ON r.target_id=t.id
        JOIN pg_tablespace AS s ON s.oid=r.tablespace_oid CROSS JOIN pg_control_system() AS c
        WHERE t.id=:target_id AND r.database_identity=:database
          AND t.state='active' AND t.medium='hdd' AND t.roles ? 'history'
    """), {"target_id": history_target_id, "database": clone.proof["database_identity"]}).mappings().one_or_none()
    if (row is None or row["tablespace_oid"] != clone.proof["tablespace_oid"]
            or row["location"] != str(_clone_root(clone) / "history")):
        raise RuntimeError("private_placement_native_history_registration_changed")
    return row


def _stable_binding(clone, row, *, target_id, root, location):
    directory = str(Path(location) / f"PG_15_{row['catalog_version']}")
    return {"database_identity": clone.proof["database_identity"], "target_id": target_id,
            "filesystem_uuid": row["filesystem_uuid"], "tablespace_oid": row["tablespace_oid"],
            "tablespace_name": row["tablespace_name"], "tablespace_location": location,
            "target_root": root, "directory": directory, "server_directory": directory,
            "catalog_version": row["catalog_version"]}


def _bound_state(conn, clone, placement, *, require_writable):
    paths = _paths(clone, Path(placement["archive_root"]), placement["filesystem_uuid"],
                   require_writable=require_writable)
    if paths != placement["paths"]:
        raise RuntimeError("private_placement_paths_changed")
    assert_postmaster_file(conn, paths["directories"]["pgdata"]["path"])
    registered = _registry(conn, clone, placement["history_target_id"])
    binding = placement["native_binding"]
    if (registered["root"], registered["filesystem_uuid"], registered["binding"]) != (
            binding["target_root"], placement["filesystem_uuid"], binding):
        raise RuntimeError("private_placement_registration_changed")
    return _target(conn, clone.proof["database_identity"], placement["history_target_id"])


def load_private_placement(path, receipt_sha256, clone, *, clone_proof_sha256):
    receipt = _read_receipt(path, receipt_sha256)
    if (receipt.get("schema"), receipt.get("clone_proof_sha256"), receipt.get("database_identity"),
            receipt.get("accepted_commit_seq"), receipt.get("device_id")) != (
                SCHEMA, clone_proof_sha256, clone.proof["database_identity"], clone.proof["accepted_commit_seq"],
                clone.proof["device_id"]):
        raise ValueError("private_placement_receipt_different_clone")
    return receipt


def assert_private_placement(conn, *, clone, placement, require_writable=False):
    """Revalidate the private physical and SQL bindings in the caller's transaction."""
    _clone_root(clone)
    assert_promoted_clone(conn, clone)
    return _bound_state(conn, clone, placement, require_writable=require_writable)


def bind_private_placement(conn, *, clone, clone_proof_sha256, seed, seed_sha256, archive_root, filesystem_uuid,
                           history_target_id, original_root, original_location):
    """Install only the two recorded binding updates, under existing fences."""
    proof = clone.proof
    if (seed.get("schema"), seed.get("clone_proof_sha256"), seed.get("database_identity"),
            seed.get("accepted_commit_seq"), seed.get("device_id"), seed.get("source_archive_inode"),
            seed.get("source_objects_inode"), seed.get("catalog_inventory_sha256"), seed.get("catalog_objects"),
            seed.get("catalog_bytes")) != (
                _SEED_VERSION, clone_proof_sha256, proof["database_identity"], proof["accepted_commit_seq"],
                proof["device_id"], proof["archive_inode"], proof["archive_objects_inode"],
                proof["archive"]["inventory_sha256"], proof["archive"]["objects"], proof["archive"]["bytes"]):
        raise ValueError("private_placement_archive_seed_different_clone")
    paths = _paths(clone, archive_root, filesystem_uuid, require_writable=False)
    if any(paths["directories"][name]["inode"] != seed[field] for name, field in (
            ("archive", "archive_inode"), ("objects", "archive_objects_inode"))):
        raise ValueError("private_placement_archive_seed_inode_changed")
    if (not conn.in_transaction() or conn.connection.driver_connection.autocommit
            or conn.get_isolation_level() != "READ COMMITTED"):
        raise ValueError("private_placement_requires_read_committed_transaction")
    conn.exec_driver_sql("SET LOCAL lock_timeout='500ms'")
    conn.exec_driver_sql("SET LOCAL statement_timeout='10s'")
    for name in ("qt.storage.management.v1", _LIFECYCLE_LOCK_NAME):
        if not conn.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"), {"name": name}):
            raise RuntimeError("private_placement_storage_owner_busy")
    assert_promoted_clone(conn, clone)
    _assert_postmaster_file(conn, paths)
    conn.exec_driver_sql("LOCK TABLE public.portal_storage_targets, public.portal_storage_header_tablespaces "
                         "IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    registered = _registry(conn, clone, history_target_id)
    if registered["filesystem_uuid"] != filesystem_uuid:
        raise RuntimeError("private_placement_registered_filesystem_changed")
    original = _stable_binding(clone, registered, target_id=history_target_id,
                               root=str(original_root), location=str(original_location))
    replacement = _stable_binding(clone, registered, target_id=history_target_id,
                                  root=paths["directories"]["root"]["path"], location=registered["location"])
    observed = registered["root"], registered["binding"]
    if observed == (str(original_root), original):
        values = {"target": history_target_id, "database": proof["database_identity"],
                  "root": replacement["target_root"], "binding": json.dumps(replacement)}
        if conn.execute(text("UPDATE public.portal_storage_targets SET root=:root WHERE id=:target"), values).rowcount != 1:
            raise RuntimeError("private_placement_target_update_failed")
        if conn.execute(text("""
            UPDATE public.portal_storage_header_tablespaces SET binding=CAST(:binding AS jsonb)
            WHERE database_identity=:database AND target_id=:target
        """), values).rowcount != 1:
            raise RuntimeError("private_placement_binding_update_failed")
    elif observed != (replacement["target_root"], replacement):
        raise RuntimeError("private_placement_unreviewed_original_registration")
    body = {"schema": SCHEMA, "clone_proof_sha256": clone_proof_sha256, "archive_seed_sha256": seed_sha256,
            "database_identity": proof["database_identity"], "accepted_commit_seq": proof["accepted_commit_seq"],
            "device_id": proof["device_id"], "filesystem_uuid": filesystem_uuid, "history_target_id": history_target_id,
            "archive_root": str(archive_root), "paths": paths, "expected_original_binding": original,
            "native_binding": replacement}
    _bound_state(conn, clone, body, require_writable=False)
    return body | {"verification_sha256": archive_evidence_hash(body)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("proof", "archive-seed", "archive-root", "original-root", "original-location"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("proof-sha256", "archive-seed-sha256", "filesystem-uuid", "history-target-id"):
        parser.add_argument(f"--{name}", required=True)
    args = parser.parse_args(argv)
    try:
        if ({name for _, name in socket.if_nameindex()} != {"lo"}
                or not re.fullmatch(r"[A-Za-z0-9-]{4,128}", args.filesystem_uuid)
                or not args.original_root.is_absolute() or args.original_root == Path("/")
                or not args.original_location.is_relative_to(args.original_root)
                or ".." in args.original_root.parts or ".." in args.original_location.parts):
            raise ValueError("private_placement_requires_network_disabled_and_exact_scope")
        proof = _read_receipt(args.proof, args.proof_sha256)
        clone = load_private_clone(args.proof, args.proof_sha256, os.environ.get("PG_DSN"))
        if proof != clone.proof:
            raise ValueError("private_placement_clone_proof_changed")
        seed = _read_receipt(args.archive_seed, args.archive_seed_sha256)
        engine = create_engine(clone.url, poolclass=NullPool, hide_parameters=True, isolation_level="READ COMMITTED",
            connect_args={"connect_timeout": 5, "passfile": "/dev/null", "application_name": "qt_private_placement",
                          "options": "-c default_transaction_read_only=off -c statement_timeout=10000"})
        try:
            with engine.begin() as conn:
                result = bind_private_placement(conn, clone=clone, clone_proof_sha256=args.proof_sha256,
                    seed=seed, seed_sha256=args.archive_seed_sha256, archive_root=args.archive_root,
                    filesystem_uuid=args.filesystem_uuid, history_target_id=args.history_target_id,
                    original_root=args.original_root, original_location=args.original_location)
        finally:
            engine.dispose()
    except SQLAlchemyError as error:
        parser.exit(1, f"private_placement_sql_failed: {type(error).__name__}\n")
    except (KeyError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"private_placement_refused: {error}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
