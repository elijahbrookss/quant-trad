#!/usr/bin/env python3
"""Explicitly prepare encrypted recovery repositories; never deploy or activate.

Run as PostgreSQL's UID in its verified filesystem/PID namespace, using PG_DSN.
The inventory, private key files and independent key copy must already exist.
"""
import argparse
import json
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from core.settings import get_settings
from core.storage_inventory import read_storage_inventory
from portal.backend.service.storage.header_resources import observe_header_resources
from portal.backend.service.storage.incremental_recovery import (
    IncrementalRecoveryConfig, prepare_encrypted_repository, _private_bytes,
)
from portal.backend.service.storage.maintenance_runtime import _unique_fields
from portal.backend.service.storage.recovery_copies import _identity


def prepare(args):
    dsn = get_settings().database.dsn
    if not dsn:
        raise RuntimeError("PG_DSN_required")
    url = make_url(dsn)
    if url.get_backend_name() != "postgresql" or url.query:
        raise ValueError("plain_canonical_PG_DSN_required")
    config = IncrementalRecoveryConfig.from_dict(json.loads(
        _private_bytes(args.incremental_config, limit=16384), object_pairs_hook=_unique_fields))
    targets = read_storage_inventory(args.inventory)
    by_id = {target.target_id:target for target in targets}
    backup = by_id.get(args.backup_target)
    recent = by_id.get(args.recent_target)
    if (len(targets) != 2 or backup is None or recent is None
            or backup.medium != "hdd" or recent.medium != "ssd"
            or "backups" not in backup.roles or "recent" not in recent.roles):
        raise ValueError("recovery_preparation_requires_existing_ssd_and_hdd")
    if (type(args.timeout_seconds) is not int or not 1 <= args.timeout_seconds <= 86400
            or args.recent_free_bytes < 0):
        raise ValueError("recovery_preparation_budget_invalid")
    deadline = monotonic()+args.timeout_seconds
    engine = create_engine(url, poolclass=NullPool, connect_args={"connect_timeout":10})
    try:
        with engine.connect() as owner, owner.begin():
            owner.execute(text("SET LOCAL statement_timeout='15s'"))
            if not owner.scalar(text(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('qt.storage.management.v1',0))"
            )):
                raise RuntimeError("recovery_preparation_storage_busy")
            identity = _identity(owner)[0]
            if identity != args.expected_database_identity:
                raise RuntimeError("recovery_preparation_database_identity_mismatch")
            proof = observe_header_resources(owner, targets, pg_controldata=args.pg_controldata,
                                             timeout_seconds=min(30,args.timeout_seconds))
            directory = next(b["directory"] for b in proof.bindings if b["role"] == "database")
            if (proof.database_identity != identity or Path(directory) != config.pg_path.resolve(strict=True)
                    or any(b["target_id"] != recent.target_id for b in proof.bindings
                           if b["role"] in ("database","database_default","wal"))):
                raise RuntimeError("recovery_preparation_serving_namespace_mismatch")
            floors = {backup.target_id:args.reserve_bytes, recent.target_id:args.recent_free_bytes}
            originals = {target.target_id:target.inspect(require_writable=True) for target in targets}
            def resources():
                if monotonic() >= deadline:
                    raise RuntimeError("recovery_preparation_time_budget_exceeded")
                owner.execute(text("SET LOCAL statement_timeout='1s'"))
                if not owner.scalar(text("""
                    WITH key AS (SELECT hashtextextended('qt.storage.management.v1',0) AS value)
                    SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
                      AND pid=pg_backend_pid() AND mode='ExclusiveLock' AND granted AND objsubid=1
                      AND classid::bigint=((key.value>>32)&4294967295)
                      AND objid::bigint=(key.value&4294967295))
                """)):
                    raise RuntimeError("recovery_preparation_storage_ownership_lost")
                for target in targets:
                    current, before = target.inspect(require_writable=True), originals[target.target_id]
                    if (current.device_id != before.device_id or current.path != before.path
                            or current.available_bytes < floors[target.target_id]):
                        raise RuntimeError("recovery_preparation_capacity_or_filesystem_changed")
            resources()
            return prepare_encrypted_repository(incremental=config, connection_url=url,
                archiver_config=args.archiver_config, target=backup, database_identity=identity,
                max_bytes=args.max_bytes, reserve_bytes=args.reserve_bytes,
                timeout_seconds=max(1,int(deadline-monotonic())), check_resources=resources)
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--incremental-config", type=Path, required=True)
    parser.add_argument("--archiver-config", type=Path, required=True)
    parser.add_argument("--recent-target", required=True)
    parser.add_argument("--backup-target", required=True)
    parser.add_argument("--expected-database-identity", required=True)
    parser.add_argument("--pg-controldata", type=Path, required=True)
    parser.add_argument("--max-bytes", type=int, required=True)
    parser.add_argument("--reserve-bytes", type=int, required=True)
    parser.add_argument("--recent-free-bytes", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args), sort_keys=True))


if __name__ == "__main__":
    main()
