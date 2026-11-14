"""Observe a held storage activation after its SQL reply is lost."""

import argparse
import json
import os
from pathlib import Path
from time import monotonic

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from market_data.fact_archive import archive_evidence_hash
from scripts.automation import storage_host_boundary as host
from scripts.automation.canonical_cold_host_hold import COMPLETION, HOLD, PAUSE_ORDER, VERSION
from scripts.automation.canonical_cold_host_inventory import assert_host_inventory
from scripts.automation.canonical_cold_package import _hash_file, _pin, _unique_fields, bind_execution, load_approved_package
from scripts.db.canonical_cold_commit_outcome import observe_activation_transaction


def observe_held_activation(control, *, state_root, inventory, package_path, approved_package_hash, timeout_seconds=30):
    """Report the exact transaction on the closed source; retain all hold authority."""
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ValueError("canonical_cold_commit_observer_invalid_timeout")
    root = Path(state_root)
    deadline = monotonic() + timeout_seconds

    def remaining():
        seconds = int(deadline - monotonic())
        if seconds < 1:
            raise TimeoutError("canonical_cold_commit_observer_deadline")
        return seconds

    with host.deployment_lock(root), host.docker_deadline(deadline):
        if (root / COMPLETION).exists():
            raise RuntimeError("canonical_cold_commit_observer_handoff_already_completed")
        path = root / HOLD
        hold = host.load_receipt(path)
        package = load_approved_package(package_path, approved_package_hash=approved_package_hash, inventory=inventory)
        stopped = [name for name in PAUSE_ORDER if inventory["services"][name]["running"]]
        phase = hold.get("phase")
        if (hold.get("schema") != VERSION or hold.get("package_hash") != approved_package_hash
                or hold.get("inventory_hash") != package["approved_inventory_hash"]
                or hold.get("project") != inventory["project"] or hold.get("source_revision") != inventory["source_revision"]
                or hold.get("stopped_services") != stopped or hold.get("inflight") is not None
                or phase not in ("activation_committing", "activation_precommit_verified", "activation_verified")):
            raise RuntimeError("canonical_cold_commit_observer_invalid_held_phase")
        evidence = hold["database_evidence"]
        committing = evidence["activation_committing"]
        fenced = evidence["database_fenced"]
        execution, execution_hash = bind_execution(
            package, approved_package_hash=approved_package_hash, source_commit_seq=committing["source_commit_seq"],
            provider_boundary=committing["provider_boundary"], independent_recovery=committing["independent_recovery"],
        )
        if execution_hash != committing["execution_hash"]:
            raise RuntimeError("canonical_cold_commit_observer_execution_changed")
        database = fenced["database"]
        if (database.get("allow_connections") is not True
                or any(database.get(key) != execution["target_database"][key]
                       for key in ("system_identifier", "database_oid", "database"))):
            raise RuntimeError("canonical_cold_commit_observer_fence_changed")
        transaction = committing["transaction"]
        precommit = evidence.get("activation_precommit_verified")
        if ((phase == "activation_committing") != (precommit is None)
                or (phase == "activation_verified" and evidence.get("activation_verified") != precommit)):
            raise RuntimeError("canonical_cold_commit_observer_precommit_phase_changed")
        if precommit is not None and (
            precommit.get("execution_hash") != execution_hash or precommit.get("xid") != transaction["xid"]
            or not _pin(precommit.get("witness_hash")) or not _pin(precommit.get("raw_mapping_index_receipt_hash"))
            or not isinstance(precommit.get("identity_retirement"), dict) or not precommit.get("wal_before_commit")
        ):
            raise RuntimeError("canonical_cold_commit_observer_precommit_changed")

        assert_host_inventory(root, expected=inventory, stopped_services=stopped, timeout_seconds=remaining())
        observed = observe_activation_transaction(
            control, database={**database, "allow_connections": False}, backend_pid=fenced["backend_pid"],
            transaction=transaction, precommit=None if precommit is None else precommit["wal_before_commit"],
            timeout_seconds=remaining(),
        )
        assert_host_inventory(root, expected=inventory, stopped_services=stopped, timeout_seconds=remaining())
        if host.load_receipt(path) != hold or (root / COMPLETION).exists():
            raise RuntimeError("canonical_cold_commit_observer_hold_changed")
        remaining()
    proof = {"schema": "qt.canonical_cold_commit_observation.v1", "hold_sha256": archive_evidence_hash(hold),
             "package_sha256": approved_package_hash, "execution_hash": execution_hash,
             "source_commit_seq": execution["source_commit_seq"], "database": database,
             "host_phase": phase, "precommit_verified": precommit is not None, **observed}
    return {**proof, "sha256": archive_evidence_hash(proof)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", required=True, type=Path)
    parser.add_argument("--inventory", required=True, type=Path, help="Private approved host-inventory JSON.")
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--approved-package-sha256", required=True)
    parser.add_argument("--timeout-seconds", default=30, type=int)
    args = parser.parse_args(argv)
    if not 1 <= args.timeout_seconds <= 60 or not _pin(args.approved_package_sha256):
        parser.error("Supply the approved SHA-256 and a timeout of 1–60 seconds.")
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        parser.error("The approved source PG_DSN is required.")
    url = make_url(dsn)
    if url.get_backend_name() != "postgresql" or not url.database:
        parser.error("PG_DSN must point to the approved PostgreSQL database.")
    _, content = _hash_file(args.inventory, json_limit=65_536)
    inventory = json.loads(content, object_pairs_hook=_unique_fields)
    engine = create_engine(
        url.set(database="postgres"), poolclass=NullPool, hide_parameters=True,
        connect_args={"connect_timeout": min(5, args.timeout_seconds), "options": "-c default_transaction_read_only=on"},
    )
    try:
        with engine.connect() as control:
            proof = observe_held_activation(
                control, state_root=args.state_root, inventory=inventory, package_path=args.package,
                approved_package_hash=args.approved_package_sha256, timeout_seconds=args.timeout_seconds,
            )
    finally:
        engine.dispose()
    print(json.dumps(proof, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
