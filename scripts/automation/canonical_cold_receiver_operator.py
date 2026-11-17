"""Run the independent receiver from a pinned plan and a private approval inbox."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
from tempfile import TemporaryDirectory
from time import monotonic, sleep

from core.storage_move_budget import MAX_MIGRATION_SECONDS
from portal.backend.service.storage.incremental_recovery import _binary, _private_bytes
from portal.backend.service.storage.recovery_copies import _sync
from scripts.automation.canonical_cold_named_recovery import (
    NamedRecoverySite, RecoverySitePolicy, _preflight_needs, _private_credential, _publish, _sign, _signer,
    prepare_replayed_receiver, recover_named_site, seal_replayed_receiver, verify_receiver_preflight,
)
from scripts.automation.canonical_cold_package import _hash_file, _pin, _unique_fields, load_approved_package
from scripts.automation.canonical_cold_receiver_archive import bind_replayed_archive_materializer
from scripts.automation.canonical_cold_receiver_postgres import ReceiverPostgres
from scripts.automation.canonical_cold_receiver_replay import replay_private_receiver
from scripts.automation.canonical_cold_receiver_runtime import bind_receiver_runtime
from scripts.automation.canonical_cold_recovery_checks import REPLAYED_PLAN_VERSION
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits, _file, _identity, _private_root, _stamp


PLAN_VERSION = "qt.canonical_cold_receiver_operator.v1"
APPROVAL_VERSION = "qt.canonical_cold_receiver_operator_approval.v1"
REPLAY_SELECTION_VERSION = "qt.canonical_cold_receiver_replay_selection.v1"
OFFER_VERSION = "qt.canonical_cold_replayed_receiver_offer.v1"
_APPROVAL = "approval.json"
_SELECTION = "replay-selection.json"
_FILE_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}")


def _fields(value, names, error):
    if not isinstance(value, dict) or value.keys() != set(names.split()):
        raise ValueError(error)


def _read_json(path, *, max_bytes, expected_hash=None):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("canonical_receiver_operator_private_path_required")
    digest, data = _hash_file(path, json_limit=max_bytes)
    if expected_hash is not None and digest != expected_hash:
        raise ValueError("canonical_receiver_operator_input_digest_changed")
    return json.loads(data, object_pairs_hook=_unique_fields)


def _private_binary(path, expected):
    path = _binary(path)
    if not _pin(expected):
        raise ValueError("canonical_receiver_operator_native_binary_pin_required")
    with path.open("rb") as stream:
        before = _stamp(os.fstat(stream.fileno()))
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != expected or before != _stamp(os.fstat(stream.fileno())) or before != _stamp(path.lstat()):
            raise ValueError("canonical_receiver_operator_native_binary_changed")
    return path


def _limits(values):
    _fields(values, "prepare_seconds approval_seconds ready_seconds recover_seconds max_custody_entries",
            "canonical_receiver_operator_invalid_lifecycle")
    if any(type(value) is not int or not 1 <= value <= maximum for value, maximum in (
        (values["prepare_seconds"], MAX_MIGRATION_SECONDS), (values["approval_seconds"], MAX_MIGRATION_SECONDS),
        (values["ready_seconds"], MAX_MIGRATION_SECONDS), (values["recover_seconds"], MAX_MIGRATION_SECONDS),
        (values["max_custody_entries"], 20_000_000),
    )):
        raise ValueError("canonical_receiver_operator_invalid_lifecycle")


def _load_site(plan):
    values = plan["site"]
    _fields(values, "receiving base_transfer_sha256 baseline_verification source_public_key receiver_public_key "
            "wal archive pending database max_archive_objects max_pending_entries", "canonical_receiver_operator_invalid_site")
    limits = {name: TransferLimits(**values[name]) for name in ("wal", "archive", "pending", "database")}
    policy = RecoverySitePolicy(**(values | limits | {"receiving": Path(values["receiving"])}))
    preflight = verify_receiver_preflight(plan["receiver_preflight"], policy.receiver_public_key)
    if preflight["site_policy"] != policy.preflight_scope():
        raise ValueError("canonical_named_recovery_preflight_policy_changed")
    return policy, preflight


def _publish_offer(staged, site, preflight, signer, *, plan_sha256, reserve_bytes, check):
    checkpoint = staged.checkpoint
    digest = checkpoint["catalog_inventory_sha256"]
    name = f"replayed-catalog-{digest}.jsonl"
    destination = site.receiving / name
    stamp = _stamp(staged.inventory.stat())
    if shutil.disk_usage(site.receiving).free < reserve_bytes + stamp[2]:
        raise ValueError("canonical_receiver_operator_catalog_space_insufficient")
    with TemporaryDirectory(prefix=".qt-offer-catalog-", dir=site.receiving) as work:
        temporary = Path(work) / name
        with temporary.open("xb") as stream:
            temporary.chmod(0o600)
            evidence = _file(staged.inventory, stamp, check, stream)
            stream.flush()
            os.fsync(stream.fileno())
        check()
        if evidence != {"bytes": stamp[2], "sha256": digest}:
            raise ValueError("canonical_receiver_operator_catalog_digest_changed")
        os.link(temporary, destination)
        _sync(site.receiving)
    offer = {"schema": OFFER_VERSION, "checkpoint": checkpoint, "site_plan_sha256": plan_sha256,
             "receiver_preflight_sha256": preflight["measurement_sha256"], "catalog": {"filename": name, **evidence}}
    offer_path = site.receiving / f"replayed-candidate-{checkpoint['verification_sha256']}.json"
    _publish(offer_path, _sign(offer, signer))
    return offer_path


def _approval(staged, policy, preflight, folder, *, wait_seconds, check):
    marker = folder / _APPROVAL
    deadline = monotonic() + wait_seconds
    while not os.path.lexists(marker):
        check()
        if monotonic() >= deadline:
            raise TimeoutError("canonical_receiver_operator_approval_deadline")
        sleep(0.2)
    approval = _read_json(marker, max_bytes=65_536)
    _fields(approval, "schema package_filename package_sha256 inventory_filename checkpoint_sha256",
            "canonical_receiver_operator_invalid_approval")
    checkpoint_sha256 = staged.checkpoint["verification_sha256"]
    if (approval["schema"] != APPROVAL_VERSION or approval["checkpoint_sha256"] != checkpoint_sha256
            or not _pin(approval["package_sha256"])
            or not all(isinstance(approval[key], str) and _FILE_NAME.fullmatch(approval[key])
                       for key in ("package_filename", "inventory_filename"))):
        raise ValueError("canonical_receiver_operator_approval_checkpoint_changed")
    path = folder / approval["package_filename"]
    package = load_approved_package(path, approved_package_hash=approval["package_sha256"],
        inventory=_read_json(folder / approval["inventory_filename"], max_bytes=65_536))
    binding = package["artifacts"]["independent_recovery"]
    recovery = _read_json(folder / binding["filename"], max_bytes=65_536, expected_hash=binding["sha256"])
    _fields(recovery, "schema base_transfer_sha256 baseline_verification_sha256 pair_receipt_sha256 "
            "pending_spool_scope_sha256 receiver_public_key receiver_preflight replayed_checkpoint_sha256 max_restart_segments",
            "canonical_receiver_operator_invalid_approved_recovery")
    scope = policy.preflight_scope()
    database = package["scope"]["target_database"]
    if (recovery["schema"] != REPLAYED_PLAN_VERSION or recovery["receiver_preflight"] != preflight
            or recovery["replayed_checkpoint_sha256"] != checkpoint_sha256
            or any(recovery[key] != scope[key] for key in ("base_transfer_sha256", "baseline_verification_sha256",
                                                         "pair_receipt_sha256", "receiver_public_key"))
            or (recovery["pending_spool_scope_sha256"] is not None) != preflight["body"]["pending_expected"]
            or (recovery["pending_spool_scope_sha256"] is not None and not _pin(recovery["pending_spool_scope_sha256"]))
            or f"{database['system_identifier']}/{database['database_oid']}" != scope["database_identity"]):
        raise ValueError("canonical_receiver_operator_approved_recovery_changed")
    site = NamedRecoverySite(**vars(policy), approved_package_sha256=approval["package_sha256"], receiver_preflight=preflight,
        replayed_checkpoint_sha256=checkpoint_sha256, max_restart_segments=recovery["max_restart_segments"])
    return site, package


def _replay_selection(path, policy, measured, check):
    while not os.path.lexists(path):
        check()
        sleep(0.2)
    selection = _read_json(path, max_bytes=8 * 1024**2)
    _fields(selection, "schema baseline_verification_sha256 receiver_preflight_sha256 timeline target_lsn replay_lsn prefixes",
            "canonical_receiver_operator_invalid_replay_selection")
    if (selection["schema"] != REPLAY_SELECTION_VERSION
            or selection["baseline_verification_sha256"] != policy.baseline_verification["verification_sha256"]
            or selection["receiver_preflight_sha256"] != measured["measurement_sha256"]
            or not isinstance(selection["prefixes"], list) or not selection["prefixes"]):
        raise ValueError("canonical_receiver_operator_replay_selection_scope_changed")
    for batch in selection["prefixes"]:
        _fields(batch, "after through prefix_sha256 previous_prefix_sha256", "canonical_receiver_operator_invalid_wal_prefix")
    check()
    return selection


def _final_headroom(site, measured):
    amounts = {role: need.copy() for role, need in measured["requirements"].items()}
    for role, restored in measured["database_targets"].items():
        amounts[role]["checkpoint_bytes"] -= restored["logical_bytes"]
        amounts[role]["entries"] -= restored["entries"]
    amounts["workspace"]["checkpoint_bytes"] -= measured["archive_restore_bytes"]
    amounts["workspace"]["entries"] -= measured["archive_restore_files"]
    restart_segments = site.max_restart_segments
    amounts["pgdata"]["checkpoint_bytes"] = max(
        amounts["pgdata"]["checkpoint_bytes"], restart_segments * site.baseline_verification["wal_segment_bytes"],
    )
    amounts["pgdata"]["entries"] = max(amounts["pgdata"]["entries"], restart_segments)
    mounts = measured["receiver_mounts"]
    by_device = _preflight_needs(mounts, amounts)
    for device, requirements in by_device.items():
        path = Path(mounts[requirements["roles"][0]]["path"])
        capacity = os.statvfs(path)
        available, entries = capacity.f_bavail * capacity.f_frsize, capacity.f_favail
        if available < requirements["required_bytes"] + requirements["reserve_bytes"] or entries < requirements["required_entries"]:
            raise RuntimeError(f"canonical_receiver_operator_final_capacity_insufficient: {device}")
        requirements.update(available_bytes=available, available_entries=entries)
    return by_device


def _device_reserve(measured, role):
    device = measured["receiver_mounts"][role]["device_id"]
    return measured["capacity_by_device"][device]["reserve_bytes"]


def run_operator(plan, *, plan_sha256, emit):
    """Prepare and recover inside a private process that keeps receiver custody."""
    _fields(plan, "schema site receiver_preflight paths tools credentials replay runtime lifecycle",
            "canonical_receiver_operator_invalid_plan")
    if plan["schema"] != PLAN_VERSION or not _pin(plan_sha256):
        raise ValueError("canonical_receiver_operator_invalid_plan")
    paths, tools, keys = plan["paths"], plan["tools"], plan["credentials"]
    replay, runtime, lifecycle = plan["replay"], plan["runtime"], plan["lifecycle"]
    _fields(paths, "workspace pgdata socket tablespaces archive_source archive_root approval_inbox",
            "canonical_receiver_operator_invalid_paths")
    _fields(tools, "postgres postgres_sha256 pg_controldata pg_controldata_sha256 pgbackrest restic",
            "canonical_receiver_operator_invalid_tools")
    _fields(keys, "receiver_signing_key database_key base_archive_key suffix_key pending_key",
            "canonical_receiver_operator_invalid_credentials")
    _fields(replay, "database role role_oid port tablespace_oids source_device source_inode archive_max_work_bytes",
            "canonical_receiver_operator_invalid_replay")
    archive_max_work_bytes = replay["archive_max_work_bytes"]
    if type(archive_max_work_bytes) is not int or not 0 < archive_max_work_bytes < 2**63:
        raise ValueError("canonical_receiver_operator_invalid_archive_work_budget")
    _fields(runtime, "source_checkout expected_revision expected_source_tree_sha256 timeout_seconds",
            "canonical_receiver_operator_invalid_runtime")
    _limits(lifecycle)
    policy, measured = _load_site(plan)
    signed_preflight = plan["receiver_preflight"]
    if (keys["pending_key"] is not None) != measured["pending_expected"]:
        raise ValueError("canonical_receiver_operator_pending_plan_changed")
    folders = {name: _private_root(Path(paths[name])) for name in ("workspace", "pgdata", "socket", "archive_root", "approval_inbox")}
    approval, approval_id = folders["approval_inbox"]
    workspace, _ = folders["workspace"]
    receiving, exchange_id = _private_root(policy.receiving)
    if folders["archive_root"][1][0] != folders["workspace"][1][0]:
        raise ValueError("canonical_receiver_operator_archive_device_changed")
    if any(left.is_relative_to(right) or right.is_relative_to(left)
           for left, right in ((approval, receiving), (approval, workspace), (approval, folders["archive_root"][0]))):
        raise ValueError("canonical_receiver_operator_approval_inbox_overlap")
    if os.path.lexists(approval / _APPROVAL):
        raise ValueError("canonical_receiver_operator_approval_before_offer")
    tablespaces = {name: _private_root(Path(path)) for name, path in paths["tablespaces"].items()}
    if tablespaces.keys() != replay["tablespace_oids"].keys():
        raise ValueError("canonical_receiver_operator_tablespace_pins_changed")
    roots = ((receiving, exchange_id), folders["workspace"], folders["pgdata"], folders["socket"], *tablespaces.values())
    pgdata, socket = folders["pgdata"][0], folders["socket"][0]
    signing_key = _private_credential(keys["receiver_signing_key"], receiving=receiving, workspace=workspace)
    signer = _signer(signing_key, policy.receiver_public_key, receiving)
    postgres = _private_binary(tools["postgres"], tools["postgres_sha256"])
    pg_controldata = _private_binary(tools["pg_controldata"], tools["pg_controldata_sha256"])
    pgbackrest, restic = _binary(tools["pgbackrest"]), _binary(tools["restic"])
    baseline = policy.baseline_verification
    native = ReceiverPostgres(postgres=postgres, postgres_sha256=tools["postgres_sha256"], pgdata=pgdata,
        workspace=workspace, socket=socket, port=replay["port"], database=replay["database"], role=replay["role"],
        pgbackrest=pgbackrest, settings=workspace / "pgbackrest.conf", expected_roots=roots)

    def check_paths():
        if any(_identity(path.lstat()) != identity for path, identity in (
                *roots, folders["archive_root"], (approval, approval_id))):
            raise RuntimeError("canonical_receiver_operator_mount_changed")

    def prepare(*, pgdata, base_restore, authenticate_prefix, check):
        def check_replay():
            check_paths()
            return check()

        emit({"phase": "awaiting_replay_selection", "inbox": str(approval)})
        selection = _replay_selection(approval / _SELECTION, policy, measured, check_replay)
        for batch in selection["prefixes"]:
            check_replay()
            authenticate_prefix(**batch)
        reserve = _device_reserve(measured, "workspace")
        if shutil.disk_usage(folders["archive_root"][0]).free < reserve + archive_max_work_bytes:
            raise RuntimeError("canonical_receiver_operator_archive_work_capacity_insufficient")
        materialize = bind_replayed_archive_materializer(pgdata=pgdata, pair_receipt=receiving / "complete.json",
            pair_receipt_sha256=baseline["pair_receipt_sha256"], base_restore=base_restore,
            source_archive=Path(paths["archive_source"]), source_identity=(replay["source_device"], replay["source_inode"]),
            output=folders["archive_root"][0], max_objects=policy.max_archive_objects,
            max_work_bytes=archive_max_work_bytes, reserve_bytes=reserve)
        evidence = replay_private_receiver(pgdata=pgdata, workspace=workspace, socket=socket, port=replay["port"],
            expected_roots=roots, expected_control_sha256=hashlib.sha256(
                _private_bytes(pgdata / "global" / "pg_control", limit=8192)).hexdigest(),
            pg_controldata=_private_binary(pg_controldata, tools["pg_controldata_sha256"]),
            database_identity=baseline["database_identity"], database=replay["database"],
            role=replay["role"], role_oid=replay["role_oid"], timeline=selection["timeline"], target_lsn=selection["target_lsn"],
            replay_lsn=selection["replay_lsn"], restore_command=native.restore_command, boot_paused=native.boot_paused,
            verify_archive=materialize, timeout_seconds=check_replay(), request_restartpoint=True,
            tablespaces={name: (path, replay["tablespace_oids"][name]) for name, (path, _) in tablespaces.items()})
        catalog = evidence["archive_receipt"]
        return evidence | {"archive_inventory": Path(catalog["catalog_inventory_path"]), "archive_objects": Path(catalog["suffix_root"])}

    check_paths()
    with prepare_replayed_receiver(site=policy, receiver_preflight=signed_preflight, signing_key=signing_key,
            pgbackrest=pgbackrest, restic=restic, database_key=keys["database_key"], base_archive_key=keys["base_archive_key"],
            workspace=workspace, pgdata=pgdata, tablespaces={name: path for name, (path, _) in tablespaces.items()},
            timeout_seconds=lifecycle["prepare_seconds"], max_custody_entries=lifecycle["max_custody_entries"],
            replay_before_approval=prepare) as staged:
        offer = _publish_offer(staged, policy, measured, signer, plan_sha256=plan_sha256,
                               reserve_bytes=_device_reserve(measured, "exchange"), check=check_paths)
        emit({"phase": "awaiting_approval", "offer": str(offer), "checkpoint_sha256": staged.checkpoint["verification_sha256"]})
        site, package = _approval(staged, policy, signed_preflight, approval, wait_seconds=lifecycle["approval_seconds"], check=check_paths)
        if package["scope"]["target_database"]["database"] != replay["database"]:
            raise ValueError("canonical_receiver_operator_approved_database_name_changed")
        witness = package["artifacts"]["replay_equivalence"]
        verify_runtime = bind_receiver_runtime(witness_path=approval / witness["filename"], witness_sha256=witness["sha256"],
            source_checkout=Path(runtime["source_checkout"]), expected_revision=runtime["expected_revision"],
            expected_source_tree_sha256=runtime["expected_source_tree_sha256"], receiving=receiving,
            timeout_seconds=runtime["timeout_seconds"])
        check_paths()
        capacity = _final_headroom(site, measured)
        with seal_replayed_receiver(staged=staged, site=site, ready_seconds=lifecycle["ready_seconds"]) as prepared:
            emit({"phase": "ready", "prepared_receiver_sha256": prepared.digest, "remaining_capacity_by_device": capacity})
            reply = recover_named_site(prepared=prepared, suffix_key=keys["suffix_key"], pending_key=keys["pending_key"],
                boot_isolated=native.boot_final, verify_original_runtime=verify_runtime,
                timeout_seconds=min(lifecycle["recover_seconds"], package["max_outage_seconds"]))
        emit({"phase": "recovered", "closure_sha256": reply["archive_closure"]["closure_sha256"]})
        return reply


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--plan-sha256", required=True)
    args = parser.parse_args(argv)
    if not _pin(args.plan_sha256):
        parser.error("--plan-sha256 requires 64 lowercase hexadecimal characters")
    plan = _read_json(args.plan, max_bytes=8 * 1024**2, expected_hash=args.plan_sha256)

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        run_operator(plan, plan_sha256=args.plan_sha256, emit=lambda event: print(json.dumps(event, sort_keys=True), flush=True))
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    main()
