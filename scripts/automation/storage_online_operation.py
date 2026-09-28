"""Fixed online migration sequence over the existing host and database owners.

This internal driver begins with admitted source-serving preparation and an
immutable worker request. It creates no state machine, receipt, retry policy or
release authority. Every mutation belongs to its existing phase owner.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import logging
import re
import time

from scripts.automation import storage_host_boundary as host
from scripts.automation import storage_online_launch as launch
from scripts.automation import storage_online_final as final
from scripts.automation import storage_online_runtime as runtime_owner

logger = logging.getLogger(__name__)
_FAMILIES = {"raw_archive_manifests", "fact_archive_manifests", "book_checkpoint_manifests"}
_CATALOGS = ("market.fact_archive_material_aliases", "market.fact_archive_canonical_dependencies")


@dataclass(frozen=True)
class OperationLimits:
    """Explicit admitted limits, never fixture defaults or a production estimate."""
    preparation_seconds: int
    final_seconds: int
    recovery_seconds: int
    runtime_seconds: int
    spool_max_bytes: int
    spool_max_entries: int
    spool_reserve_bytes: int
    repository_max_bytes: int
    repository_reserve_bytes: int
    recent_free_bytes: int

    def validate(self, request):
        values = vars(self)
        if any(type(value) is not int for value in values.values()):
            raise ValueError("storage_online_operation_limits_invalid")
        if (not 1 <= self.preparation_seconds <= 3600
                or not 1 <= self.final_seconds <= 96*3600
                or not 1 <= self.recovery_seconds <= 600
                or not 1 <= self.runtime_seconds <= 600
                or not 0 < self.spool_max_bytes <= 64*1024**3
                or not 1 <= self.spool_max_entries <= 4096
                or min(self.spool_reserve_bytes, self.repository_reserve_bytes, self.recent_free_bytes) < 0
                or self.repository_max_bytes <= 0
                or max(self.preparation_seconds, self.final_seconds) > request["resource_limits"]["movement_timeout_seconds"]):
            raise ValueError("storage_online_operation_limits_invalid")


def prepare_background(exchange, *, preparation_seconds):
    """Follow finite baseline phases, then fairly catch SQL/archive live tails.

    The channel and worker retain the original capture deadline on every call.
    Empty observations only end background preparation; final owners independently
    exclude publishers, drain, verify and decide whether a switch is admissible.
    """
    if type(preparation_seconds) is not int or not 1 <= preparation_seconds <= 3600:
        raise ValueError("storage_online_operation_preparation_limit_invalid")

    def phase(step, relation=None):
        result = exchange("prepare_step", step=step, relation=relation,
            max_duration_seconds=preparation_seconds)["result"]
        if result.get("committed") is not True:
            raise RuntimeError("storage_online_operation_preparation_unconfirmed")

    phase("catalog_history", "qt_fact_storage_cutover_v1.fact_versions")
    while True:
        result = exchange("sql_copy")["result"]
        if result["outcome"] == "identity_relocation_required":
            phase("identity_history")
        elif result["outcome"] == "raw_relocation_required":
            phase("raw_history")
        elif result["phase"] == "catch_up":
            break  # Do not chase a growing tail before finishing finite preparation.
        elif result["outcome"] not in {"page_budget_reached", "pass_time_budget_reached"}:
            raise RuntimeError("storage_online_operation_copy_progress_invalid")
    phase("identity_capture")
    after = None
    seen = set()
    while True:
        page = exchange("inspect_references", after=after)["result"]
        names = [row["relation"] for row in page["references"]]
        if (tuple(page["catalogs"]) != _CATALOGS or len(names) > 32
                or names != sorted(set(names)) or seen.intersection(names)
                or any(after is not None and name <= after for name in names)
                or len(seen)+len(names) > 8192
                or page["next_after"] is not None and (not names or page["next_after"] != names[-1])):
            raise RuntimeError("storage_online_operation_reference_page_invalid")
        for relation in names:
            phase("reference_prepare", relation)
            phase("reference_validate", relation)
        seen.update(names)
        after = page["next_after"]
        if after is None:
            break
    phase("reference_adopt")
    for relation in _CATALOGS:
        phase("catalog_history", relation)
    baselines = set()
    rounds = 0
    while True:
        archive = exchange("archive_copy")["result"]
        if archive["family"] not in _FAMILIES:
            raise RuntimeError("storage_online_operation_archive_family_invalid")
        if archive.get("baseline_complete") is True:
            baselines.add(archive["family"])
        proof = exchange("reprove")
        sql = exchange("sql_copy")
        rounds += 1
        if (baselines == _FAMILIES
                and set(proof["reproved_families_at_observation"]) == _FAMILIES
                and sql["result"]["outcome"] == "both_tails_observed_empty"):
            break
    logger.info("storage_online_background_prepared | references=%s rounds=%s switch_authorized=false", len(seen), rounds)
    return dict(reference_count=len(seen), tail_rounds=rounds, final_switch_authorized=False)


def run_prepared_operation(state_root, *, project, source_revision, source_image,
        image, request, inventory_path, descriptor_limit, memory_bytes,
        limits, keys_root, socket_volume, spool_destination):
    """One continuous live operation from prepared source through runtime readiness.

    Caller supplies reviewed private recipes, prepared roots and measured limits.
    Uncertainty propagates immediately, retaining phase intent. No automatic abort,
    reopen, reconnect, replay, marker deletion or repeated COMMIT occurs here.
    Runtime readiness is not encrypted-pair completion or ordinary relaunch authority.
    """
    if not isinstance(limits, OperationLimits):
        raise ValueError("storage_online_operation_limits_invalid")
    limits.validate(request)
    if not isinstance(source_image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", source_image):
        raise ValueError("storage_online_operation_source_image_invalid")
    state_root = launch._canonical(state_root)
    # The caller must prepare configuration before starting any migration work.
    # Full semantic and actual-container admission remains owned by runtime.
    host.load_receipt(state_root/runtime_owner.RUNTIME_RECIPE, max_bytes=524288)
    started = time.monotonic()
    try:
        with host.deployment_lock(state_root), ExitStack() as holds:
            with launch.launched_online_worker_locked(state_root, project=project,
                    source_revision=source_revision, image=image, request=request,
                    inventory_path=inventory_path, descriptor_limit=descriptor_limit,
                    memory_bytes=memory_bytes) as (worker, receipt):
                channel = host.OnlineWorkerChannel(worker,
                    deadline=time.monotonic()+receipt["deadline"]-time.time(),
                    command_seconds=max(40, limits.preparation_seconds))
                exchange = channel.exchange
                prepared = prepare_background(exchange, preparation_seconds=limits.preparation_seconds)
                paused = final.stop_online_source_locked(state_root, project=project,
                    source_revision=source_revision, worker_id=receipt["container_id"],
                    controller_id=channel.greeting["controller_id"], max_duration_seconds=limits.final_seconds)
                holds.enter_context(final.held_source_writers_locked(state_root, source_image=source_image))
                final.observe_source_drain_locked(state_root, exchange=exchange,
                    max_entries=limits.spool_max_entries)
                deadline = time.monotonic()+final._remaining(paused)-0.1
                final.copy_final_delta_locked(state_root, exchange=exchange, deadline=deadline, max_rounds=64)
                final.record_switch_entry_locked(state_root, deadline=deadline,
                    observe_worker=lambda **args: exchange("status", response_deadline=args["deadline"]))
                final.close_database_logins_locked(state_root, exchange=exchange)
                final.copy_final_delta_locked(state_root, exchange=exchange, deadline=deadline, max_rounds=64)
                outcome = final.commit_online_handoff_locked(state_root, exchange=exchange)
                if outcome.get("outcome") != "committed" or outcome.get("initial_policy_activated") is not True:
                    raise RuntimeError("storage_online_operation_commit_unconfirmed")
                exchange("close")
            # Launcher has independently reaped/verified the reader. Existing
            # recovery owners check that fact again before exposing any keys.
            final.prepare_recovery_database_locked(state_root, worker_process=worker,
                keys_root=keys_root, socket_volume=socket_volume,
                max_duration_seconds=limits.recovery_seconds)
            final.prepare_online_repositories_locked(state_root, worker_process=worker,
                max_bytes=limits.repository_max_bytes, reserve_bytes=limits.repository_reserve_bytes,
                recent_free_bytes=limits.recent_free_bytes, max_duration_seconds=limits.recovery_seconds)
            final.prepare_online_runtime_spool_locked(state_root, worker_process=worker,
                destination=spool_destination, max_bytes=limits.spool_max_bytes,
                max_entries=limits.spool_max_entries, reserve_bytes=limits.spool_reserve_bytes,
                max_duration_seconds=limits.recovery_seconds)
            runtime = final.activate_online_runtime_locked(state_root, worker_process=worker,
                max_duration_seconds=limits.runtime_seconds)
            return dict(background=prepared, runtime=runtime, elapsed_seconds=time.monotonic()-started,
                complete_backup_confirmed=False, ordinary_relaunch_authorized=False)
    except BaseException as exc:
        logger.error("storage_online_operation_stopped | project=%s error_type=%s intent_retained=true replay_authorized=false",
                     project, type(exc).__name__)
        raise
