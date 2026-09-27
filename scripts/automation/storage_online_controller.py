"""Process-local online migration controller; no production launch authority.

An admitted host owns preparation, publisher drain and runtime activation.
The bounded pipe channel exposes background work and a retained rollback fence,
never host restart or database COMMIT authority. One context
owns destination leases until close, including an internal final database
commit/reconciliation. Never reconstruct authority from a serialized reply.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from copy import deepcopy
import json
import logging
import math
import os
from pathlib import Path
import select
from time import monotonic, sleep
from uuid import uuid4

from sqlalchemy import text

from market_data.archive_namespace import archive_namespace

from portal.backend.service.storage.header_resource_claims import _limits
from scripts.db import archive_root_v2_copy as archives
from scripts.db import archive_root_v2_online as archive_online
from scripts.db import fact_header_v2_capture as capture
from scripts.db import fact_header_v2_copy as headers
from scripts.db import fact_header_v2_online as online
from scripts.db import fact_header_v2_online_proof as protection
from scripts.db import fact_header_v2_handoff as handoff
from scripts.db import fact_header_v2_cancel as cancellation
from scripts.db.archive_file_v2_proof import ArchiveFileProof

logger = logging.getLogger(__name__)
_LOCK = "qt.storage.online.controller.v1"
_ROLLBACK_OPERATIONS = {"rollback_fence_begin", "rollback_fence_check", "rollback_fence_end"}
_SESSION_OPERATIONS = {"final_session_begin", "final_session_check", "final_session_quiesce"}
_OPERATIONS = _SESSION_OPERATIONS | _ROLLBACK_OPERATIONS | {"status", "sql_copy", "archive_copy", "reprove", "prepare_step", "source_drain", "final_delta", "inspect_outcome", "cancel", "close"}


class OnlineController:
    """One prepared attempt, fixed inputs, main-thread leases and DB ownership.

    Construct only after the source-read identity transition. Preparation and
    physical relocations are separately admitted boundaries, not implicit work
    hidden behind a status request. The host must bind image/namespace/mounts
    and admit resources before this internal component is used.
    """

    def __init__(self, engine, *, placement, policy, resource_limits, source_root,
                 destination_root, expected_started_at, max_objects, max_bytes,
                 max_page_bytes, page_rows=128, command_seconds=30):
        if type(command_seconds) is not int or not 1 <= command_seconds <= 60:
            raise ValueError("storage_online_command_budget_invalid")
        if type(page_rows) is not int or not 1 <= page_rows <= 256:
            raise ValueError("storage_online_page_rows_invalid")
        if type(max_page_bytes) is not int or max_page_bytes <= 0:
            raise ValueError("storage_online_page_bytes_invalid")
        if not isinstance(expected_started_at, str) or not expected_started_at:
            raise ValueError("storage_online_original_attempt_required")
        self.engine, self.placement, self.policy = engine, placement, policy
        self._admitted_limits = deepcopy(_limits(resource_limits, migration=True))
        self.limits = deepcopy(self._admitted_limits)
        self.limits["movement_timeout_seconds"] = min(
            self.limits["movement_timeout_seconds"], command_seconds)
        self.source_root, self.destination_root = Path(source_root), Path(destination_root)
        self.expected_started_at = expected_started_at
        self.max_objects, self.max_bytes = max_objects, max_bytes
        self.max_page_bytes, self.page_rows = max_page_bytes, page_rows
        self.controller_id = uuid4().hex
        self.state = "new"
        self._stack = None
        self._owner = None
        self.proof = None
        self._capture = None
        self._source = None
        self._family = 0
        self._reprove_family = 0
        self._cursors = {name: "" for name in archives.FAMILIES}
        self._reproved = set()
        self._final_deadline = None
        self._final_connection = None
        self._final_connection_entered = False
        self._jobs_stop_requested = False
        self._jobs_stopped = False
        self._jobs_catalog = None
        self._archive_namespace_check = None
        self._rollback_context = self._rollback_check = None
        self._sequence = 0
        self._last_request = self._last_reply = None

    def __enter__(self):
        if self.state != "new":
            raise RuntimeError("storage_online_new_controller_required")
        self._stack = ExitStack()
        try:
            self._owner = self.engine.connect()
            # A session lock survives individual page commits. Always invalidate
            # this dedicated connection on exit: never return a locked session
            # to a pool, and never transparently reconnect after losing ownership.
            self._stack.callback(self._release_owner)
            with self._owner.begin():
                self._owner.exec_driver_sql("SET LOCAL statement_timeout='5s'")
                if not self._owner.scalar(text(
                        "SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": _LOCK}):
                    raise RuntimeError("storage_online_controller_busy")
                self._pid = self._owner.scalar(text("SELECT pg_backend_pid()"))
                with capture.migration_step(self._owner, 30):
                    observed = capture.inspect_capture(self._owner)
                    if observed["started_at"] != self.expected_started_at:
                        raise RuntimeError("storage_online_attempt_binding_changed")
                    saved = headers._inspect_progress(self._owner)["placement"]
                    if saved is None:
                        raise RuntimeError("storage_online_fixed_placement_required")
                    if handoff.physical._restore(saved["plan"]) != self.placement:
                        raise RuntimeError("storage_online_placement_changed")
                    protection.inspect_protection(self._owner)
                    archive_online._inspect(self._owner, self.source_root, self.destination_root)
                    self._capture = self._capture_row(self._owner)
                    remaining = capture.capture_remaining_seconds(self._owner)
                    self._source_device = saved["recent_device"]
                    self._source = archives._root(self.source_root, self._source_device)[1]
            self.proof = self._stack.enter_context(ArchiveFileProof(
                self.destination_root, max_files=self.max_objects, max_bytes=self.max_bytes,
                deadline=monotonic()+remaining))
            self.state = "background"
            logger.info("storage_online_controller_started | controller_id=%s attempt=%s",
                        self.controller_id, self.expected_started_at)
            return self
        except BaseException:
            self.state = "failed"
            self._stack.close()
            raise

    def _release_owner(self):
        if self._owner is not None:
            try:
                self._owner.invalidate()
            finally:
                self._owner.close()

    @staticmethod
    def _capture_row(conn):
        return dict(conn.execute(text(f"SELECT * FROM {capture.STATE}")).mappings().one())

    def _ownership(self, *, deadline=None):
        if self._owner is None or self._owner.closed or self._owner.invalidated:
            raise RuntimeError("storage_online_controller_ownership_lost")
        seconds = 5 if deadline is None else min(5, deadline-monotonic())
        if seconds <= 0:
            raise RuntimeError("storage_online_sql_drain_deadline_expired")
        with self._owner.begin():
            self._owner.execute(text("SELECT set_config('statement_timeout',:value,true)"),
                                {"value": str(max(1, math.floor(seconds*1000)))})
            if self._owner.scalar(text("SELECT pg_backend_pid()")) != self._pid:
                raise RuntimeError("storage_online_controller_ownership_lost")
        if deadline is not None and monotonic() >= deadline:
            raise RuntimeError("storage_online_sql_drain_deadline_expired")

    def check(self):
        if self.state not in {"background", "commit_unknown", "committed", "rolled_back", "resume_fenced", "aborted"}:
            raise RuntimeError("storage_online_controller_not_active")
        if self.state == "aborted" and (self._final_deadline is None or monotonic() >= self._final_deadline):
            raise RuntimeError("storage_online_terminal_deadline_expired")
        if self.state == "resume_fenced":
            self._rollback_check()
        self._ownership()
        self.proof.check()
        if self._archive_namespace_check is not None:
            self._archive_namespace_check()
        if archives._root(self.source_root, self._source_device)[1] != self._source:
            raise RuntimeError("storage_online_source_changed")

    @contextmanager
    def final_database_session(self, *, deadline):
        """Retain one SQL session for a separately admitted final window.

        Internal only. This opens no pipe command, changes no login setting and
        grants no publisher exclusion. The host retains its original intent,
        proof and deadline. Once entered, a lost session never reconnects.
        Finish background and tail work before this context.
        """
        if self.state != "background" or self._final_connection_entered:
            raise RuntimeError("storage_online_final_session_state_invalid")
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not 0 < deadline-monotonic() <= self._admitted_limits["movement_timeout_seconds"]
                or deadline > self.proof.deadline
                or (self._final_deadline is not None and deadline != self._final_deadline)):
            raise ValueError("storage_online_final_session_deadline_invalid")
        self._final_deadline = deadline
        self._admit_attempt()
        if not monotonic() < deadline <= self.proof.deadline:
            raise ValueError("storage_online_final_session_deadline_invalid")
        self._final_connection_entered = True
        with self.engine.connect() as conn:
            self._final_connection = conn
            try:
                with conn.begin():
                    with capture._bounded_step(conn, math.ceil(deadline-monotonic())) as shorten:
                        shorten(deadline-monotonic())
                        self._final_pid = conn.scalar(text("SELECT pg_backend_pid()"))
                yield
            finally:
                # Discard even after normal completion; never pool a session
                # that supplied final-window identity or uncertain outcome.
                try:
                    conn.invalidate()
                finally:
                    self._final_connection = None

    def final_session_observation(self, *, deadline):
        """Fresh retained-session identity/capture only; no gate or COMMIT authority."""
        if (not self._final_connection_entered or deadline != self._final_deadline
                or monotonic() >= deadline):
            raise RuntimeError("storage_online_final_session_deadline_invalid")
        self._ownership(deadline=deadline)
        self._admit_attempt()
        with self._database_connection() as conn, conn.begin():
            with capture._bounded_step(conn, math.ceil(deadline-monotonic())) as shorten:
                shorten(deadline-monotonic())
                observed = conn.scalar(text("SELECT json_build_object("
                    "'cluster',(SELECT system_identifier::text FROM pg_control_system()),"
                    "'oid',d.oid::bigint,'name',d.datname,'allow_connections',d.datallowconn) "
                    "FROM pg_database d WHERE datname=current_database()"))
                captured = conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1"))
        self._ownership(deadline=deadline)
        return {"database": observed, "capture": captured, "backend_pid": self._final_pid,
                "owner_pid": self._pid, "database_switch_authorized": False,
                "collection_resume_authorized": False, "runtime_activation_authorized": False}

    def quiesce_final_database_jobs(self, *, deadline):
        """Stop pinned Timescale jobs only behind the host's durable login gate.

        Host must have recorded login_closing before this internal operation.
        The stop request is not transactional; loss remains unresolved, never
        retried or automatically reversed. Job definitions are preserved. This
        does not exclude host/archive publishers or authorize COMMIT/restart.
        """
        observed = self.final_session_observation(deadline=deadline)
        if observed["database"]["allow_connections"] or self._jobs_stop_requested:
            raise RuntimeError("storage_online_jobs_closed_gate_required")
        with self._database_connection() as conn, conn.begin():
            with capture._bounded_step(conn, math.ceil(deadline-monotonic())) as shorten:
                shorten(deadline-monotonic())
                self._require_job_environment(conn)
                self._jobs_catalog = self._job_catalog(conn)
                self._jobs_stop_requested = True  # Before a nontransactional request.
                if conn.scalar(text("SELECT _timescaledb_functions.stop_background_workers()")) is not True:
                    raise RuntimeError("storage_online_jobs_stop_unconfirmed")
        while True:
            self._ownership(deadline=deadline)
            with self._database_connection() as conn, conn.begin():
                with capture._bounded_step(conn, math.ceil(deadline-monotonic())) as shorten:
                    shorten(deadline-monotonic())
                    self._require_job_environment(conn)
                    if self._job_catalog(conn) != self._jobs_catalog:
                        raise RuntimeError("storage_online_job_definitions_changed")
                    conn.exec_driver_sql("SELECT pg_stat_clear_snapshot()")
                    remaining = conn.scalar(text("""SELECT count(*) FROM pg_stat_activity
                        WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
                          AND pid NOT IN (pg_backend_pid(), :owner_pid)"""), {"owner_pid": self._pid})
                    if not remaining:
                        self._jobs_stopped = True
                        break
            sleep(min(.1, max(0, deadline-monotonic())))
        result = self.final_session_observation(deadline=deadline)
        return {**result, "database_jobs_stopped": True, "job_definitions_preserved": True}

    @staticmethod
    def _job_catalog(conn):
        # Keep job arguments/private configuration inside PostgreSQL. This is a
        # definition-drift check, not a cryptographic provenance certificate.
        count, size = conn.execute(text("""SELECT count(*),
            coalesce(sum(octet_length(to_jsonb(j)::text)),0) FROM _timescaledb_config.bgw_job j""")).one()
        if count > 256 or size > 1024*1024:
            raise RuntimeError("storage_online_job_inventory_bound_exceeded")
        fingerprint = conn.scalar(text("""SELECT
            md5(coalesce(jsonb_agg(to_jsonb(j) ORDER BY id)::text, '[]'))
            FROM _timescaledb_config.bgw_job j"""))
        return count, fingerprint

    @staticmethod
    def _require_job_environment(conn, *, allow_connections=False):
        extensions = dict(conn.execute(text("SELECT extname, extversion FROM pg_extension")).all())
        if (extensions.get("timescaledb") != "2.14.2" or extensions.get("plpgsql") != "1.0"
                or set(extensions)-{"timescaledb", "plpgsql", "pg_stat_statements", "pgcrypto", "pg_buffercache"}
                or ("pg_buffercache" in extensions and extensions["pg_buffercache"] != "1.3")
                or ("pgcrypto" in extensions and extensions["pgcrypto"] != "1.3")
                or ("pg_stat_statements" in extensions and extensions["pg_stat_statements"] != "1.10")
                or set(conn.scalar(text("SHOW shared_preload_libraries")).replace(" ", "").split(","))
                    not in ({"timescaledb"}, {"timescaledb", "pg_stat_statements"})):
            raise RuntimeError("storage_online_database_job_environment_unqualified")
        if conn.scalar(text("""SELECT pg_is_in_recovery()
            OR (SELECT datallowconn FROM pg_database WHERE datname=current_database()) IS DISTINCT FROM :allow_connections
            OR EXISTS(SELECT 1 FROM pg_subscription WHERE subdbid=(
                SELECT oid FROM pg_database WHERE datname=current_database()))
            OR EXISTS(SELECT 1 FROM pg_replication_slots)
            OR EXISTS(SELECT 1 FROM pg_stat_replication)"""), {"allow_connections": allow_connections}):
            raise RuntimeError("storage_online_database_job_gate_or_replication_unqualified")

    @contextmanager
    def _database_connection(self):
        if not self._final_connection_entered:
            with self.engine.connect() as conn:
                yield conn
            return
        conn = self._final_connection
        if (conn is None or conn.closed or conn.invalidated or conn.in_transaction()
                or conn.connection.driver_connection.get_backend_pid() != self._final_pid):
            raise RuntimeError("storage_online_final_session_lost")
        yield conn

    def _admit_attempt(self):
        self.check()
        with self._database_connection() as conn, conn.begin():
            with capture.migration_step(conn, 30, deadline=self._final_deadline):
                if self._capture_row(conn) != self._capture:
                    raise RuntimeError("storage_online_attempt_binding_changed")
                # A later wall-clock adjustment can shrink but never extend the
                # process's already admitted monotonic ceiling.
                self.proof.deadline = min(self.proof.deadline,
                                         monotonic()+capture.capture_remaining_seconds(conn))

    def status(self):
        return {"schema_version": "qt.storage_online_controller.v1",
                "controller_id": self.controller_id, "state": self.state,
                "last_sequence": self._sequence,
                "reproved_families_at_observation": sorted(self._reproved),
                "background_hashed_bytes": self.proof.hashed_bytes,
                "bound_final_deadline": self._final_deadline,
                "migration_ready": False, "final_switch_authorized": False,
                "collection_resume_authorized": False}

    def _archive_page(self, reprove):
        families = tuple(archives.FAMILIES)
        index = self._reprove_family if reprove else self._family
        family = families[index % len(families)]
        options = dict(family=family, source_root=self.source_root,
            destination_root=self.destination_root, page_rows=self.page_rows,
            max_page_bytes=self.max_page_bytes, policy=self.policy,
            resource_limits=self.limits, file_proof=self.proof)
        if reprove:
            report = archives.copy_archive_page(
                self.engine, after_id=self._cursors[family], **options)
            self._cursors[family] = report["next_after_id"]
            if not report["page_objects"]:
                self._reproved.add(family)
            self._reprove_family += 1
        else:
            report = archive_online.copy_page(self.engine, **options)
            self._family += 1
        # No per-file keys, resource internals, DSN or unbounded inventory on wire.
        result = {key: report[key] for key in (
            "family", "page_objects", "copied_objects", "reused_objects", "verified_bytes")}
        if not reprove:
            result["baseline_complete"] = report["baseline_complete"]
        return result

    def command(self, request):
        fields = {"controller_id", "sequence", "operation"}
        preparing = isinstance(request, dict) and request.get("operation") == "prepare_step"
        draining = isinstance(request, dict) and request.get("operation") == "source_drain"
        finalizing = isinstance(request, dict) and request.get("operation") == "final_delta"
        inspecting = isinstance(request, dict) and request.get("operation") == "inspect_outcome"
        fencing = isinstance(request, dict) and request.get("operation") in _ROLLBACK_OPERATIONS
        session = isinstance(request, dict) and request.get("operation") in _SESSION_OPERATIONS
        if finalizing or inspecting or fencing or session:
            fields |= {"deadline"}
        if draining:
            fields |= {"deadline", "max_entries"}
        if preparing:
            fields |= {"step", "relation", "max_duration_seconds"}
        if (not isinstance(request, dict)
                or set(request) != fields
                or request["controller_id"] != self.controller_id
                or type(request["sequence"]) is not int
                or not 1 <= request["sequence"] <= 2**53
                or not isinstance(request["operation"], str)
                or request["operation"] not in _OPERATIONS):
            raise ValueError("storage_online_command_invalid")
        operation = request["operation"]
        if finalizing and (type(request["deadline"]) not in (int, float)
                or not math.isfinite(request["deadline"])
                or not 0 < request["deadline"]-monotonic() <= self._admitted_limits["movement_timeout_seconds"]
                or request["deadline"] > self.proof.deadline):
            raise ValueError("storage_online_final_delta_command_budget_invalid")
        if inspecting and (type(request["deadline"]) not in (int, float)
                or not math.isfinite(request["deadline"])
                or not 0 < request["deadline"]-monotonic() <= min(5, self.limits["movement_timeout_seconds"])
                or (self._final_deadline is not None and request["deadline"] > self._final_deadline)):
            raise ValueError("storage_online_outcome_command_budget_invalid")
        if (fencing or session) and (type(request["deadline"]) not in (int, float)
                or not math.isfinite(request["deadline"])
                or self._final_deadline is None or request["deadline"] != self._final_deadline
                or not 0 < request["deadline"]-monotonic() <= self._admitted_limits["movement_timeout_seconds"]):
            raise ValueError("storage_online_final_session_command_deadline_invalid" if session
                             else "storage_online_rollback_command_deadline_invalid")
        if draining:
            if (type(request["deadline"]) not in (int, float)
                    or not math.isfinite(request["deadline"])
                    or not 0 < request["deadline"]-monotonic() <= self.limits["movement_timeout_seconds"]
                    or request["deadline"] > self.proof.deadline
                    or type(request["max_entries"]) is not int
                    or not 1 <= request["max_entries"] <= 1_000_000):
                raise ValueError("storage_online_spool_command_budget_invalid")
        if preparing:
            step, relation = request["step"], request["relation"]
            relation_step = step in {"reference_prepare", "reference_validate"} if isinstance(step, str) else False
            if (not isinstance(step, str) or step not in online._PREPARATION_STEPS
                    or (relation_step and (not isinstance(relation, str) or not 1 <= len(relation) <= 256))
                    or (not relation_step and relation is not None)
                    or type(request["max_duration_seconds"]) is not int
                    or not 1 <= request["max_duration_seconds"] <= self._admitted_limits["movement_timeout_seconds"]):
                raise ValueError("storage_online_preparation_command_invalid")
        if request["sequence"] == self._sequence and request == self._last_request:
            # Same-process transport retry never executes a mutation twice.
            # It is not a restart/resume token and never revives dead proof.
            if self.state not in {"closed", "cancelled"}:
                self.check()
            if session:
                raise RuntimeError("storage_online_final_session_fresh_sequence_required")
            if fencing:
                raise RuntimeError("storage_online_rollback_fresh_sequence_required")
            if draining or inspecting:
                raise RuntimeError("storage_online_observation_fresh_sequence_required" if inspecting
                                   else "storage_online_spool_fresh_sequence_required")
            return deepcopy(self._last_reply)
        readable = operation in {"inspect_outcome", "close"} and self.state in {"commit_unknown", "committed", "rolled_back", "aborted"}
        rollback_allowed = (operation == "rollback_fence_begin" and self.state in {"background", "commit_unknown", "rolled_back"}
            or operation in {"rollback_fence_check", "rollback_fence_end", "close"} and self.state == "resume_fenced")
        if request["sequence"] != self._sequence+1 or (self.state != "background" and not readable and not rollback_allowed):
            raise RuntimeError("storage_online_command_sequence_or_state_invalid")
        if self._final_deadline is not None and operation not in _SESSION_OPERATIONS | _ROLLBACK_OPERATIONS | {"status", "source_drain", "final_delta", "inspect_outcome", "close", "cancel"}:
            raise RuntimeError("storage_online_final_background_work_refused")
        if self._final_connection_entered and operation not in {"status", "inspect_outcome", "close", "final_session_check", "final_session_quiesce", "final_delta"} | _ROLLBACK_OPERATIONS:
            raise RuntimeError("storage_online_final_session_background_work_refused")
        result = {}
        try:
            if operation == "close":
                self._ownership()
                self._abandon_rollback_channel()
                self.state = "closed"
            elif operation == "cancel":
                # Explicit terminal cleanup may be needed after proof/attempt
                # expiry; do not renew either clock or require healthy leases.
                self._ownership()
                with self.engine.begin() as conn:
                    cancellation.cancel_attempt(conn,
                        expected_started_at=self.expected_started_at,
                        source_root=self.source_root, destination_root=self.destination_root,
                        timeout_seconds=min(30, self.limits["movement_timeout_seconds"]))
                self.state = "cancelled"
                result = {"attempt_cancelled": True, "source_preserved": True}
            elif session:
                if operation == "final_session_begin":
                    self._stack.enter_context(self.final_database_session(deadline=request["deadline"]))
                result = (self.quiesce_final_database_jobs(deadline=request["deadline"])
                          if operation == "final_session_quiesce"
                          else self.final_session_observation(deadline=request["deadline"]))
            elif fencing:
                result = self._rollback_channel(operation, deadline=request["deadline"])
            elif inspecting:
                result = self.inspect_outcome(deadline=request["deadline"])
            elif finalizing:
                result = self.final_delta(deadline=request["deadline"])
            elif draining:
                # Read-only and explicit: the same live host supplies its already
                # decreasing final deadline. It cannot widen page allowances or
                # create switch/drain authority from this instantaneous result.
                from scripts.automation.storage_online_drain import inspect_spool
                result = inspect_spool(self.source_root.parent,
                    deadline=request["deadline"], max_entries=request["max_entries"],
                    check=self.proof.check)
                self.check()
                if monotonic() >= request["deadline"]:
                    raise RuntimeError("storage_online_spool_deadline_expired")
            else:
                self._admit_attempt()
                if operation == "sql_copy":
                    report = online.copy_pass(self.engine, placement=self.placement,
                        policy=self.policy, resource_limits=self.limits, page_rows=self.page_rows,
                        max_pages=2, max_duration_seconds=self.limits["movement_timeout_seconds"])
                    result = {key: report[key] for key in
                              ("outcome", "phase", "committed_pages", "verified_page_rows")}
                elif operation in {"archive_copy", "reprove"}:
                    result = self._archive_page(operation == "reprove")
                elif preparing:
                    # Explicit phase requests use their admitted allowance, never
                    # silently widen page/status commands or the original attempt.
                    result = online.preparation_step(self.engine, step=request["step"],
                        relation=request["relation"], placement=self.placement,
                        policy=self.policy, resource_limits=self._admitted_limits,
                        expected_started_at=self.expected_started_at, page_rows=self.page_rows,
                        max_duration_seconds=request["max_duration_seconds"])
                self.check()
        except BaseException as exc:
            self._abandon_rollback_channel()
            if self.state != "committed":
                self.state = "failed"
            # Static guard codes are safe diagnostics; arbitrary database error
            # text may contain connection or source data and is never logged.
            code = str(exc)
            guard = code if code.startswith("storage_") and all(c.islower() or c == "_" for c in code) else "unclassified"
            logger.error("storage_online_controller_failed | controller_id=%s operation=%s error_type=%s guard=%s",
                         self.controller_id, operation, type(exc).__name__, guard)
            raise
        self._sequence = request["sequence"]
        reply = {**self.status(), "operation": operation, "result": result}
        self._last_request, self._last_reply = deepcopy(request), deepcopy(reply)
        return reply

    def _abandon_rollback_channel(self):
        context = self._rollback_context
        self._rollback_context = self._rollback_check = None
        if context is not None:
            failure = RuntimeError("storage_online_rollback_channel_closed")
            context.__exit__(type(failure), failure, failure.__traceback__)

    def _rollback_channel(self, operation, *, deadline):
        """Retain the existing live fence across ordered private-pipe requests.

        Replies are observations only. No command starts a host client. EOF,
        malformed framing, failed checks and controller exit release ownership;
        a host must remain held unless its separately qualified transition can
        supervise every in-flight action. This channel cannot recreate a fence.
        """
        if operation == "rollback_fence_begin":
            if self._rollback_context is not None:
                raise RuntimeError("storage_online_rollback_channel_already_entered")
            context = self.rollback_source_fence(deadline=deadline)
            check = context.__enter__()
            self._rollback_context, self._rollback_check = context, check
        elif self.state != "resume_fenced" or self._rollback_context is None:
            raise RuntimeError("storage_online_rollback_channel_not_entered")
        result = self._rollback_check()
        if operation == "rollback_fence_end":
            context = self._rollback_context
            self._rollback_context = self._rollback_check = None
            context.__exit__(None, None, None)
            result = {**result, "database_resume_fence_held": False}
        return {**result, "runtime_activation_authorized": False}

    def final_delta(self, *, deadline):
        """Bounded tail-only round; no host pause, switch or restart authority.

        Caller retains its admitted final intent, publisher exclusion and SAME
        live worker. Bind one absolute final window for this process; no later
        round, commit or rollback admission may widen it. Original capture and
        short page ceilings still apply. After login closure and confirmed job
        retirement, reuse the retained backend for independently committed pages;
        never reconnect or return to baseline work. Empty observations are not readiness.
        """
        if self.state != "background" or (self._final_connection_entered and not self._jobs_stopped):
            raise RuntimeError("storage_online_final_delta_state_invalid")
        now = monotonic()
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or not 0 < deadline-now <= self._admitted_limits["movement_timeout_seconds"]
                or deadline > self.proof.deadline
                or (self._final_deadline is not None and deadline != self._final_deadline)):
            raise ValueError("storage_online_final_delta_deadline_invalid")
        self._final_deadline = deadline
        page_deadline = min(deadline, now+self.limits["movement_timeout_seconds"])
        try:
            self.check()
            # Admit every baseline before any tail mutation. Later per-page
            # checks repeat this under migration ownership, so no race can
            # silently fall back to a bulk copy or relocation.
            with self._database_connection() as conn, conn.begin():
                with capture.migration_step(conn, self.limits["movement_timeout_seconds"],
                                            deadline=page_deadline):
                    if self._final_connection_entered:
                        self._require_external_sql_clients_absent(conn, deadline=page_deadline)
                    if self._capture_row(conn) != self._capture:
                        raise RuntimeError("storage_online_attempt_binding_changed")
                    if online._phase(headers._inspect_progress(conn), handoff.raw._inspect(conn)) != "catch_up":
                        raise RuntimeError("storage_online_final_delta_sql_baseline_required")
                    archive_online._inspect(conn, self.source_root, self.destination_root)
                    progress = conn.execute(text(f"SELECT family,baseline_complete FROM {archive_online.PROGRESS}")).mappings().all()
                    if ({row["family"] for row in progress} != set(archives.FAMILIES)
                            or any(not row["baseline_complete"] for row in progress)):
                        raise RuntimeError("storage_online_final_delta_archive_baseline_required")
                    if self._reproved != set(archives.FAMILIES):
                        raise RuntimeError("storage_online_final_delta_background_reproof_required")
            sql = online.copy_pass(self.engine, placement=self.placement,
                policy=self.policy, resource_limits=self.limits, page_rows=self.page_rows,
                max_pages=2, max_duration_seconds=self.limits["movement_timeout_seconds"],
                tail_only=True, deadline=page_deadline,
                **({"connection": self._final_connection} if self._final_connection_entered else {}))
            pages = []
            for family in archives.FAMILIES:
                self.check()
                report = archive_online.copy_page(self.engine, family=family,
                    source_root=self.source_root, destination_root=self.destination_root,
                    page_rows=self.page_rows, max_page_bytes=self.max_page_bytes,
                    policy=self.policy, resource_limits=self.limits, file_proof=self.proof,
                    tail_only=True, deadline=page_deadline,
                    **({"connection": self._final_connection} if self._final_connection_entered else {}))
                pages.append({key: report[key] for key in ("family", "page_objects",
                    "verified_bytes", "captured_tail_empty_at_observation")})
            if self._final_connection_entered:
                with self._database_connection() as conn, conn.begin():
                    with capture.migration_step(conn, self.limits["movement_timeout_seconds"],
                                                deadline=page_deadline):
                        self._require_external_sql_clients_absent(conn, deadline=page_deadline)
            self.check()
            if monotonic() >= page_deadline:
                raise RuntimeError("storage_online_final_delta_deadline_expired")
            return {"sql": {key: sql[key] for key in ("outcome", "committed_pages", "verified_page_rows")},
                "archives": pages, "migration_ready": False,
                "final_switch_authorized": False, "collection_resume_authorized": False}
        except BaseException:
            self.state = "failed"
            raise

    def _require_external_sql_clients_absent(self, conn, *, deadline):
        """Necessary refusal only, never host publisher-exclusion authority.

        Exclude only this actual switch connection and the independently checked
        live owner connection. Idle sessions can publish later; names, addresses
        and reported application identity do not establish ownership. Prepared
        transactions can commit without a live backend and also refuse.
        Before gated job stop, this does not admit PostgreSQL/extension workers.
        After stop is requested, every other target backend must be absent and
        the closed gate, pinned environment and job definitions must still match.
        Host/archive/spool exclusion remains separately required.
        """
        self._ownership(deadline=deadline)
        if self._archive_namespace_check is not None:
            self._archive_namespace_check()
        if self._jobs_stop_requested:
            if not self._jobs_stopped:
                raise RuntimeError("storage_online_jobs_stop_unconfirmed")
            self._require_job_environment(conn)
            if self._job_catalog(conn) != self._jobs_catalog:
                raise RuntimeError("storage_online_job_definitions_changed")
        conn.exec_driver_sql("SELECT pg_stat_clear_snapshot()")
        observed = conn.execute(text("""
            SELECT
              (SELECT count(*) FROM pg_stat_activity
               WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
                 AND (:all_backends OR backend_type='client backend')
                 AND pid NOT IN (pg_backend_pid(), :owner_pid)) AS other_backends,
              (SELECT count(*) FROM pg_prepared_xacts
               WHERE database=current_database()) AS prepared_transactions
        """), {"owner_pid": self._pid, "all_backends": self._jobs_stop_requested}).mappings().one()
        if observed["other_backends"] or observed["prepared_transactions"]:
            raise RuntimeError("storage_online_sql_publishers_not_drained")
        self._ownership(deadline=deadline)

    def commit_database(self, *, deadline):
        """Internal host seam, intentionally NOT a pipe command.

        Caller must own separately qualified exact publisher drain/short-pause
        admission and an absolute monotonic deadline covering that final pause.
        This method supplies no host authority. Retain the context
        through reconcile_database() on any exception; never replay the switch.
        """
        if self.state != "background":
            raise RuntimeError("storage_online_commit_state_invalid")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("storage_online_final_deadline_invalid")
        if self._final_deadline is not None and deadline > self._final_deadline:
            raise ValueError("storage_online_final_deadline_widened")
        # All final archive pages must already be published. Keep the SAME
        # inode lock through COMMIT uncertainty and fresh outcome inspection;
        # context retirement releases it, never a serialized receipt. This only
        # excludes cooperating current stores, not unadmitted legacy/host actors.
        if self._archive_namespace_check is None:
            self._archive_namespace_check = self._stack.enter_context(
                archive_namespace(self.destination_root, exclusive=True))
        self._admit_attempt()
        remaining = deadline-monotonic()
        if (remaining <= 0
                or remaining > self._admitted_limits["movement_timeout_seconds"]
                or deadline > self.proof.deadline):
            raise ValueError("storage_online_final_deadline_not_admitted")
        # A short page allowance is not the host's final-pause allowance. The
        # latter must already be admitted, remains inside the original resource
        # and attempt ceilings, and is never renewed after drain or a retry.
        limits = {**self._admitted_limits,
                  "movement_timeout_seconds": math.ceil(remaining)}
        self.state = "commit_unknown"
        result = handoff.commit_handoff(self.engine, policy=self.policy,
            resource_limits=limits, source_root=self.source_root,
            destination_root=self.destination_root, max_objects=self.max_objects,
            max_bytes=self.max_bytes, page_rows=self.page_rows, file_proof=self.proof,
            deadline=deadline, publisher_check=self._require_external_sql_clients_absent,
            connection=self._final_connection if self._final_connection_entered else None)
        self.state = "committed"
        return result

    def inspect_outcome(self, *, deadline):
        """Fresh bounded outcome only; keep live proof and grant no host authority."""
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("storage_online_outcome_deadline_invalid")
        remaining = deadline-monotonic()
        if (not 0 < remaining <= min(5, self.limits["movement_timeout_seconds"])
                or (self._final_deadline is not None and deadline > self._final_deadline)):
            raise ValueError("storage_online_outcome_deadline_invalid")
        self._ownership()
        try:
            with self._database_connection() as conn, conn.begin():
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                with capture._bounded_step(conn, math.ceil(remaining)) as shorten:
                    shorten(deadline-monotonic())
                    if self._capture_row(conn) != self._capture:
                        raise RuntimeError("storage_online_attempt_binding_changed")
                    observed = handoff.inspect_handoff(conn, policy=self.policy,
                        source_root=self.source_root, destination_root=self.destination_root)
            self._ownership()
            if monotonic() >= deadline:
                raise RuntimeError("storage_online_outcome_deadline_expired")
            committed = observed["database_handoff_committed"]
            return {"outcome": "committed" if committed else "uncommitted",
                    "database_handoff_committed": committed,
                    "collection_resume_authorized": False, "runtime_activation_authorized": False}
        except RuntimeError as exc:
            if str(exc) not in {"fact_header_copy_migration_busy", "fact_header_handoff_outcome_pending"}:
                raise
            self._ownership()
            if monotonic() >= deadline:
                raise RuntimeError("storage_online_outcome_deadline_expired") from exc
            logger.warning("storage_online_outcome_pending | controller_id=%s reason=migration_ownership_busy",
                           self.controller_id)
            return {"outcome": "pending", "database_handoff_committed": None,
                    "collection_resume_authorized": False, "runtime_activation_authorized": False}

    def reconcile_database(self):
        if self.state != "commit_unknown":
            raise RuntimeError("storage_online_reconcile_state_invalid")
        # Reconciliation remains possible after proof expiry. It is outcome
        # inspection only, not a replacement file proof or restart permission.
        self._ownership()
        with self._database_connection() as conn, conn.begin():
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
            result = handoff.inspect_handoff(conn, policy=self.policy,
                source_root=self.source_root, destination_root=self.destination_root)
        self.state = "committed" if result["database_handoff_committed"] else "rolled_back"
        return result

    @contextmanager
    def rollback_source_fence(self, *, deadline):
        """Retain database migration ownership while the host admits old clients.

        Internal seam only; not a restart certificate or pipe operation. The host
        must already own its durable final intent, exact client/recipe admission
        and original final deadline. Call the yielded check before and after each
        bounded host action. Losing this connection invalidates the fence; saved
        negative outcomes never replace it. No copy or switch can follow this
        terminal abort on the same controller.
        """
        if self.state not in {"background", "commit_unknown", "rolled_back"}:
            raise RuntimeError("storage_online_rollback_fence_state_invalid")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("storage_online_rollback_deadline_invalid")
        if self._final_deadline is not None and deadline > self._final_deadline:
            raise ValueError("storage_online_rollback_deadline_widened")
        remaining = deadline-monotonic()
        if not 0 < remaining <= self._admitted_limits["movement_timeout_seconds"]:
            raise ValueError("storage_online_rollback_deadline_not_admitted")
        self._ownership()
        self.state = "resume_fencing"
        try:
            with self._database_connection() as conn, conn.begin():
                conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                # Outcome/abort inspection may outlive the copy attempt. This
                # does not renew its deadline or call any preparation/mover.
                with capture._bounded_step(conn, math.ceil(remaining)) as shorten:
                    shorten(deadline-monotonic())
                    if self._final_connection_entered:
                        if not self._jobs_stopped:
                            raise RuntimeError("storage_online_rollback_jobs_unconfirmed")
                        self._require_job_environment(conn)
                    observed = handoff.inspect_handoff(conn, policy=self.policy,
                        source_root=self.source_root, destination_root=self.destination_root)
                    if observed["database_handoff_committed"]:
                        self.state = "committed"
                        raise RuntimeError("storage_online_rollback_committed_refused")
                    # Protect the original relation names/OIDs against DDL while
                    # permitting the native source inserts needed on resumption.
                    conn.exec_driver_sql("LOCK TABLE market.fact_versions, "
                        "market.raw_archive_record_mappings IN ACCESS SHARE MODE NOWAIT")
                    if self._capture_row(conn) != self._capture:
                        raise RuntimeError("storage_online_attempt_binding_changed")
                    capture.inspect_capture(conn)
                    protection.inspect_protection(conn)
                    handoff.raw._inspect(conn)
                    archive_online._inspect(conn, self.source_root, self.destination_root)
                    pid = conn.scalar(text("SELECT pg_backend_pid()"))
                    self.state = "resume_fenced"

                    def check():
                        try:
                            if (self.state != "resume_fenced" or conn.closed
                                    or conn.invalidated or not conn.in_transaction()):
                                raise RuntimeError("storage_online_rollback_fence_lost")
                            if monotonic() >= deadline:
                                raise RuntimeError("storage_online_rollback_deadline_expired")
                            # SAME connection owns advisory/relation locks;
                            # invalidation never transparently reconnects.
                            if conn.scalar(text("SELECT pg_backend_pid()")) != pid:
                                raise RuntimeError("storage_online_rollback_fence_lost")
                            if archives._root(self.source_root, self._source_device)[1] != self._source:
                                raise RuntimeError("storage_online_source_changed")
                            session = {}
                            if self._final_connection_entered:
                                database = conn.scalar(text("SELECT json_build_object("
                                    "'cluster',(SELECT system_identifier::text FROM pg_control_system()),"
                                    "'oid',oid::bigint,'name',datname,'allow_connections',datallowconn) "
                                    "FROM pg_database WHERE datname=current_database()"))
                                self._require_job_environment(conn,
                                    allow_connections=database["allow_connections"])
                                if self._job_catalog(conn) != self._jobs_catalog:
                                    raise RuntimeError("storage_online_job_definitions_changed")
                                session = {"database": database,
                                    "capture": conn.scalar(text(f"SELECT to_jsonb(c) FROM {capture.STATE} c WHERE id=1")),
                                    "backend_pid": pid, "owner_pid": self._pid,
                                    "database_switch_authorized": False}
                            return {**session, "database_handoff_committed": False,
                                    "database_resume_fence_held": True,
                                    "collection_resume_authorized": False}
                        except BaseException:
                            # Even a caller that catches a failed check cannot
                            # later resurrect this process-local fence.
                            self.state = "failed"
                            raise

                    check()
                    yield check
                    check()
            self.state = "aborted"
        except BaseException:
            if self.state != "committed":
                self.state = "failed"
            logger.error("storage_online_rollback_fence_failed | controller_id=%s",
                         self.controller_id)
            raise

    def __exit__(self, *exc):
        try:
            self._abandon_rollback_channel()
        finally:
            try:
                self._stack.__exit__(*exc)
            finally:
                self.state = "closed"
        return False


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("storage_online_duplicate_command_field")
        result[key] = value
    return result


def _wait(fd, event, seconds):
    # poll supports explicitly admitted descriptor counts above FD_SETSIZE.
    poller = select.poll()
    poller.register(fd, event)
    return bool(poller.poll(max(0, math.ceil(seconds*1000))))


def _write(fd, value, seconds):
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()+b"\n"
    if len(data) > 16384:
        raise RuntimeError("storage_online_reply_budget_exceeded")
    deadline = monotonic()+seconds
    previous = os.get_blocking(fd)
    os.set_blocking(fd, False)
    try:
        while data:
            remaining = deadline-monotonic()
            if remaining <= 0 or not _wait(fd, select.POLLOUT, remaining):
                raise RuntimeError("storage_online_reply_timeout")
            try:
                count = os.write(fd, data)
            except BlockingIOError:
                continue
            data = data[count:]
    finally:
        os.set_blocking(fd, previous)


def serve(controller, *, input_fd, output_fd, channel_seconds=5):
    """Bounded newline-JSON over host-owned pipes; EOF closes live proofs.

    No listeners, files, shell commands, paths or runtime activation commands.
    Named preparation phases are explicit requests with separate admitted time
    allowances; existing source/capture/resource guards still own each phase.
    Caller owns the controller context and closes it on every return/exception.
    """
    if (type(channel_seconds) not in (int, float) or not math.isfinite(channel_seconds)
            or not 0 < channel_seconds <= 5):
        raise ValueError("storage_online_channel_budget_invalid")
    _write(output_fd, controller.status(), channel_seconds)
    pending = bytearray()
    partial_deadline = None
    while controller.state in {"background", "commit_unknown", "committed", "rolled_back", "resume_fenced", "aborted"}:
        controller.check()
        wait = min(channel_seconds, max(0, controller.proof.deadline-monotonic()))
        if controller.state in {"resume_fenced", "aborted"}:
            wait = min(wait, max(0, controller._final_deadline-monotonic()), .1)
        if partial_deadline is not None:
            wait = min(wait, max(0, partial_deadline-monotonic()))
        if wait <= 0:
            raise RuntimeError("storage_online_command_timeout")
        if not _wait(input_fd, select.POLLIN, wait):
            if pending and monotonic() >= partial_deadline:
                raise RuntimeError("storage_online_command_timeout")
            continue
        data = os.read(input_fd, 4097-len(pending))
        if not data:
            if pending:
                raise ValueError("storage_online_truncated_command")
            return
        pending.extend(data)
        if len(pending) > 4096:
            raise ValueError("storage_online_command_budget_exceeded")
        if partial_deadline is None:
            partial_deadline = monotonic()+channel_seconds
        if b"\n" not in pending:
            continue
        # No command pipelining: there is only one in-flight bounded operation.
        if pending.count(b"\n") != 1 or pending[-1:] != b"\n":
            raise ValueError("storage_online_command_pipelining_refused")
        request = json.loads(pending, object_pairs_hook=_unique)
        reply = controller.command(request)
        _write(output_fd, reply, channel_seconds)
        pending.clear()
        partial_deadline = None
