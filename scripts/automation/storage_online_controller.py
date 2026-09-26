"""Process-local online migration controller; no production launch authority.

An admitted host owns preparation, publisher drain and runtime activation.
The bounded pipe channel deliberately exposes background work only. One context
owns destination leases until close, including an internal final database
commit/reconciliation. Never reconstruct authority from a serialized reply.
"""
from __future__ import annotations

from contextlib import ExitStack
from copy import deepcopy
import json
import logging
import math
import os
from pathlib import Path
import select
from time import monotonic
from uuid import uuid4

from sqlalchemy import text

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
_OPERATIONS = {"status", "sql_copy", "archive_copy", "reprove", "cancel", "close"}


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
        self.limits = deepcopy(_limits(resource_limits, migration=True))
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

    def _ownership(self):
        if self._owner is None or self._owner.closed or self._owner.invalidated:
            raise RuntimeError("storage_online_controller_ownership_lost")
        with self._owner.begin():
            self._owner.exec_driver_sql("SET LOCAL statement_timeout='5s'")
            if self._owner.scalar(text("SELECT pg_backend_pid()")) != self._pid:
                raise RuntimeError("storage_online_controller_ownership_lost")

    def check(self):
        if self.state not in {"background", "commit_unknown", "committed", "rolled_back"}:
            raise RuntimeError("storage_online_controller_not_active")
        self._ownership()
        self.proof.check()
        if archives._root(self.source_root, self._source_device)[1] != self._source:
            raise RuntimeError("storage_online_source_changed")

    def _admit_attempt(self):
        self.check()
        with self.engine.begin() as conn:
            with capture.migration_step(conn, 30):
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
        return {key: report[key] for key in (
            "family", "page_objects", "copied_objects", "reused_objects", "verified_bytes")}

    def command(self, request):
        if (not isinstance(request, dict)
                or set(request) != {"controller_id", "sequence", "operation"}
                or request["controller_id"] != self.controller_id
                or type(request["sequence"]) is not int
                or not 1 <= request["sequence"] <= 2**53
                or not isinstance(request["operation"], str)
                or request["operation"] not in _OPERATIONS):
            raise ValueError("storage_online_command_invalid")
        operation = request["operation"]
        if request["sequence"] == self._sequence and request == self._last_request:
            # Same-process transport retry never executes a mutation twice.
            # It is not a restart/resume token and never revives dead proof.
            if self.state not in {"closed", "cancelled"}:
                self.check()
            return deepcopy(self._last_reply)
        if request["sequence"] != self._sequence+1 or self.state != "background":
            raise RuntimeError("storage_online_command_sequence_or_state_invalid")
        result = {}
        try:
            if operation == "close":
                self._ownership()
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
                self.check()
        except BaseException:
            self.state = "failed"
            logger.error("storage_online_controller_failed | controller_id=%s operation=%s",
                             self.controller_id, operation)
            raise
        self._sequence = request["sequence"]
        reply = {**self.status(), "operation": operation, "result": result}
        self._last_request, self._last_reply = deepcopy(request), deepcopy(reply)
        return reply

    def commit_database(self):
        """Internal host seam, intentionally NOT a pipe command.

        Caller must own separately qualified exact publisher drain/short-pause
        admission. This method supplies no host authority. Retain the context
        through reconcile_database() on any exception; never replay the switch.
        """
        if self.state != "background":
            raise RuntimeError("storage_online_commit_state_invalid")
        self._admit_attempt()
        self.state = "commit_unknown"
        result = handoff.commit_handoff(self.engine, policy=self.policy,
            resource_limits=self.limits, source_root=self.source_root,
            destination_root=self.destination_root, max_objects=self.max_objects,
            max_bytes=self.max_bytes, page_rows=self.page_rows, file_proof=self.proof)
        self.state = "committed"
        return result

    def reconcile_database(self):
        if self.state != "commit_unknown":
            raise RuntimeError("storage_online_reconcile_state_invalid")
        # Reconciliation remains possible after proof expiry. It is outcome
        # inspection only, not a replacement file proof or restart permission.
        self._ownership()
        with self.engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION READ ONLY")
            conn.exec_driver_sql("SET LOCAL statement_timeout='5s'")
            result = handoff.inspect_handoff(conn, policy=self.policy,
                source_root=self.source_root, destination_root=self.destination_root)
        self.state = "committed" if result["database_handoff_committed"] else "rolled_back"
        return result

    def __exit__(self, *exc):
        try:
            return self._stack.__exit__(*exc)
        finally:
            self.state = "closed"


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
    Caller owns the controller context and closes it on every return/exception.
    """
    if (type(channel_seconds) not in (int, float) or not math.isfinite(channel_seconds)
            or not 0 < channel_seconds <= 5):
        raise ValueError("storage_online_channel_budget_invalid")
    _write(output_fd, controller.status(), channel_seconds)
    pending = bytearray()
    partial_deadline = None
    while controller.state == "background":
        controller.check()
        wait = min(channel_seconds, max(0, controller.proof.deadline-monotonic()))
        if partial_deadline is not None:
            wait = min(wait, max(0, partial_deadline-monotonic()))
        if wait <= 0:
            raise RuntimeError("storage_online_command_timeout")
        if not _wait(input_fd, select.POLLIN, wait):
            if pending:
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
