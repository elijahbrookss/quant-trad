"""Bind one prepared receiver attempt to the approved recovery site."""

import re
from time import time
from uuid import uuid4

from market_data.fact_archive import archive_evidence_hash
from scripts.automation.verify_canonical_recovery_archive import _sha256


PREPARED_READY_FILENAME = "named-recovery-prepared.json"
PREPARED_READY_VERSION = "qt.named_recovery_prepared.v1"
_BASE_PINS = ("base_transfer_sha256", "baseline_verification_sha256", "pair_receipt_sha256")


def prepared_receiver_scope(*, approved_package_sha256, receiver_preflight_sha256, site_policy):
    scope = {"approved_package_sha256": approved_package_sha256,
             "receiver_preflight_sha256": receiver_preflight_sha256,
             **{field: site_policy[field] for field in ("database_identity", *_BASE_PINS)}}
    if (not all(map(_sha256, (approved_package_sha256, receiver_preflight_sha256, *(scope[field] for field in _BASE_PINS))))
            or re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", str(scope["database_identity"])) is None):
        raise ValueError("canonical_named_recovery_invalid_prepared_scope")
    return scope


def make_prepared_receiver_ready(*, scope, materialized_sha256, expires_at):
    body = {"schema": PREPARED_READY_VERSION, **scope, "materialized_sha256": materialized_sha256,
            "attempt_id": uuid4().hex, "expires_at": expires_at}
    verify_prepared_receiver_ready(body, scope=scope)
    return body


def verify_prepared_receiver_ready(body, *, scope):
    """Accept an exact, current body after verification of its receiver signature."""
    if (not isinstance(body, dict) or set(body) != set(scope) | {"schema", "materialized_sha256", "attempt_id", "expires_at"}
            or body["schema"] != PREPARED_READY_VERSION or any(body[key] != value for key, value in scope.items())
            or not _sha256(body["materialized_sha256"])
            or not isinstance(body["attempt_id"], str) or re.fullmatch(r"[0-9a-f]{32}", body["attempt_id"]) is None
            or type(body["expires_at"]) is not int):
        raise ValueError("canonical_named_recovery_prepared_scope_changed")
    if body["expires_at"] <= time():
        raise TimeoutError("canonical_named_recovery_prepared_receiver_expired")
    return archive_evidence_hash(body)
