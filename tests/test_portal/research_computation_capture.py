"""Opt-in differential capture for the same synthetic fixtures on two sources.

Invoke with -p tests.test_portal.research_computation_capture and explicit output
and source-root environment variables. It never dispatches scientific jobs.
"""
from __future__ import annotations
from contextvars import ContextVar
from functools import wraps
import json
import hashlib
import math
from dataclasses import fields, is_dataclass
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from enum import Enum
import pandas as pd
import os
from pathlib import Path

_depth = ContextVar("research_capture_depth", default=0)
_records = []
_node = None
_sequence = 0


def _material(value):
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, tuple):
        return [_material(item) for item in value]
    return value


def _snapshot(value):
    # Audit serialization only; never computes a feature or an evidence hash.
    if isinstance(value, pd.DataFrame):
        return {"dataframe": _snapshot(value.to_dict(orient="split")),
                "dtypes": [str(item) for item in value.dtypes]}
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"nonfinite_float": str(value)}
    if isinstance(value, (datetime, Decimal)):
        return {type(value).__name__: str(value)}
    if isinstance(value, Enum):
        return {"enum": type(value).__name__, "value": _snapshot(value.value)}
    if isinstance(value, Mapping):
        return {str(key): _snapshot(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_snapshot(item) for item in value]
    if is_dataclass(value):
        return {"dataclass": type(value).__name__,
                "fields": {field.name: _snapshot(getattr(value, field.name)) for field in fields(value)}}
    if hasattr(value, "to_dict"):
        return _snapshot(value.to_dict())
    if hasattr(value, "item"):
        return _snapshot(value.item())
    raise TypeError("comparison_input_type_unsupported: "+type(value).__name__)


def _input_digest(value):
    encoded = json.dumps(_snapshot(value), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _wrap(original, kind):
    @wraps(original)
    def capture(*args, **kwargs):
        global _sequence
        outer = _depth.get() == 0
        token = _depth.set(_depth.get()+1)
        record = None
        if outer:
            record = {"node": _node, "sequence": _sequence, "kind": kind}
            _sequence += 1
            # Do not touch test instrumentation properties or checkpoint counters.
            if "snapshot_sweep_record_visits" not in (_node or ""):
                audit_inputs = {"args": args[1:] if kind.startswith("evaluate:") else args,
                                "kwargs": kwargs}
                evidence = kwargs.get("inputs", {}).get("indicator_evidence", {})
                if "perf" in evidence:
                    # Existing runtime fixture excludes this wall-clock instrumentation.
                    audit_inputs = {**audit_inputs, "kwargs": {**kwargs, "inputs": {
                        **kwargs["inputs"], "indicator_evidence": {
                            key: value for key, value in evidence.items() if key != "perf"}}}}
                    record["excluded_input_fields"] = ["inputs.indicator_evidence.perf"]
                record["captured_input_sha256"] = _input_digest(audit_inputs)
            else:
                record["input_audit_exclusion"] = "performance-instrumented record properties"
        try:
            result = original(*args, **kwargs)
            if outer:
                record["result"] = _material(result)
            return result
        except Exception as exc:
            if outer:
                record["error"] = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            _depth.reset(token)
            if outer:
                if "captured_input_sha256" in record:
                    record["input_unchanged"] = record["captured_input_sha256"] == _input_digest(audit_inputs)
                _records.append(record)
    return capture


def pytest_sessionstart(session):
    from portal.backend.service.research import registry, event_fact_evaluator
    from portal.backend.service.research.matched_origin_evaluator import MatchedOriginEvaluator
    from portal.backend.service.research.shared_landmark_evaluator import SharedLandmarkEvaluator
    from portal.backend.service.research.first_return_evaluator import FirstReturnEvaluator
    from portal.backend.service.research.forward_risk_evaluator import ForwardRiskEvaluator
    expected = Path(os.environ["QT_RESEARCH_COMPARISON_SOURCE"]).resolve()
    assert Path(event_fact_evaluator.__file__).resolve().is_relative_to(expected)
    for cls in [event_fact_evaluator.EventFactEvaluator, MatchedOriginEvaluator,
                SharedLandmarkEvaluator, FirstReturnEvaluator, ForwardRiskEvaluator,
                registry._LegacyEvaluator]:
        cls.evaluate = _wrap(cls.evaluate, "evaluate:"+cls.__name__)
    for name in ["materialize_check_definition", "normalize_check_request",
                 "pin_check_definition_to_plan", "reconstruct_pinned_check_definition"]:
        setattr(registry, name, _wrap(getattr(registry, name), name))
    _records.append({"kind": "registered_definitions",
                     "result": [item.to_dict() for item in registry.CHECK_REGISTRY.definitions()]})


def pytest_runtest_setup(item):
    global _node, _sequence
    _node, _sequence = item.nodeid, 0


def pytest_sessionfinish(session, exitstatus):
    destination = Path(os.environ["QT_RESEARCH_COMPARISON_OUT"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps({"source_root": os.environ["QT_RESEARCH_COMPARISON_SOURCE"],
        "revision": os.environ["QT_RESEARCH_COMPARISON_REVISION"],
        "coverage": "synthetic representative fixtures; no archived real Check execution",
        "pytest_exit": int(exitstatus), "records": _records}, sort_keys=True, indent=2)+"\n")
