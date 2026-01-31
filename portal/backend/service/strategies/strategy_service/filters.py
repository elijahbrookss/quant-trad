"""Strategy filter definitions, validation, and evaluation helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ALLOWED_SOURCES = {"regime_stats", "candle_stats"}
ALLOWED_OPERATORS = {
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "between",
    "exists",
    "missing",
}
ALLOWED_MISSING_POLICIES = {"fail", "pass", "ignore"}


def _utcnow() -> datetime:
    return datetime.utcnow()


@dataclass
class FilterDefinition:
    id: str
    scope: str
    name: str
    dsl: Dict[str, Any]
    enabled: bool = True
    description: Optional[str] = None
    created_at: datetime = field(default_factory=_utcnow)
    updated_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "scope": self.scope,
            "name": self.name,
            "description": self.description,
            "dsl": dict(self.dsl or {}),
            "enabled": bool(self.enabled),
            "created_at": self.created_at.isoformat() + "Z",
            "updated_at": self.updated_at.isoformat() + "Z",
        }

    def to_storage_payload(self, owner_id: str) -> Dict[str, Any]:
        payload = {
            "id": self.id,
            "scope": self.scope,
            "name": self.name,
            "description": self.description,
            "dsl": dict(self.dsl or {}),
            "enabled": bool(self.enabled),
        }
        if self.scope == "GLOBAL":
            payload["strategy_id"] = owner_id
        else:
            payload["rule_id"] = owner_id
        return payload


@dataclass(frozen=True)
class FilterContext:
    instrument_id: str
    candle_time: Optional[datetime]
    candle_stats_latest: Mapping[Tuple[str, datetime], Dict[str, Any]]
    candle_stats_by_version: Mapping[Tuple[str, datetime, str], Dict[str, Any]]
    regime_stats_latest: Mapping[Tuple[str, datetime], Dict[str, Any]]
    regime_stats_by_version: Mapping[Tuple[str, datetime, str], Dict[str, Any]]

    def lookup_source(
        self, source: str, version: Optional[str]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
        if self.candle_time is None:
            return None, None, "signal_time_missing"
        if source not in ALLOWED_SOURCES:
            return None, None, "unsupported_source"

        if source == "candle_stats":
            if version:
                key = (self.instrument_id, self.candle_time, version)
                if key in self.candle_stats_by_version:
                    return self.candle_stats_by_version[key], version, None
                latest = self.candle_stats_latest.get((self.instrument_id, self.candle_time))
                if latest and latest.get("version") not in (None, version):
                    return None, latest.get("version"), "version_mismatch"
                return None, None, "missing_data"
            latest = self.candle_stats_latest.get((self.instrument_id, self.candle_time))
            if latest:
                return latest.get("stats"), latest.get("version"), None
            return None, None, "missing_data"

        if version:
            key = (self.instrument_id, self.candle_time, version)
            if key in self.regime_stats_by_version:
                return self.regime_stats_by_version[key], version, None
            latest = self.regime_stats_latest.get((self.instrument_id, self.candle_time))
            if latest and latest.get("version") not in (None, version):
                return None, latest.get("version"), "version_mismatch"
            return None, None, "missing_data"

        latest = self.regime_stats_latest.get((self.instrument_id, self.candle_time))
        if latest:
            return latest.get("regime"), latest.get("version"), None
        return None, None, "missing_data"


def validate_filter_dsl(dsl: Mapping[str, Any], *, path: str = "dsl") -> None:
    if not isinstance(dsl, Mapping):
        raise ValueError(f"{path} must be an object")

    group_keys = [key for key in ("all", "any", "not") if key in dsl]
    is_predicate = "source" in dsl or "operator" in dsl or "path" in dsl

    if group_keys and is_predicate:
        raise ValueError(f"{path} cannot mix group and predicate fields")
    if len(group_keys) > 1:
        raise ValueError(f"{path} must use exactly one group operator")

    if group_keys:
        key = group_keys[0]
        value = dsl.get(key)
        if key == "not":
            if not isinstance(value, Mapping):
                raise ValueError(f"{path}.{key} must be an object")
            validate_filter_dsl(value, path=f"{path}.{key}")
        else:
            if not isinstance(value, list) or not value:
                raise ValueError(f"{path}.{key} must be a non-empty list")
            for idx, child in enumerate(value):
                validate_filter_dsl(child, path=f"{path}.{key}[{idx}]")
        return

    source = dsl.get("source")
    if source not in ALLOWED_SOURCES:
        raise ValueError(f"{path}.source must be one of {sorted(ALLOWED_SOURCES)}")

    operator = dsl.get("operator")
    if operator not in ALLOWED_OPERATORS:
        raise ValueError(f"{path}.operator must be one of {sorted(ALLOWED_OPERATORS)}")

    json_path = dsl.get("path")
    if not isinstance(json_path, str) or not json_path.strip():
        raise ValueError(f"{path}.path must be a json path string")
    if not json_path.strip().startswith("$."):
        raise ValueError(f"{path}.path must start with '$.'")

    missing_policy = dsl.get("missing_data_policy", "fail")
    if missing_policy not in ALLOWED_MISSING_POLICIES:
        raise ValueError(f"{path}.missing_data_policy must be one of {sorted(ALLOWED_MISSING_POLICIES)}")

    if source == "candle_stats" and dsl.get("regime_version"):
        raise ValueError(f"{path}.regime_version is invalid for candle_stats")
    if source == "regime_stats" and dsl.get("stats_version"):
        raise ValueError(f"{path}.stats_version is invalid for regime_stats")

    if operator in {"exists", "missing"}:
        return

    value = dsl.get("value")
    if operator == "between":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError(f"{path}.value must be a two-item list for 'between'")
    elif operator in {"in", "not_in"}:
        if not isinstance(value, list) or not value:
            raise ValueError(f"{path}.value must be a non-empty list for '{operator}'")
    else:
        if value is None:
            raise ValueError(f"{path}.value is required for '{operator}'")


def collect_filter_versions(
    filters: Iterable[FilterDefinition],
) -> Tuple[Sequence[str], Sequence[str], bool, bool]:
    candle_versions: List[str] = []
    regime_versions: List[str] = []
    needs_latest_candle = False
    needs_latest_regime = False

    def _walk(node: Mapping[str, Any]) -> None:
        nonlocal needs_latest_candle, needs_latest_regime
        group_keys = [key for key in ("all", "any", "not") if key in node]
        if group_keys:
            key = group_keys[0]
            value = node.get(key)
            if key == "not" and isinstance(value, Mapping):
                _walk(value)
            elif isinstance(value, list):
                for child in value:
                    if isinstance(child, Mapping):
                        _walk(child)
            return

        source = node.get("source")
        if source == "candle_stats":
            version = node.get("stats_version")
            if version:
                candle_versions.append(str(version))
            else:
                needs_latest_candle = True
        elif source == "regime_stats":
            version = node.get("regime_version")
            if version:
                regime_versions.append(str(version))
            else:
                needs_latest_regime = True

    for flt in filters:
        if not flt.enabled:
            continue
        if isinstance(flt.dsl, Mapping):
            _walk(flt.dsl)

    return candle_versions, regime_versions, needs_latest_candle, needs_latest_regime


def _extract_path_value(payload: Mapping[str, Any], path: str) -> Tuple[bool, Any]:
    cursor: Any = payload
    segments = [seg for seg in path.strip().split(".") if seg and seg != "$"]
    for segment in segments:
        if isinstance(cursor, Mapping):
            if segment not in cursor:
                return False, None
            cursor = cursor.get(segment)
        else:
            return False, None
    return True, cursor


def _compare(operator: str, actual: Any, expected: Any) -> bool:
    if operator == "eq":
        return actual == expected
    if operator == "ne":
        return actual != expected
    if operator == "gt":
        return actual is not None and expected is not None and actual > expected
    if operator == "gte":
        return actual is not None and expected is not None and actual >= expected
    if operator == "lt":
        return actual is not None and expected is not None and actual < expected
    if operator == "lte":
        return actual is not None and expected is not None and actual <= expected
    if operator == "in":
        return actual in expected
    if operator == "not_in":
        return actual not in expected
    if operator == "between":
        return actual is not None and expected[0] <= actual <= expected[1]
    if operator == "exists":
        return actual is not None
    if operator == "missing":
        return actual is None
    return False


def _evaluate_node(node: Mapping[str, Any], context: FilterContext) -> Dict[str, Any]:
    group_keys = [key for key in ("all", "any", "not") if key in node]
    if group_keys:
        key = group_keys[0]
        if key == "not":
            child = node.get("not")
            child_result = _evaluate_node(child, context) if isinstance(child, Mapping) else {}
            passed = not child_result.get("passed", False)
            return {
                "type": "group",
                "operator": "not",
                "passed": passed,
                "children": [child_result] if child_result else [],
            }

        children: List[Dict[str, Any]] = []
        for child in node.get(key) or []:
            if isinstance(child, Mapping):
                children.append(_evaluate_node(child, context))
        if key == "all":
            passed = all(child.get("passed") for child in children) if children else False
        else:
            passed = any(child.get("passed") for child in children) if children else False
        return {
            "type": "group",
            "operator": key,
            "passed": passed,
            "children": children,
        }

    source = str(node.get("source") or "")
    operator = str(node.get("operator") or "")
    json_path = str(node.get("path") or "")
    missing_policy = node.get("missing_data_policy", "fail")
    value = node.get("value")
    version = node.get("stats_version") if source == "candle_stats" else node.get("regime_version")

    payload, payload_version, missing_reason = context.lookup_source(source, str(version) if version else None)
    if payload is None:
        if operator == "missing":
            return {
                "type": "predicate",
                "source": source,
                "operator": operator,
                "path": json_path,
                "value": value,
                "actual": None,
                "passed": True,
                "missing": True,
                "missing_reason": missing_reason,
                "missing_data_policy": missing_policy,
                "version": payload_version,
            }
        if missing_policy == "pass":
            return {
                "type": "predicate",
                "source": source,
                "operator": operator,
                "path": json_path,
                "value": value,
                "actual": None,
                "passed": True,
                "missing": True,
                "missing_reason": missing_reason,
                "missing_data_policy": missing_policy,
                "version": payload_version,
            }
        if missing_policy == "ignore":
            return {
                "type": "predicate",
                "source": source,
                "operator": operator,
                "path": json_path,
                "value": value,
                "actual": None,
                "passed": True,
                "ignored": True,
                "missing": True,
                "missing_reason": missing_reason,
                "missing_data_policy": missing_policy,
                "version": payload_version,
            }
        return {
            "type": "predicate",
            "source": source,
            "operator": operator,
            "path": json_path,
            "value": value,
            "actual": None,
            "passed": False,
            "missing": True,
            "missing_reason": missing_reason,
            "missing_data_policy": missing_policy,
            "version": payload_version,
        }

    found, actual = _extract_path_value(payload, json_path)
    if not found:
        if operator == "missing":
            return {
                "type": "predicate",
                "source": source,
                "operator": operator,
                "path": json_path,
                "value": value,
                "actual": None,
                "passed": True,
                "missing": True,
                "missing_reason": "path_missing",
                "missing_data_policy": missing_policy,
                "version": payload_version,
            }
        if missing_policy == "pass":
            return {
                "type": "predicate",
                "source": source,
                "operator": operator,
                "path": json_path,
                "value": value,
                "actual": None,
                "passed": True,
                "missing": True,
                "missing_reason": "path_missing",
                "missing_data_policy": missing_policy,
                "version": payload_version,
            }
        if missing_policy == "ignore":
            return {
                "type": "predicate",
                "source": source,
                "operator": operator,
                "path": json_path,
                "value": value,
                "actual": None,
                "passed": True,
                "ignored": True,
                "missing": True,
                "missing_reason": "path_missing",
                "missing_data_policy": missing_policy,
                "version": payload_version,
            }
        return {
            "type": "predicate",
            "source": source,
            "operator": operator,
            "path": json_path,
            "value": value,
            "actual": None,
            "passed": False,
            "missing": True,
            "missing_reason": "path_missing",
            "missing_data_policy": missing_policy,
            "version": payload_version,
        }

    passed = _compare(operator, actual, value)
    return {
        "type": "predicate",
        "source": source,
        "operator": operator,
        "path": json_path,
        "value": value,
        "actual": actual,
        "passed": passed,
        "version": payload_version,
    }


def evaluate_filter_definition(filter_def: FilterDefinition, context: FilterContext) -> Dict[str, Any]:
    if not filter_def.enabled:
        return {
            "filter_id": filter_def.id,
            "name": filter_def.name,
            "scope": filter_def.scope,
            "enabled": False,
            "passed": True,
            "reason": "disabled",
            "details": {},
        }

    if context.candle_time is None:
        return {
            "filter_id": filter_def.id,
            "name": filter_def.name,
            "scope": filter_def.scope,
            "enabled": True,
            "passed": False,
            "reason": "signal_time_missing",
            "details": {},
        }

    details = _evaluate_node(filter_def.dsl, context)
    passed = bool(details.get("passed"))
    reason = None
    if not passed:
        reason = "predicate_failed"
    elif details.get("ignored"):
        reason = "missing_data_ignored"
    elif details.get("missing") and details.get("missing_data_policy") == "pass":
        reason = "missing_data_passed"

    return {
        "filter_id": filter_def.id,
        "name": filter_def.name,
        "scope": filter_def.scope,
        "enabled": True,
        "passed": passed,
        "reason": reason,
        "details": details,
    }


def evaluate_filter_definitions(
    filters: Sequence[FilterDefinition],
    context: FilterContext,
) -> List[Dict[str, Any]]:
    return [evaluate_filter_definition(flt, context) for flt in filters]


def summarize_filter_results(results: Sequence[Dict[str, Any]]) -> Tuple[bool, List[Dict[str, Any]]]:
    failed = [res for res in results if not res.get("passed", False)]
    return (len(failed) == 0, failed)
