#!/usr/bin/env python3
"""Build the Phase 1 reconciliation coverage ledger from a frozen Git tree."""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
import re
import subprocess
import tokenize
from collections import Counter, defaultdict
from pathlib import Path, PurePosixPath
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASELINE = "d46e40bf55caeea12f4ccbde640c71f271eaf9c4"
DEFAULT_OUTPUT = (
    ROOT
    / "docs"
    / "plans"
    / "documentation-reconciliation"
    / "coverage-ledger.json"
)
CONTRACT_LANGUAGE_PATTERN = re.compile(
    r"\b(?:"
    r"must|required|requires|never|do not|cannot|only|"
    r"canonical|immutable|append-only|source of truth|authoritative|"
    r"fail loud|known-at|walk-forward|no-lookahead|causal|"
    r"fenc(?:e|ing)|determin(?:ism|istic)|"
    r"guarantee|atomic|contract|boundary|replay|frozen"
    r")\b",
    flags=re.IGNORECASE,
)


def _git(*args: str, text: bool = True) -> str | bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=text,
    )
    return completed.stdout


def _baseline_files(baseline: str) -> list[str]:
    raw = _git("ls-tree", "-r", "--name-only", "-z", baseline, text=False)
    assert isinstance(raw, bytes)
    return sorted(
        value.decode("utf-8")
        for value in raw.split(b"\0")
        if value
    )


def _baseline_bytes(baseline: str, path: str) -> bytes:
    raw = _git("show", f"{baseline}:{path}", text=False)
    assert isinstance(raw, bytes)
    return raw


def _baseline_text(baseline: str, path: str) -> str:
    return _baseline_bytes(baseline, path).decode("utf-8")


def _string_literal(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _name(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return None


def _keyword(call: ast.Call, key: str) -> ast.AST | None:
    for keyword in call.keywords:
        if keyword.arg == key:
            return keyword.value
    return None


def _call_attribute(call: ast.Call, attribute: str) -> tuple[str | None, ast.Attribute] | None:
    if not isinstance(call.func, ast.Attribute) or call.func.attr != attribute:
        return None
    return _name(call.func.value), call.func


def _assigned_name(node: ast.Assign | ast.AnnAssign) -> str | None:
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target = node.targets[0]
    elif isinstance(node, ast.AnnAssign):
        target = node.target
    else:
        return None
    return target.id if isinstance(target, ast.Name) else None


def _owner_from_cli_command(command: str) -> str:
    top_level = command.split()[1] if len(command.split()) > 1 else ""
    return {
        "setup": "platform",
        "health": "platform",
        "bots": "execution-runtime",
        "runs": "execution-runtime",
        "logs": "observability",
        "strategies": "decision-layer",
        "indicators": "indicator-runtime",
        "reports": "reporting",
        "instruments": "data",
        "providers": "data",
        "data": "data",
        "market-data": "data",
        "collectors": "data",
        "research": "research-orchestration",
        "experiments": "research-orchestration",
        "mcp": "research-orchestration",
    }.get(top_level, "cli")


def _static_value(node: ast.AST | None, environment: dict[str, Any]) -> Any:
    if node is None:
        return None
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return environment.get(node.id, node.id)
    if isinstance(node, (ast.Tuple, ast.List)):
        return [_static_value(item, environment) for item in node.elts]
    if isinstance(node, ast.JoinedStr):
        return "".join(str(_static_value(item, environment)) for item in node.values)
    if isinstance(node, ast.FormattedValue):
        return _static_value(node.value, environment)
    if isinstance(node, ast.IfExp):
        branch = node.body if bool(_static_value(node.test, environment)) else node.orelse
        return _static_value(branch, environment)
    if isinstance(node, ast.Compare) and len(node.ops) == len(node.comparators) == 1:
        left = _static_value(node.left, environment)
        right = _static_value(node.comparators[0], environment)
        if isinstance(node.ops[0], ast.Eq):
            return left == right
        if isinstance(node.ops[0], ast.NotEq):
            return left != right
    raise ValueError(f"unsupported static expression: {ast.dump(node, include_attributes=False)}")


def _bind_static_target(
    target: ast.AST, value: Any, environment: dict[str, Any]
) -> None:
    if isinstance(target, ast.Name):
        environment[target.id] = value
        return
    if isinstance(target, (ast.Tuple, ast.List)) and isinstance(value, list):
        if len(target.elts) != len(value):
            raise ValueError("static loop target/value length mismatch")
        for child, child_value in zip(target.elts, value, strict=True):
            _bind_static_target(child, child_value, environment)
        return
    raise ValueError(f"unsupported static loop target: {ast.dump(target)}")


def _dynamic_cli_commands(
    builder: ast.FunctionDef | ast.AsyncFunctionDef,
    subparser_parents: dict[str, tuple[str, ...]],
) -> tuple[list[dict[str, Any]], set[int], list[dict[str, Any]]]:
    commands: list[dict[str, Any]] = []
    resolved_lines: set[int] = set()
    unresolved: list[dict[str, Any]] = []
    for loop in (node for node in ast.walk(builder) if isinstance(node, ast.For)):
        add_parser_calls = [
            node
            for node in ast.walk(loop)
            if isinstance(node, ast.Call) and _call_attribute(node, "add_parser")
        ]
        handler_calls = [
            node
            for node in ast.walk(loop)
            if isinstance(node, ast.Call)
            and _call_attribute(node, "set_defaults")
            and _keyword(node, "func") is not None
        ]
        if not add_parser_calls and not handler_calls:
            continue
        if len(add_parser_calls) != 1 or len(handler_calls) != 1:
            unresolved.append(
                {
                    "surface": "cli",
                    "line": loop.lineno,
                    "reason": "unsupported_dynamic_parser_loop_shape",
                }
            )
            continue
        add_parser = add_parser_calls[0]
        handler_call = handler_calls[0]
        parent_name = _call_attribute(add_parser, "add_parser")[0]
        parent_path = subparser_parents.get(str(parent_name))
        try:
            values = _static_value(loop.iter, {})
        except ValueError as exc:
            unresolved.append(
                {
                    "surface": "cli",
                    "line": loop.lineno,
                    "reason": "non_static_parser_loop",
                    "detail": str(exc),
                }
            )
            continue
        if parent_path is None or not isinstance(values, list):
            unresolved.append(
                {
                    "surface": "cli",
                    "line": loop.lineno,
                    "reason": "unresolved_dynamic_parser_parent_or_values",
                }
            )
            continue
        try:
            for value in values:
                environment: dict[str, Any] = {}
                _bind_static_target(loop.target, value, environment)
                command_name = _static_value(
                    add_parser.args[0] if add_parser.args else None, environment
                )
                handler = _static_value(_keyword(handler_call, "func"), environment)
                help_text = _static_value(_keyword(add_parser, "help"), environment)
                if not isinstance(command_name, str) or not isinstance(handler, str):
                    raise ValueError("dynamic command name or handler is not a string")
                commands.append(
                    {
                        "command": "qt " + " ".join((*parent_path, command_name)),
                        "handler": handler,
                        "handler_line": handler_call.lineno,
                        "parser_line": add_parser.lineno,
                        "help": help_text if isinstance(help_text, str) else None,
                        "static_expansion_line": loop.lineno,
                    }
                )
        except ValueError as exc:
            unresolved.append(
                {
                    "surface": "cli",
                    "line": loop.lineno,
                    "reason": "failed_static_parser_loop_expansion",
                    "detail": str(exc),
                }
            )
            continue
        resolved_lines.update({add_parser.lineno, handler_call.lineno})

    helpers = [
        node
        for node in builder.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    for helper in helpers:
        add_parser_calls = [
            node
            for node in ast.walk(helper)
            if isinstance(node, ast.Call) and _call_attribute(node, "add_parser")
        ]
        handler_calls = [
            node
            for node in ast.walk(helper)
            if isinstance(node, ast.Call)
            and _call_attribute(node, "set_defaults")
            and _keyword(node, "func") is not None
        ]
        if not add_parser_calls and not handler_calls:
            continue
        if len(add_parser_calls) != 1 or len(handler_calls) != 1:
            unresolved.append(
                {
                    "surface": "cli",
                    "line": helper.lineno,
                    "reason": "unsupported_parser_helper_shape",
                    "helper": helper.name,
                }
            )
            continue
        add_parser = add_parser_calls[0]
        handler_call = handler_calls[0]
        parent_name = _call_attribute(add_parser, "add_parser")[0]
        parent_path = subparser_parents.get(str(parent_name))
        calls = [
            node
            for node in ast.walk(builder)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == helper.name
        ]
        if parent_path is None:
            unresolved.append(
                {
                    "surface": "cli",
                    "line": helper.lineno,
                    "reason": "unresolved_parser_helper_parent",
                    "helper": helper.name,
                }
            )
            continue
        parameters = [argument.arg for argument in helper.args.args]
        for call in calls:
            environment = {
                key: _static_value(value, {})
                for key, value in zip(parameters, call.args)
            }
            environment.update(
                {
                    keyword.arg: _static_value(keyword.value, {})
                    for keyword in call.keywords
                    if keyword.arg is not None
                }
            )
            try:
                command_name = _static_value(
                    add_parser.args[0] if add_parser.args else None, environment
                )
                handler = _static_value(_keyword(handler_call, "func"), environment)
                help_text = _static_value(_keyword(add_parser, "help"), environment)
                if not isinstance(command_name, str) or not isinstance(handler, str):
                    raise ValueError("helper command name or handler is not a string")
            except ValueError as exc:
                unresolved.append(
                    {
                        "surface": "cli",
                        "line": call.lineno,
                        "reason": "failed_static_parser_helper_expansion",
                        "helper": helper.name,
                        "detail": str(exc),
                    }
                )
                continue
            commands.append(
                {
                    "command": "qt " + " ".join((*parent_path, command_name)),
                    "handler": handler,
                    "handler_line": handler_call.lineno,
                    "parser_line": add_parser.lineno,
                    "help": help_text if isinstance(help_text, str) else None,
                    "static_expansion_line": call.lineno,
                }
            )
        resolved_lines.update({add_parser.lineno, handler_call.lineno})
    return commands, resolved_lines, unresolved


def _extract_cli_commands(
    baseline: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    path = "cli/main.py"
    tree = ast.parse(_baseline_text(baseline, path), filename=path)
    builder = next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "build_parser"
        ),
        None,
    )
    if builder is None:
        return [], [{"surface": "cli", "reason": "build_parser_not_found"}]

    parser_paths: dict[str, tuple[str, ...]] = {"parser": ()}
    parser_details: dict[str, dict[str, Any]] = {}
    subparser_parents: dict[str, tuple[str, ...]] = {}
    handlers: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    statements = sorted(
        (
            node
            for node in ast.walk(builder)
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.Expr))
        ),
        key=lambda node: (node.lineno, node.col_offset),
    )
    for statement in statements:
        value = statement.value if isinstance(statement, (ast.Assign, ast.AnnAssign)) else statement.value
        if not isinstance(value, ast.Call):
            continue
        target = _assigned_name(statement) if isinstance(statement, (ast.Assign, ast.AnnAssign)) else None
        add_subparsers = _call_attribute(value, "add_subparsers")
        if target and add_subparsers:
            parent_name = add_subparsers[0]
            parent_path = parser_paths.get(str(parent_name))
            if parent_path is None:
                unresolved.append(
                    {
                        "surface": "cli",
                        "line": statement.lineno,
                        "reason": "unknown_add_subparsers_parent",
                        "expression": ast.unparse(value),
                    }
                )
            else:
                subparser_parents[target] = parent_path
            continue
        add_parser = _call_attribute(value, "add_parser")
        if target and add_parser:
            parent_name = add_parser[0]
            command_name = _string_literal(value.args[0]) if value.args else None
            parent_path = subparser_parents.get(str(parent_name))
            if parent_path is None or command_name is None:
                unresolved.append(
                    {
                        "surface": "cli",
                        "line": statement.lineno,
                        "reason": "dynamic_or_unresolved_add_parser",
                        "expression": ast.unparse(value),
                    }
                )
            else:
                parser_paths[target] = (*parent_path, command_name)
                parser_details[target] = {
                    "parser_line": statement.lineno,
                    "help": _string_literal(_keyword(value, "help")),
                }
            continue
        set_defaults = _call_attribute(value, "set_defaults")
        if set_defaults:
            parser_name = set_defaults[0]
            handler_node = _keyword(value, "func")
            handler = _name(handler_node)
            if parser_name not in parser_paths or handler is None:
                unresolved.append(
                    {
                        "surface": "cli",
                        "line": statement.lineno,
                        "reason": "dynamic_or_unresolved_handler",
                        "expression": ast.unparse(value),
                    }
                )
            else:
                handlers[str(parser_name)] = {
                    "handler": handler,
                    "handler_line": statement.lineno,
                }

    parent_paths = set(subparser_parents.values())
    for parser_name, parser_path in sorted(parser_paths.items()):
        if not parser_path or parser_path in parent_paths or parser_name in handlers:
            continue
        unresolved.append(
            {
                "surface": "cli",
                "line": parser_details.get(parser_name, {}).get("parser_line"),
                "reason": "leaf_parser_without_handler",
                "parser": parser_name,
                "command": "qt " + " ".join(parser_path),
            }
        )

    commands: list[dict[str, Any]] = []
    for parser_name, handler in sorted(
        handlers.items(), key=lambda item: parser_paths[item[0]]
    ):
        command = "qt " + " ".join(parser_paths[parser_name])
        commands.append(
            {
                "command": command,
                "handler": handler["handler"],
                "handler_line": handler["handler_line"],
                **parser_details.get(parser_name, {}),
            }
        )
    dynamic_commands, resolved_lines, dynamic_unresolved = _dynamic_cli_commands(
        builder, subparser_parents
    )
    unresolved = [
        finding
        for finding in unresolved
        if not (
            finding.get("line") in resolved_lines
            and finding.get("reason")
            in {
                "dynamic_or_unresolved_add_parser",
                "dynamic_or_unresolved_handler",
            }
        )
    ]
    unresolved.extend(dynamic_unresolved)
    commands.extend(dynamic_commands)
    identities = Counter(str(command["command"]) for command in commands)
    for command, count in sorted(identities.items()):
        if count > 1:
            unresolved.append(
                {
                    "surface": "cli",
                    "reason": "duplicate_command_path",
                    "command": command,
                    "count": count,
                }
            )
    commands.sort(key=lambda value: str(value["command"]))
    return commands, unresolved


def _api_prefixes(baseline: str) -> tuple[dict[str, str], list[dict[str, Any]]]:
    path = "portal/backend/main.py"
    tree = ast.parse(_baseline_text(baseline, path), filename=path)
    aliases: dict[str, str] = {}
    prefixes: dict[str, str] = {}
    unresolved: list[dict[str, Any]] = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and (node.module or "").endswith("controller"):
            for alias in node.names:
                aliases[alias.asname or alias.name] = alias.name
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_attribute(node, "include_router") is None:
            continue
        router = node.args[0] if node.args else None
        prefix = _string_literal(_keyword(node, "prefix"))
        if (
            isinstance(router, ast.Attribute)
            and router.attr == "router"
            and isinstance(router.value, ast.Name)
            and router.value.id in aliases
            and prefix is not None
        ):
            prefixes[aliases[router.value.id]] = prefix
        else:
            unresolved.append(
                {
                    "surface": "api",
                    "line": node.lineno,
                    "reason": "dynamic_or_unresolved_router_registration",
                    "expression": ast.unparse(node),
                }
            )
    return prefixes, unresolved


def _join_api_path(prefix: str, route: str) -> str:
    if not prefix:
        return route or "/"
    if not route:
        return prefix
    return prefix.rstrip("/") + (route if route.startswith("/") else f"/{route}")


def _owner_from_api_module(module: str) -> str:
    return {
        "bots": "execution-runtime",
        "candles": "data",
        "indicators": "indicator-runtime",
        "instruments": "data",
        "market_data": "data",
        "providers": "data",
        "reports": "reporting",
        "research": "research-orchestration",
        "strategies": "decision-layer",
        "main": "platform",
    }.get(module, "api")


def _owner_from_api_route(module: str, route: str, function: str) -> tuple[str, str]:
    """Prefer route-level semantic ownership where a controller spans boundaries."""

    if module == "providers" and "/credentials" in route:
        return "security", "api-route-rule"
    if module != "bots":
        return _owner_from_api_module(module), "api-controller-rule"

    if route in {"/api/bots", "/api/bots/", "/api/bots/{bot_id}"} and function in {
        "create_bot",
        "delete_bot",
        "get_bot",
        "list_bots",
        "update_bot",
    }:
        return "persistence", "api-route-rule"
    if function in {"stream_bots", "stream_active_bot_runs"}:
        return "frontend", "api-route-rule"
    if (
        function.startswith("bot_lens")
        or function == "bot_telemetry_ingest"
        or "forensic" in function
        or function == "bot_run_lifecycle_events"
    ):
        return "botlens-projections", "api-route-rule"
    if function in {"bot_runtime_capacity", "bot_watchdog_status"}:
        return "observability", "api-route-rule"
    return "execution-runtime", "api-route-rule"


def _extract_api_routes(
    baseline: str, files: list[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    prefixes, unresolved = _api_prefixes(baseline)
    paths = [
        value
        for value in files
        if value == "portal/backend/main.py"
        or (
            value.startswith("portal/backend/controller/")
            and value.endswith(".py")
            and not value.endswith("/__init__.py")
        )
    ]
    routes: list[dict[str, Any]] = []
    supported = {"get", "post", "put", "patch", "delete", "options", "head", "websocket"}
    for path in sorted(paths):
        module = PurePosixPath(path).stem
        prefix = "" if module == "main" else prefixes.get(module)
        if prefix is None:
            unresolved.append(
                {
                    "surface": "api",
                    "path": path,
                    "reason": "controller_without_static_router_prefix",
                }
            )
            prefix = ""
        tree = ast.parse(_baseline_text(baseline, path), filename=path)
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in function.decorator_list:
                if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                    continue
                target = _name(decorator.func.value)
                decorator_name = decorator.func.attr
                if target not in {"router", "app"}:
                    continue
                route = _string_literal(decorator.args[0]) if decorator.args else None
                if route is None:
                    unresolved.append(
                        {
                            "surface": "api",
                            "path": path,
                            "line": decorator.lineno,
                            "reason": "dynamic_route_path",
                            "expression": ast.unparse(decorator),
                        }
                    )
                    continue
                methods: list[str]
                if decorator_name in supported:
                    methods = ["WEBSOCKET" if decorator_name == "websocket" else decorator_name.upper()]
                elif decorator_name == "api_route":
                    methods_node = _keyword(decorator, "methods")
                    if isinstance(methods_node, (ast.List, ast.Tuple)):
                        methods = [
                            value
                            for item in methods_node.elts
                            if (value := _string_literal(item)) is not None
                        ]
                    else:
                        methods = []
                    if not methods:
                        unresolved.append(
                            {
                                "surface": "api",
                                "path": path,
                                "line": decorator.lineno,
                                "reason": "dynamic_api_route_methods",
                                "expression": ast.unparse(decorator),
                            }
                        )
                        continue
                else:
                    continue
                for method in methods:
                    routes.append(
                        {
                            "method": method.upper(),
                            "route": _join_api_path(prefix, route),
                            "function": function.name,
                            "line": decorator.lineno,
                            "path": path,
                            "module": module,
                        }
                    )
    identities = Counter((route["method"], route["route"]) for route in routes)
    for identity, count in sorted(identities.items()):
        if count > 1:
            unresolved.append(
                {
                    "surface": "api",
                    "reason": "duplicate_method_and_route",
                    "method": identity[0],
                    "route": identity[1],
                    "count": count,
                }
            )
    return routes, unresolved


def _owner_from_mcp_name(name: str) -> str:
    normalized = name.lower().replace("-", "_")
    if normalized in {"quanttrad://health", "health_check"}:
        return "platform"
    if normalized == "get_active_run":
        return "execution-runtime"
    if "runtime_profile" in normalized:
        return "execution-runtime"
    if "experiment" in normalized or "research" in normalized:
        return "research-orchestration"
    if normalized == "check_data_coverage":
        return "data"
    if any(
        value in normalized
        for value in ("collector", "market_data", "market-data", "instrument", "provider")
    ):
        return "data"
    if any(value in normalized for value in ("bot", "run_status", "run_wait")):
        return "execution-runtime"
    if "indicator" in normalized:
        return "indicator-runtime"
    if any(value in normalized for value in ("strategy", "strategies", "variant")):
        return "decision-layer"
    if "report" in normalized or "dataset" in normalized:
        return "reporting"
    return "research-orchestration"


def _extract_mcp_surfaces(
    baseline: str,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    path = "cli/mcp_server.py"
    tree = ast.parse(_baseline_text(baseline, path), filename=path)
    methods = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in {"_resource_list", "_resource_templates", "_build_tools"}
    }
    result: dict[str, list[dict[str, Any]]] = {
        "resources": [],
        "resource_templates": [],
        "tools": [],
    }
    unresolved: list[dict[str, Any]] = []
    for method_name, helper, target in (
        ("_resource_list", "_resource", "resources"),
        ("_resource_templates", "_template", "resource_templates"),
    ):
        method = methods.get(method_name)
        if method is None:
            unresolved.append({"surface": "mcp", "reason": f"{method_name}_not_found"})
            continue
        calls = sorted(
            (
                node
                for node in ast.walk(method)
                if isinstance(node, ast.Call) and _name(node.func) == helper
            ),
            key=lambda node: (node.lineno, node.col_offset),
        )
        for call in calls:
            uri = _string_literal(call.args[0]) if call.args else None
            display_name = _string_literal(call.args[1]) if len(call.args) > 1 else None
            if uri is None or display_name is None:
                unresolved.append(
                    {
                        "surface": "mcp",
                        "line": call.lineno,
                        "reason": f"dynamic_{target}",
                        "expression": ast.unparse(call),
                    }
                )
                continue
            result[target].append(
                {"uri": uri, "name": display_name, "line": call.lineno}
            )

    tools_method = methods.get("_build_tools")
    returned = next(
        (
            node.value
            for node in ast.walk(tools_method) if isinstance(node, ast.Return)
        ),
        None,
    ) if tools_method is not None else None
    if not isinstance(returned, ast.Dict):
        unresolved.append({"surface": "mcp", "reason": "static_tool_dictionary_not_found"})
    else:
        for key_node, value_node in zip(returned.keys, returned.values, strict=True):
            tool_name = _string_literal(key_node)
            if tool_name is None or not isinstance(value_node, ast.Dict):
                unresolved.append(
                    {
                        "surface": "mcp",
                        "line": getattr(key_node, "lineno", None),
                        "reason": "dynamic_tool_registration",
                        "expression": ast.unparse(value_node),
                    }
                )
                continue
            fields = {
                key: value
                for key_node_inner, value in zip(value_node.keys, value_node.values, strict=True)
                if (key := _string_literal(key_node_inner)) is not None
            }
            handler = _name(fields.get("handler"))
            description = _string_literal(fields.get("description"))
            if handler is None:
                unresolved.append(
                    {
                        "surface": "mcp",
                        "line": key_node.lineno,
                        "reason": "dynamic_tool_handler",
                        "tool": tool_name,
                    }
                )
                continue
            result["tools"].append(
                {
                    "name": tool_name,
                    "handler": handler,
                    "description": description,
                    "line": key_node.lineno,
                }
            )
    return result, unresolved


def _parse_frontmatter(text: str) -> dict[str, str | list[str]]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    data: dict[str, str | list[str]] = {}
    current_key: str | None = None
    for line in lines[1:]:
        stripped = line.strip()
        if stripped == "---":
            break
        if not stripped:
            continue
        if stripped.startswith("- ") and current_key:
            current = data.setdefault(current_key, [])
            if isinstance(current, list):
                current.append(stripped[2:].strip())
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value:
            data[key] = value
            current_key = None
        else:
            data[key] = []
            current_key = key
    return data


def _as_list(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _title(text: str, path: str) -> str:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return PurePosixPath(path).name


def _document_authority(path: str) -> str:
    if path == "AGENTS.md":
        return "normative-agent-governance"
    if path == "docs/contracts/README.md":
        return "authority-descriptor"
    if path.startswith("docs/contracts/"):
        return "normative-contract"
    if path == "src/indicators/market_profile/docs/timing_contract.md":
        return "normative-component-contract"
    if path.startswith("docs/architecture/decisions/") and not path.endswith(
        "/README.md"
    ):
        return "decision-record"
    if path == "docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md":
        return "generated-index"
    if path.startswith("docs/architecture/"):
        return "explanatory-architecture"
    if path.startswith("docs/incidents/"):
        return "historical-incident"
    if path.startswith("docs/engineering/canonical-fact-migration-"):
        return "historical-migration-evidence"
    if path.startswith("docs/engineering/collector-operations-"):
        return "historical-campaign-evidence"
    if path == "docs/engineering/frontend-v2-operator-validation.md":
        return "historical-validation-evidence"
    if path in {
        "docs/plans/backtest-dataset-boundary.md",
        "docs/plans/platform-baseline-cleanup.md",
    }:
        return "historical-campaign-ledger"
    if path.startswith("docs/plans/"):
        return "working-plan"
    if path == "docs/research-campaigns/CHAINLINK_RESEARCH_BOUNDARY_LIMITATIONS.md":
        return "explanatory-corrective-notice"
    if path.startswith("docs/research-campaigns/"):
        return "research-evidence"
    if path.startswith("docs/operators/") or path.startswith("docs/guides/"):
        return "operational-guidance"
    if path == "docs/getting-started.md":
        return "operational-guidance"
    if path.startswith("docs/engineering/"):
        return "engineering-guidance"
    if path == "docker/grafana/provisioning/dashboards/README.md":
        return "operational-guidance"
    return "explanatory"


def _document_lifecycle(
    path: str, metadata: dict[str, str | list[str]]
) -> tuple[str, str]:
    historical_overrides = {
        "docs/engineering/canonical-fact-migration-backup.md",
        "docs/engineering/canonical-fact-migration-discovery.md",
        "docs/engineering/canonical-fact-migration-validation.md",
        "docs/engineering/collector-operations-discovery.md",
        "docs/engineering/collector-operations-validation.md",
        "docs/engineering/frontend-v2-operator-validation.md",
        "docs/plans/backtest-dataset-boundary.md",
        "docs/plans/platform-baseline-cleanup.md",
        "docs/research-campaigns/BTC_PERP_MARKET_STRUCTURE_CAMPAIGN_V3_DOSSIER.md",
        "docs/research-campaigns/CHAINLINK_ETH_USD_BREAKOUT_V2_DOSSIER.md",
        "docs/research-campaigns/CHAINLINK_ETH_USD_BREAKOUT_V3_SIX_MONTH_DOSSIER.md",
    }
    if path in historical_overrides:
        return "historical", "content-reviewed-override"
    if path == "docs/research-campaigns/CHAINLINK_RESEARCH_BOUNDARY_LIMITATIONS.md":
        return "active", "content-reviewed-override"
    if path == "portal/frontend/README.md":
        return "superseded", "content-reviewed-override"
    status = metadata.get("status")
    if isinstance(status, str) and status:
        return status, "frontmatter"
    if path.startswith("docs/incidents/") and not path.endswith("/README.md"):
        return "historical", "path-rule"
    if path.startswith("docs/research-campaigns/"):
        return "unclear", "path-rule"
    if path.startswith("docs/plans/"):
        return "active", "path-rule"
    return "active", "path-rule"


def _document_audit(path: str) -> tuple[str, list[str]]:
    findings = {
        "docs/architecture/research-orchestration/AUTONOMOUS_RESEARCH_AND_PROMOTION_ROADMAP.md": (
            "conflicting",
            ["DOC-AUTH-001"],
        ),
        "docs/architecture/decisions/README.md": (
            "conflicting",
            ["DOC-INDEX-001", "DOC-INDEX-002"],
        ),
        "docs/index.md": ("stale", ["DOC-LINK-001"]),
        "docs/architecture/ARCHITECTURE_DOCS_MODEL.md": (
            "stale",
            ["DOC-MODEL-001"],
        ),
        "docs/architecture/README.md": ("stale", ["DOC-MODEL-001"]),
        "docs/architecture/frontend/OPERATOR_CONSOLE_V2.md": (
            "stale",
            ["DOC-PATH-001", "TEST-SCOPE-001"],
        ),
        "docs/architecture/data/MARKET_STRUCTURE_DATA_PLANE.md": (
            "conflicting",
            ["DOC-MARKET-STRUCTURE-001"],
        ),
        "docs/architecture/execution-runtime/RUNTIME_COMPOSITION_ROOT.md": (
            "conflicting",
            ["DOC-RUNTIME-COMPOSITION-001"],
        ),
        "docs/architecture/execution-runtime/PAPER_ENGINE_V1_DESIGN.md": (
            "conflicting",
            ["DOC-RUNTIME-COMPOSITION-001"],
        ),
        "docs/architecture/decisions/0033-use-promtail-as-runtime-loki-ingress.md": (
            "conflicting",
            ["DOC-LOG-INGRESS-001"],
        ),
        "docs/architecture/decisions/0048-gate-agent-mutation-and-research-promotion.md": (
            "conflicting",
            ["DOC-MUTATION-SCOPE-001"],
        ),
        "docs/engineering/testing/ci-test-topology.md": (
            "stale",
            ["DOC-CI-TOPOLOGY-001"],
        ),
        "docs/engineering/collector-operations-discovery.md": (
            "conflicting",
            ["DOC-LIFECYCLE-001"],
        ),
        "docs/engineering/collector-operations-validation.md": (
            "conflicting",
            ["DOC-LIFECYCLE-001"],
        ),
        "docs/engineering/frontend-v2-operator-validation.md": (
            "conflicting",
            ["DOC-LIFECYCLE-001"],
        ),
        "docs/plans/backtest-dataset-boundary.md": (
            "conflicting",
            ["DOC-LIFECYCLE-001"],
        ),
        "portal/frontend/README.md": ("stale", ["DOC-STALE-001"]),
        "docker/grafana/provisioning/dashboards/README.md": (
            "stale",
            ["DOC-OPS-001"],
        ),
        "docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md": ("verified", []),
        "docs/contracts/README.md": ("verified", []),
    }
    return findings.get(path, ("unverified", []))


def _owner_from_document(
    path: str, metadata: dict[str, str | list[str]]
) -> tuple[str, str]:
    subsystem = metadata.get("subsystem")
    if isinstance(subsystem, str) and subsystem:
        return subsystem, "frontmatter"

    exact = {
        "AGENTS.md": "platform",
        "README.md": "platform",
        "docs/README.md": "platform",
        "docs/index.md": "platform",
        "docs/overview.md": "platform",
        "docs/getting-started.md": "platform",
        "docs/architecture/README.md": "architecture-docs",
        "docs/architecture/ARCHITECTURE_COMPONENT_INDEX.md": "architecture-docs",
        "docs/architecture/decisions/README.md": "architecture-docs",
        "docs/contracts/README.md": "platform",
        "docs/concepts/runtime-timeline.md": "engine",
        "docs/concepts/execution-model.md": "execution-runtime",
        "docs/concepts/strategies-and-signals.md": "decision-layer",
        "docs/concepts/botlens.md": "botlens-projections",
        "docs/concepts/reporting-datasets.md": "reporting",
        "docs/engineering/architecture.md": "platform",
        "docs/engineering/data-layer.md": "data",
        "docs/engineering/runtime-engine.md": "engine",
        "docs/engineering/observability.md": "observability",
        "docs/engineering/observability-doctrine.md": "observability",
        "docs/engineering/server-deployment.md": "deployment",
        "docs/plans/platform-baseline-cleanup.md": "platform",
        "portal/frontend/README.md": "frontend",
        "src/indicators/market_profile/docs/README.md": "indicator-runtime",
        "src/indicators/market_profile/docs/timing_contract.md": "indicator-runtime",
        "docker/grafana/provisioning/dashboards/README.md": "observability",
    }
    if path in exact:
        return exact[path], "exact-path-rule"
    if path.startswith("docs/contracts/"):
        return "platform", "path-rule"
    if path.startswith("docs/engineering/canonical-fact-"):
        return "data", "path-rule"
    if path.startswith("docs/engineering/collector-"):
        return "data", "path-rule"
    if path.startswith("docs/engineering/testing/"):
        return "testing-ci", "path-rule"
    if path.startswith("docs/engineering/documentation/"):
        return "architecture-docs", "path-rule"
    if path.startswith("docs/engineering/"):
        return "engineering", "path-rule"
    if path.startswith("docs/guides/creating-an-indicator"):
        return "indicator-runtime", "path-rule"
    if path.startswith("docs/guides/creating-a-strategy"):
        return "decision-layer", "path-rule"
    if path.startswith("docs/guides/"):
        return "data", "path-rule"
    if path.startswith("docs/incidents/runtime/"):
        return "execution-runtime", "path-rule"
    if path.startswith("docs/incidents/"):
        return "operations", "path-rule"
    if path.startswith("docs/operators/"):
        return "operations", "path-rule"
    if path.startswith("docs/research-campaigns/"):
        return "research-orchestration", "path-rule"
    return "unclear", "unresolved"


def _test_owner(path: str) -> tuple[str, str]:
    lower = path.lower()
    direct = (
        ("tests/test_cli/", "cli"),
        ("tests/test_market_data/", "data"),
        ("tests/test_data_providers/", "data"),
        ("tests/test_indicators/", "indicator-runtime"),
        ("tests/test_strategies/", "decision-layer"),
        ("tests/test_research_science/", "research-orchestration"),
        ("tests/test_reports/", "reporting"),
        ("tests/integration/runtime/", "execution-runtime"),
    )
    for prefix, owner in direct:
        if lower.startswith(prefix):
            return owner, "test-path-rule"
    keyword_rules = (
        ("botlens", "botlens-projections"),
        ("report", "reporting"),
        ("research", "research-orchestration"),
        ("strategy", "decision-layer"),
        ("indicator", "indicator-runtime"),
        ("provider", "data"),
        ("market_data", "data"),
        ("market_structure", "data"),
        ("candle", "data"),
        ("instrument", "data"),
        ("collector", "data"),
        ("wallet", "execution-runtime"),
        ("execution", "execution-runtime"),
        ("runtime", "execution-runtime"),
        ("bot_", "execution-runtime"),
        ("order", "execution-runtime"),
        ("fill", "execution-runtime"),
        ("observability", "observability"),
        ("logging", "observability"),
        ("security", "security"),
    )
    name = PurePosixPath(lower).name
    for keyword, owner in keyword_rules:
        if keyword in name:
            return owner, "test-name-rule"
    return "platform", "test-default-rule"


def _frontend_test_owner(path: str) -> tuple[str, str]:
    owner, basis = _test_owner(path)
    if basis != "test-default-rule":
        return owner, basis
    return "frontend", "frontend-test-default-rule"


def _static_python_test_count(text: str, path: str) -> int:
    tree = ast.parse(text, filename=path)
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    )


def _static_frontend_test_count(text: str) -> int:
    return len(
        re.findall(
            r"^\s*(?:it|test)(?:\.(?:skip|only|todo))?\s*\(",
            text,
            flags=re.MULTILINE,
        )
    )


def _schema_owner(path: str) -> tuple[str, str]:
    lower = PurePosixPath(path).name.lower()
    if any(value in lower for value in ("market", "fact", "dataset", "collector")):
        return "data", "schema-name-rule"
    if any(value in lower for value in ("bot", "runtime_event", "lifecycle")):
        return "execution-runtime", "schema-name-rule"
    if "research" in lower or "async_job" in lower:
        return "research-orchestration", "schema-name-rule"
    if "report" in lower:
        return "reporting", "schema-name-rule"
    if "observability" in lower or "rollup" in lower:
        return "observability", "schema-name-rule"
    if "provider_credential" in lower:
        return "security", "schema-name-rule"
    if "strategy" in lower:
        return "decision-layer", "schema-name-rule"
    return "persistence", "schema-default-rule"


def _table_owner(table: str) -> tuple[str, str]:
    lower = table.lower()
    exact_owners = {
        "portal_async_jobs": "research-orchestration",
        "portal_bots": "persistence",
        "portal_strategies": "decision-layer",
    }
    if lower in exact_owners:
        return exact_owners[lower], "table-exact-rule"
    if "credential" in lower:
        return "security", "table-name-rule"
    if lower.startswith("portal_research_") or "holdout" in lower:
        return "research-orchestration", "table-name-rule"
    if any(value in lower for value in ("strategy", "strategies", "atm_template")):
        return "decision-layer", "table-name-rule"
    if "indicator" in lower:
        return "indicator-runtime", "table-name-rule"
    if "report" in lower:
        return "reporting", "table-name-rule"
    if "botlens" in lower:
        return "botlens-projections", "table-name-rule"
    if "capacity" in lower:
        return "observability", "table-name-rule"
    if any(
        value in lower
        for value in (
            "portal_bot",
            "trade_event",
            "run_event",
            "run_lease",
            "step_rollup",
        )
    ):
        return "execution-runtime", "table-name-rule"
    return "data", "table-default-rule"


def _extract_database_tables(
    baseline: str, files: list[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tables: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    model_paths = sorted(
        value
        for value in files
        if value.startswith("portal/backend/db/") and value.endswith("_models.py")
        or value == "portal/backend/db/models.py"
    )
    for path in model_paths:
        tree = ast.parse(_baseline_text(baseline, path), filename=path)
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            table_assignments = [
                child
                for child in node.body
                if isinstance(child, (ast.Assign, ast.AnnAssign))
                and _assigned_name(child) == "__tablename__"
            ]
            if not table_assignments:
                continue
            assignment = table_assignments[0]
            table = _string_literal(assignment.value)
            if table is None:
                unresolved.append(
                    {
                        "surface": "database",
                        "path": path,
                        "line": assignment.lineno,
                        "reason": "dynamic_table_name",
                        "class": node.name,
                    }
                )
                continue
            tables.append(
                {
                    "table": table,
                    "class": node.name,
                    "line": assignment.lineno,
                    "path": path,
                }
            )
    identities = Counter(str(table["table"]) for table in tables)
    for table, count in sorted(identities.items()):
        if count > 1:
            unresolved.append(
                {
                    "surface": "database",
                    "reason": "duplicate_table_name",
                    "table": table,
                    "count": count,
                }
            )
    return tables, unresolved


def _fallback_code_owner(path: str) -> tuple[str, str]:
    rules = (
        ("src/overlays/", "indicator-runtime"),
        ("src/core/overlay", "indicator-runtime"),
        ("src/core/chart_plotter.py", "indicator-runtime"),
        ("src/core/metrics.py", "observability"),
        ("src/core/events.py", "execution-runtime"),
        ("src/risk/", "execution-runtime"),
        ("src/utils/log_context.py", "observability"),
        ("src/utils/perf_log.py", "observability"),
        ("src/utils/time.py", "platform"),
        ("scripts/", "platform"),
        ("cli/", "cli"),
        ("portal/backend/", "api"),
        ("src/", "platform"),
    )
    for prefix, owner in rules:
        if path == prefix or path.startswith(prefix):
            return owner, "fallback-code-path-rule"
    return "unclear", "unresolved"


def _exact_architecture_ownership(
    path: str, components: list[dict[str, Any]]
) -> tuple[str, str, list[str], list[str]] | None:
    """Return the strongest ownership signal: a literal frontmatter code_path."""

    matched = [
        component
        for component in components
        if path in component.get("code_paths", [])
    ]
    if not matched:
        return None
    component_ids = sorted(str(component["component"]) for component in matched)
    boundaries = sorted({str(component["owning_boundary"]) for component in matched})
    owner = boundaries[0] if len(boundaries) == 1 else "shared"
    return owner, "architecture-exact-code-path", component_ids, boundaries


def _code_ownership(
    path: str, components: list[dict[str, Any]]
) -> tuple[str, str, list[str], list[str]]:
    exact = _exact_architecture_ownership(path, components)
    if exact is not None:
        return exact
    matched = [
        component
        for component in components
        if any(
            path == code_path or path.startswith(code_path.rstrip("/") + "/")
            for code_path in component.get("code_paths", [])
        )
    ]
    component_ids = sorted(str(component["component"]) for component in matched)
    boundaries = sorted({str(component["owning_boundary"]) for component in matched})
    if len(boundaries) == 1:
        return boundaries[0], "architecture-code-path", component_ids, boundaries
    if boundaries:
        return "shared", "architecture-code-path", component_ids, boundaries
    owner, basis = _fallback_code_owner(path)
    return owner, basis, [], []


def _contract_language_passages(
    baseline: str, files: list[str]
) -> tuple[list[dict[str, Any]], int]:
    production_paths = sorted(
        value
        for value in files
        if value.endswith(".py")
        and value.startswith(("src/", "portal/backend/", "cli/", "scripts/"))
    )
    passages: list[dict[str, Any]] = []

    def add_passage(
        *,
        path: str,
        passage_kind: str,
        symbol: str,
        line: int,
        passage: str,
    ) -> None:
        if not CONTRACT_LANGUAGE_PATTERN.search(passage):
            return
        normalized = " ".join(passage.split())
        passages.append(
            {
                "path": path,
                "passage_kind": passage_kind,
                "symbol": symbol,
                "line": line,
                "text_sha256": hashlib.sha256(passage.encode("utf-8")).hexdigest(),
                "text_length": len(passage),
                "excerpt": normalized[:600],
                "claim_triage": "unreviewed",
            }
        )

    class DocstringVisitor(ast.NodeVisitor):
        def __init__(self, path: str) -> None:
            self.path = path
            self.stack: list[str] = []

        def _visit_definition(
            self, node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
        ) -> None:
            self.stack.append(node.name)
            value = ast.get_docstring(node, clean=False)
            if value and node.body:
                add_passage(
                    path=self.path,
                    passage_kind=(
                        "class-docstring"
                        if isinstance(node, ast.ClassDef)
                        else "function-docstring"
                    ),
                    symbol=".".join(self.stack),
                    line=node.lineno,
                    passage=value,
                )
            self.generic_visit(node)
            self.stack.pop()

        def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
            self._visit_definition(node)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
            self._visit_definition(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
            self._visit_definition(node)

    for path in production_paths:
        text = _baseline_text(baseline, path)
        tree = ast.parse(text, filename=path)
        module_docstring = ast.get_docstring(tree, clean=False)
        if module_docstring and tree.body:
            add_passage(
                path=path,
                passage_kind="module-docstring",
                symbol="<module>",
                line=1,
                passage=module_docstring,
            )
        DocstringVisitor(path).visit(tree)
        source_lines = text.splitlines()
        comments: list[dict[str, Any]] = []
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            if token.type != tokenize.COMMENT:
                continue
            preceding = source_lines[token.start[0] - 1][: token.start[1]]
            comments.append(
                {
                    "line": token.start[0],
                    "column": token.start[1],
                    "text": token.string[1:],
                    "standalone": not preceding.strip(),
                }
            )
        blocks: list[list[dict[str, Any]]] = []
        for comment in comments:
            previous = blocks[-1][-1] if blocks else None
            joins_previous = (
                previous is not None
                and bool(previous["standalone"])
                and bool(comment["standalone"])
                and int(comment["line"]) == int(previous["line"]) + 1
                and int(comment["column"]) == int(previous["column"])
            )
            if joins_previous:
                blocks[-1].append(comment)
            else:
                blocks.append([comment])
        for block in blocks:
            passage = "\n".join(str(comment["text"]) for comment in block)
            add_passage(
                path=path,
                passage_kind="comment",
                symbol="<comment>",
                line=int(block[0]["line"]),
                passage=passage,
            )
    passages.sort(
        key=lambda value: (
            str(value["path"]),
            int(value["line"]),
            str(value["passage_kind"]),
            str(value["symbol"]),
        )
    )
    return passages, len(production_paths)


def _unit(
    *,
    unit_id: str,
    kind: str,
    path: str,
    owner: str,
    owner_basis: str,
    authority: str,
    lifecycle: str,
    audit_status: str = "unverified",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": unit_id,
        "kind": kind,
        "path": path,
        "owning_boundary": owner,
        "owner_basis": owner_basis,
        "authority": authority,
        "lifecycle": lifecycle,
        "audit_status": audit_status,
    }
    if extra:
        value.update(extra)
    return value


def _tracked_directories(files: list[str]) -> set[str]:
    directories: set[str] = set()
    for path in files:
        current = PurePosixPath(path).parent
        while str(current) not in {"", "."}:
            directories.add(current.as_posix())
            current = current.parent
    return directories


def build_ledger(baseline: str) -> dict[str, Any]:
    files = _baseline_files(baseline)
    file_set = set(files)
    directories = _tracked_directories(files)
    units: list[dict[str, Any]] = []
    components: list[dict[str, Any]] = []
    surface_unresolved: list[dict[str, Any]] = []
    contract_language_production_file_count = 0
    component_paths: dict[str, set[str]] = defaultdict(set)
    component_statuses: dict[str, set[str]] = defaultdict(set)

    for path in (value for value in files if value.endswith(".md")):
        content = _baseline_bytes(baseline, path)
        text = content.decode("utf-8")
        metadata = _parse_frontmatter(text)
        owner, owner_basis = _owner_from_document(path, metadata)
        lifecycle, lifecycle_basis = _document_lifecycle(path, metadata)
        audit_status, finding_ids = _document_audit(path)
        units.append(
            _unit(
                unit_id=f"document:{path}",
                kind="document",
                path=path,
                owner=owner,
                owner_basis=owner_basis,
                authority=_document_authority(path),
                lifecycle=lifecycle,
                audit_status=audit_status,
                extra={
                    "lifecycle_basis": lifecycle_basis,
                    "title": _title(text, path),
                    "has_frontmatter": bool(metadata),
                    "frontmatter_fields": sorted(metadata),
                    "doc_type": metadata.get("doc_type"),
                    "component": metadata.get("component"),
                    "status_raw": metadata.get("status"),
                    "source_role": (
                        "primary"
                        if _document_authority(path).startswith("normative")
                        else "evidence"
                        if "evidence" in _document_authority(path)
                        or _document_authority(path).startswith("historical")
                        else "summary"
                    ),
                    "included_in_semantic_audit": True,
                    "content_sha256": hashlib.sha256(content).hexdigest(),
                    "line_count": len(text.splitlines()),
                    "finding_ids": finding_ids,
                },
            )
        )
        component = metadata.get("component")
        subsystem = metadata.get("subsystem")
        if (
            path.startswith("docs/architecture/")
            and isinstance(component, str)
            and isinstance(subsystem, str)
        ):
            status = str(metadata.get("status") or "unclear")
            code_paths = _as_list(metadata.get("code_paths"))
            component_record = _unit(
                unit_id=f"component:{component}",
                kind="architecture-component",
                path=path,
                owner=subsystem,
                owner_basis="frontmatter",
                authority=_document_authority(path),
                lifecycle=status,
                audit_status=audit_status,
                extra={
                    "component": component,
                    "layer": metadata.get("layer"),
                    "doc_type": metadata.get("doc_type"),
                    "code_paths": code_paths,
                    "finding_ids": finding_ids,
                },
            )
            units.append(component_record)
            components.append(component_record)
            for code_path in code_paths:
                component_paths[code_path].add(component)
                component_statuses[code_path].add(status)

    for path in sorted(component_paths):
        exists = path in file_set or path in directories
        statuses = sorted(component_statuses[path])
        mapping_lifecycle = (
            "active"
            if any(value in {"active", "accepted", "draft"} for value in statuses)
            else "historical"
        )
        owners = sorted(
            {
                str(component["owning_boundary"])
                for component in components
                if path in component.get("code_paths", [])
            }
        )
        units.append(
            _unit(
                unit_id=f"implementation-path:{path}",
                kind="implementation-path",
                path=path,
                owner=owners[0] if len(owners) == 1 else "shared",
                owner_basis="architecture-frontmatter",
                authority="implementation-evidence",
                lifecycle="active" if exists else "missing",
                audit_status="unverified" if exists else "stale",
                extra={
                    "exists_at_baseline": exists,
                    "mapping_lifecycle": mapping_lifecycle,
                    "components": sorted(component_paths[path]),
                    "owning_boundaries": owners,
                    "component_statuses": statuses,
                    "finding_ids": [] if exists else ["DOC-PATH-001"],
                },
            )
        )

    passages, contract_language_production_file_count = _contract_language_passages(
        baseline, files
    )
    for passage in passages:
        owner, owner_basis, component_ids, boundaries = _code_ownership(
            str(passage["path"]), components
        )
        units.append(
            _unit(
                unit_id=(
                    f"contract-language:{passage['path']}:{passage['line']}:"
                    f"{passage['passage_kind']}:{passage['symbol']}"
                ),
                kind="contract-language-passage",
                path=str(passage["path"]),
                owner=owner,
                owner_basis=owner_basis,
                authority="implementation-language-candidate",
                lifecycle="active",
                extra={
                    key: value for key, value in passage.items() if key != "path"
                }
                | {
                    "architecture_component_ids": component_ids,
                    "owning_boundaries": boundaries,
                    "architecture_owner_missing": not component_ids,
                },
            )
        )

    schema_artifact_paths = sorted(
        value
        for value in files
        if value.endswith(".sql")
        or (
            value.startswith("scripts/db/")
            and PurePosixPath(value).name.startswith("migrate_")
            and value.endswith(".py")
        )
    )
    for path in schema_artifact_paths:
        exact_owner = _exact_architecture_ownership(path, components)
        if exact_owner is None:
            owner, owner_basis = _schema_owner(path)
            component_ids, boundaries = [], []
        else:
            owner, owner_basis, component_ids, boundaries = exact_owner
        lifecycle = (
            "historical"
            if "/manual_" in path or PurePosixPath(path).name.startswith("migrate_")
            else "active"
        )
        units.append(
            _unit(
                unit_id=f"schema-artifact:{path}",
                kind="schema-artifact",
                path=path,
                owner=owner,
                owner_basis=owner_basis,
                authority="implementation-evidence",
                lifecycle=lifecycle,
                extra={
                    "artifact_type": (
                        "python-data-migration" if path.endswith(".py") else "sql"
                    ),
                    "architecture_components": component_ids,
                    "architecture_boundaries": boundaries,
                },
            )
        )

    schema_source_paths = {
        "portal/backend/db/market_data_models.py": "data",
        "portal/backend/db/models.py": "persistence",
        "portal/backend/db/session.py": "persistence",
        "portal/backend/service/db/postgres_extensions.py": "persistence",
        "scripts/db/render_canonical_fact_registry.py": "data",
    }
    for path, owner in sorted(schema_source_paths.items()):
        architecture_context = _exact_architecture_ownership(path, components)
        if architecture_context is None:
            component_ids, boundaries = [], []
        else:
            _, _, component_ids, boundaries = architecture_context
        units.append(
            _unit(
                unit_id=f"schema-source:{path}",
                kind="schema-source",
                path=path,
                owner=owner,
                owner_basis="schema-source-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra={
                    "semantic_role": "relational-schema-definition",
                    "architecture_components": component_ids,
                    "architecture_boundaries": boundaries,
                },
            )
        )

    database_tables, unresolved = _extract_database_tables(baseline, files)
    surface_unresolved.extend(unresolved)
    for table in database_tables:
        owner, owner_basis = _table_owner(str(table["table"]))
        units.append(
            _unit(
                unit_id=f"database-table:{table['table']}",
                kind="database-table",
                path=str(table["path"]),
                owner=owner,
                owner_basis=owner_basis,
                authority="implementation-evidence",
                lifecycle="active",
                extra={
                    key: value for key, value in table.items() if key != "path"
                },
            )
        )

    python_test_paths = sorted(
        value
        for value in files
        if value.startswith("tests/")
        and PurePosixPath(value).name.startswith("test_")
        and value.endswith(".py")
    )
    frontend_test_paths = sorted(
        value
        for value in files
        if value.startswith("portal/frontend/")
        and re.search(r"\.(?:test|spec)\.(?:js|jsx|ts|tsx)$", value)
    )
    for path in python_test_paths:
        exact_owner = _exact_architecture_ownership(path, components)
        if exact_owner is None:
            owner, owner_basis = _test_owner(path)
            component_ids, boundaries = [], []
        else:
            owner, owner_basis, component_ids, boundaries = exact_owner
        text = _baseline_text(baseline, path)
        units.append(
            _unit(
                unit_id=f"test-suite:{path}",
                kind="test-suite",
                path=path,
                owner=owner,
                owner_basis=owner_basis,
                authority="proof-evidence",
                lifecycle="active",
                extra={
                    "execution_profile": "pytest",
                    "static_test_case_count": _static_python_test_count(text, path),
                    "architecture_components": component_ids,
                    "architecture_boundaries": boundaries,
                },
            )
        )
    for path in frontend_test_paths:
        exact_owner = _exact_architecture_ownership(path, components)
        if exact_owner is None:
            owner, owner_basis = _frontend_test_owner(path)
            component_ids, boundaries = [], []
        else:
            owner, owner_basis, component_ids, boundaries = exact_owner
        requires_unconfigured_vitest = path.endswith(".jsx")
        silently_skips_missing_scope = path.endswith("v2ReadOnlySurface.test.js")
        suite_findings = (
            (["TEST-GAP-001"] if requires_unconfigured_vitest else [])
            + (["TEST-SCOPE-001"] if silently_skips_missing_scope else [])
        )
        text = _baseline_text(baseline, path)
        units.append(
            _unit(
                unit_id=f"test-suite:{path}",
                kind="test-suite",
                path=path,
                owner=owner,
                owner_basis=owner_basis,
                authority="proof-evidence",
                lifecycle="active",
                audit_status=(
                    "stale"
                    if requires_unconfigured_vitest or silently_skips_missing_scope
                    else "unverified"
                ),
                extra={
                    "execution_profile": (
                        "vitest-unwired" if requires_unconfigured_vitest else "node-test"
                    ),
                    "static_test_case_count": _static_frontend_test_count(text),
                    "architecture_components": component_ids,
                    "architecture_boundaries": boundaries,
                    "finding_ids": suite_findings,
                },
            )
        )

    test_support_paths = sorted(
        value
        for value in files
        if value.startswith("tests/") and value not in set(python_test_paths)
    )
    for path in test_support_paths:
        units.append(
            _unit(
                unit_id=f"test-support:{path}",
                kind="test-support",
                path=path,
                owner="testing-ci",
                owner_basis="test-support-rule",
                authority="proof-support",
                lifecycle="active",
            )
        )

    interface_paths = sorted(
        value
        for value in files
        if (
            (value.startswith("cli/") and value.endswith(".py"))
            or value == "portal/backend/main.py"
            or (
                value.startswith("portal/backend/controller/")
                and value.endswith(".py")
            )
        )
    )
    for path in interface_paths:
        if path == "cli/mcp_server.py":
            owner = "research-orchestration"
            surface = "mcp"
        elif path.startswith("cli/"):
            owner = "cli"
            surface = "cli"
        else:
            owner = "api"
            surface = "api"
        units.append(
            _unit(
                unit_id=f"interface-module:{path}",
                kind="interface-module",
                path=path,
                owner=owner,
                owner_basis="surface-path-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra={"surface": surface},
            )
        )

    cli_commands, unresolved = _extract_cli_commands(baseline)
    surface_unresolved.extend(unresolved)
    for command in cli_commands:
        command_name = str(command["command"])
        units.append(
            _unit(
                unit_id=f"cli-command:{command_name}",
                kind="cli-command",
                path="cli/main.py",
                owner=_owner_from_cli_command(command_name),
                owner_basis="cli-command-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra=command,
            )
        )
    cli_groups: dict[str, int] = Counter()
    for command in cli_commands:
        parts = str(command["command"]).split()
        for length in range(2, len(parts)):
            cli_groups[" ".join(parts[:length])] += 1
    for command_name, descendant_count in sorted(cli_groups.items()):
        units.append(
            _unit(
                unit_id=f"cli-command-group:{command_name}",
                kind="cli-command-group",
                path="cli/main.py",
                owner=_owner_from_cli_command(command_name),
                owner_basis="cli-command-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra={
                    "command": command_name,
                    "descendant_leaf_count": descendant_count,
                },
            )
        )

    api_routes, unresolved = _extract_api_routes(baseline, files)
    surface_unresolved.extend(unresolved)
    for route in api_routes:
        owner, owner_basis = _owner_from_api_route(
            str(route["module"]), str(route["route"]), str(route["function"])
        )
        units.append(
            _unit(
                unit_id=f"api-route:{route['method']}:{route['route']}",
                kind="api-route",
                path=str(route["path"]),
                owner=owner,
                owner_basis=owner_basis,
                authority="implementation-evidence",
                lifecycle="active",
                extra={
                    key: value
                    for key, value in route.items()
                    if key not in {"path", "module"}
                },
            )
        )

    mcp_surfaces, unresolved = _extract_mcp_surfaces(baseline)
    surface_unresolved.extend(unresolved)
    for resource in mcp_surfaces["resources"]:
        uri = str(resource["uri"])
        units.append(
            _unit(
                unit_id=f"mcp-resource:{uri}",
                kind="mcp-resource",
                path="cli/mcp_server.py",
                owner=_owner_from_mcp_name(uri),
                owner_basis="mcp-name-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra=resource,
            )
        )
    for template in mcp_surfaces["resource_templates"]:
        uri = str(template["uri"])
        units.append(
            _unit(
                unit_id=f"mcp-resource-template:{uri}",
                kind="mcp-resource-template",
                path="cli/mcp_server.py",
                owner=_owner_from_mcp_name(uri),
                owner_basis="mcp-name-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra=template,
            )
        )
    for tool in mcp_surfaces["tools"]:
        tool_name = str(tool["name"])
        units.append(
            _unit(
                unit_id=f"mcp-tool:{tool_name}",
                kind="mcp-tool",
                path="cli/mcp_server.py",
                owner=_owner_from_mcp_name(tool_name),
                owner_basis="mcp-name-rule",
                authority="implementation-evidence",
                lifecycle="active",
                extra=tool,
            )
        )

    validation_paths = {
        "Makefile",
        "portal/frontend/eslint.config.js",
        "portal/frontend/package.json",
        "portal/frontend/vite.config.js",
        "pyproject.toml",
        "pytest.ini",
        "tests/conftest.py",
        *(
            value
            for value in files
            if value.startswith(".github/workflows/")
            or value.startswith("scripts/ci/")
            or value.startswith("scripts/docs/")
        ),
    }
    for path in sorted(validation_paths):
        units.append(
            _unit(
                unit_id=f"validation-surface:{path}",
                kind="validation-surface",
                path=path,
                owner="testing-ci" if "docs/" not in path else "architecture-docs",
                owner_basis="validation-path-rule",
                authority="implementation-evidence",
                lifecycle="active",
            )
        )

    for path in sorted(value for value in files if value.endswith(".mmd")):
        units.append(
            _unit(
                unit_id=f"diagram-source:{path}",
                kind="diagram-source",
                path=path,
                owner=path.split("/")[2] if path.startswith("docs/architecture/") else "platform",
                owner_basis="diagram-path-rule",
                authority="explanatory-source",
                lifecycle="active",
            )
        )
    for path in sorted(
        value
        for value in files
        if value.startswith("docs/") and value.endswith(".svg")
    ):
        source = path.removesuffix(".svg") + ".mmd"
        source_present = source in file_set
        units.append(
            _unit(
                unit_id=(
                    f"generated-asset:{path}"
                    if source_present
                    else f"unverified-doc-asset:{path}"
                ),
                kind="generated-asset" if source_present else "unverified-doc-asset",
                path=path,
                owner=path.split("/")[2] if path.startswith("docs/architecture/") else "platform",
                owner_basis="generated-path-rule" if source_present else "asset-path-rule",
                authority="generated" if source_present else "unverified-lineage",
                lifecycle="active",
                audit_status="unverified",
                extra={
                    "source_path": source if source_present else None,
                    "source_present": source_present,
                    "lineage_status": (
                        "source-linked" if source_present else "unverified-orphan"
                    ),
                    "finding_ids": [] if source_present else ["DOC-LINEAGE-001"],
                },
            )
        )
    for path in sorted(
        value
        for value in files
        if value.startswith("docs/research-campaigns/evidence/")
    ):
        units.append(
            _unit(
                unit_id=f"research-evidence:{path}",
                kind="research-evidence",
                path=path,
                owner="research-orchestration",
                owner_basis="evidence-path-rule",
                authority="evidence",
                lifecycle="historical",
            )
        )

    units.sort(key=lambda value: (str(value["kind"]), str(value["id"])))
    kind_counts = Counter(str(value["kind"]) for value in units)
    audit_counts = Counter(str(value["audit_status"]) for value in units)
    python_suites = [
        value
        for value in units
        if value["kind"] == "test-suite"
        and value.get("execution_profile") == "pytest"
    ]
    node_suites = [
        value
        for value in units
        if value["kind"] == "test-suite"
        and value.get("execution_profile") == "node-test"
    ]
    unwired_vitest_suites = [
        value
        for value in units
        if value["kind"] == "test-suite"
        and value.get("execution_profile") == "vitest-unwired"
    ]
    contract_passages = [
        value
        for value in units
        if value["kind"] == "contract-language-passage"
    ]
    unowned = [
        str(value["id"])
        for value in units
        if value["owning_boundary"] == "unclear"
    ]
    missing_paths = [
        str(value["path"])
        for value in units
        if value["kind"] == "implementation-path"
        and not value.get("exists_at_baseline", False)
    ]
    return {
        "schema_version": "qt.documentation_reconciliation.coverage.v1",
        "baseline": {
            "ref": baseline,
            "commit": str(_git("rev-parse", baseline)).strip(),
            "subject": str(_git("show", "-s", "--format=%s", baseline)).strip(),
        },
        "scope": {
            "tracked_file_count": len(files),
            "documentation_artifact_count": sum(
                1
                for value in units
                if value["kind"]
                in {
                    "document",
                    "diagram-source",
                    "generated-asset",
                    "unverified-doc-asset",
                    "research-evidence",
                }
            ),
            "policy": "docs/plans/documentation-reconciliation/README.md",
            "notes": [
                "The denominator is read from the frozen Git tree.",
                "Rendered assets are lineage units, not independent authorities.",
                "Exact CLI/API/MCP members are expanded statically and unresolved dynamic registrations are reported.",
                "Contract-bearing comments/docstrings and database model ownership are added by dedicated Phase 1 inventories.",
            ],
        },
        "summary": {
            "unit_count": len(units),
            "kind_counts": dict(sorted(kind_counts.items())),
            "audit_status_counts": dict(sorted(audit_counts.items())),
            "unowned_unit_count": len(unowned),
            "missing_architecture_code_path_count": len(missing_paths),
            "surface_extraction_unresolved_count": len(surface_unresolved),
            "architecture_code_path_declaration_count": sum(
                len(component.get("code_paths", [])) for component in components
            ),
            "architecture_unique_code_path_count": len(component_paths),
            "architecture_shared_code_path_count": sum(
                1 for owners in component_paths.values() if len(owners) > 1
            ),
            "tracked_sql_file_count": sum(
                1 for value in files if value.endswith(".sql")
            ),
            "python_test_suite_count": len(python_suites),
            "python_static_test_case_count": sum(
                int(value.get("static_test_case_count", 0)) for value in python_suites
            ),
            "frontend_node_test_suite_count": len(node_suites),
            "frontend_node_static_test_case_count": sum(
                int(value.get("static_test_case_count", 0)) for value in node_suites
            ),
            "frontend_unwired_vitest_suite_count": len(unwired_vitest_suites),
            "frontend_unwired_vitest_static_test_case_count": sum(
                int(value.get("static_test_case_count", 0))
                for value in unwired_vitest_suites
            ),
            "cli_command_node_count": len(cli_commands) + len(cli_groups),
            "cli_unique_handler_count": len(
                {str(command["handler"]) for command in cli_commands}
            ),
            "contract_language_production_file_count": (
                contract_language_production_file_count
            ),
            "contract_language_passage_counts": dict(
                sorted(
                    Counter(
                        str(value.get("passage_kind"))
                        for value in contract_passages
                    ).items()
                )
            ),
            "contract_language_unmapped_to_architecture_count": sum(
                bool(value.get("architecture_owner_missing"))
                for value in contract_passages
            ),
        },
        "unresolved": {
            "unowned_unit_ids": unowned,
            "missing_architecture_code_paths": missing_paths,
            "surface_extraction": surface_unresolved,
        },
        "units": units,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", default=DEFAULT_BASELINE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    ledger = build_ledger(args.baseline)
    rendered = json.dumps(ledger, indent=2, sort_keys=True) + "\n"
    output = args.output if args.output.is_absolute() else ROOT / args.output
    if args.check:
        if not output.exists() or output.read_text(encoding="utf-8") != rendered:
            raise SystemExit(f"reconciliation inventory is stale: {output.relative_to(ROOT)}")
        print(f"verified {output.relative_to(ROOT)}")
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(
        f"wrote {output.relative_to(ROOT)} with "
        f"{ledger['summary']['unit_count']} coverage units"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
