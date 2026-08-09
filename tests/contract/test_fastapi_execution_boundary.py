from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTROLLER_ROOT = ROOT / "portal" / "backend" / "controller"
HTTP_ROUTE_METHODS = {"get", "post", "put", "patch", "delete"}
KNOWN_BLOCKING_CALLS = {
    "enqueue_overlay_job",
    "enqueue_signal_job",
    "get_instance_meta",
    "rebuild_symbol_projection_snapshot",
    "reuse_quantlab_job",
    "_historical_run_snapshot",
    "_run_bot_id",
}
MIXED_ASYNC_FILES = (
    ROOT / "portal" / "backend" / "controller" / "indicators.py",
    ROOT / "portal" / "backend" / "service" / "bots" / "botlens_symbol_service.py",
)


def _route_method(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    for decorator in node.decorator_list:
        if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
            continue
        if decorator.func.attr in HTTP_ROUTE_METHODS:
            return decorator.func.attr
    return None


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _is_thread_offloaded(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current = node
    while current in parents:
        current = parents[current]
        if not isinstance(current, ast.Call):
            continue
        name = _call_name(current)
        if name in {"run_in_threadpool", "to_thread", "run_sync", "run_in_executor"}:
            return True
    return False


def test_async_http_routes_are_reserved_for_real_async_work() -> None:
    violations: list[str] = []
    for path in sorted(CONTROLLER_ROOT.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if not isinstance(node, ast.AsyncFunctionDef) or _route_method(node) is None:
                continue
            if not any(isinstance(child, ast.Await) for child in ast.walk(node)):
                violations.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.name}")

    assert violations == [], (
        "HTTP routes that call synchronous services must be declared with def so "
        "FastAPI runs them in its worker threadpool. async def is reserved for "
        "routes that actually await cooperative work. Violations: "
        + ", ".join(violations)
    )


def test_known_blocking_calls_inside_mixed_async_services_are_thread_offloaded() -> None:
    violations: list[str] = []
    for path in MIXED_ASYNC_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        for function in (node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)):
            for call in (node for node in ast.walk(function) if isinstance(node, ast.Call)):
                name = _call_name(call)
                if name not in KNOWN_BLOCKING_CALLS or _is_thread_offloaded(call, parents):
                    continue
                violations.append(
                    f"{path.relative_to(ROOT)}:{call.lineno}:{function.name}:{name}"
                )

    assert violations == [], (
        "Known synchronous DB/job/replay calls inside async services must cross an "
        "explicit thread offload seam. Violations: " + ", ".join(violations)
    )
