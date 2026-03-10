from __future__ import annotations

import ast
from pathlib import Path


def _is_sys_modules_mutation(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in {"setdefault", "pop", "__setitem__"}:
        value = func.value
        if isinstance(value, ast.Subscript):
            if isinstance(value.value, ast.Attribute) and value.value.attr == "modules":
                return isinstance(value.value.value, ast.Name) and value.value.value.id == "sys"
        if isinstance(value, ast.Attribute) and value.attr == "modules":
            return isinstance(value.value, ast.Name) and value.value.id == "sys"
    return False


def test_tests_do_not_mutate_sys_modules_at_module_scope():
    offenders: list[str] = []
    for path in Path("tests").rglob("test_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                if _is_sys_modules_mutation(node.value):
                    offenders.append(f"{path}:{node.lineno}")
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Subscript):
                        if isinstance(target.value, ast.Attribute) and target.value.attr == "modules":
                            if isinstance(target.value.value, ast.Name) and target.value.value.id == "sys":
                                offenders.append(f"{path}:{node.lineno}")
    assert offenders == [], "Top-level sys.modules mutation is forbidden: " + ", ".join(offenders)
