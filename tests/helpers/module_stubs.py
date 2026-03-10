from __future__ import annotations

import types
from collections.abc import Mapping


def install_module_stubs(monkeypatch, modules: Mapping[str, object]) -> None:
    """Install module stubs in ``sys.modules`` scoped to a test.

    This helper centralizes module patching so tests never mutate ``sys.modules``
    during import/collection.
    """

    for module_name, module_value in modules.items():
        module_obj = module_value
        if not isinstance(module_obj, types.ModuleType):
            module_obj = types.ModuleType(module_name)
            for attr, value in vars(module_value).items():
                setattr(module_obj, attr, value)
        monkeypatch.setitem(__import__("sys").modules, module_name, module_obj)
