"""Indicator registry and decorator for discovery and parameter metadata."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Type

from core.logger import logger


@dataclass
class ParameterSpec:
    name: str
    required: bool
    default: Any = None


@dataclass
class IndicatorSpec:
    name: str
    cls: Type
    inputs: Optional[Iterable[str]] = None
    outputs: Optional[Iterable[str]] = None
    parameters: List[ParameterSpec] = field(default_factory=list)

    def create(self, **kwargs):
        missing = [p.name for p in self.parameters if p.required and p.name not in kwargs]
        if missing:
            raise TypeError(
                f"Missing required parameters for indicator '{self.name}': {', '.join(missing)}"
            )
        return self.cls(**kwargs)


_REGISTRY: Dict[str, IndicatorSpec] = {}


def indicator(name: str, inputs: Optional[Iterable[str]] = None, outputs: Optional[Iterable[str]] = None):
    """Decorator to register indicator classes with metadata."""

    def wrapper(cls: Type):
        if name in _REGISTRY:
            logger.warning("Replacing existing indicator registration for '%s'", name)

        parameters: List[ParameterSpec] = []
        signature = inspect.signature(cls.__init__)
        for param_name, param in list(signature.parameters.items())[1:]:  # skip self
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue
            required = param.default is inspect._empty
            default = None if required else param.default
            parameters.append(ParameterSpec(name=param_name, required=required, default=default))

        _REGISTRY[name] = IndicatorSpec(
            name=name,
            cls=cls,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
        )

        cls.NAME = getattr(cls, "NAME", name)
        return cls

    return wrapper


def get_indicator(name: str) -> Type:
    spec = _REGISTRY.get(name)
    if spec is None:
        raise KeyError(f"Indicator '{name}' is not registered")
    return spec.cls


def create_indicator(name: str, **kwargs):
    spec = _REGISTRY.get(name)
    if spec is None:
        raise KeyError(f"Indicator '{name}' is not registered")
    return spec.create(**kwargs)


def list_indicators() -> List[IndicatorSpec]:
    return list(_REGISTRY.values())
