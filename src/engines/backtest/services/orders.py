"""Builders for templated ladder targets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence

from ..utils import coerce_float


@dataclass
class OrderTemplateBuilder:
    """Create take-profit order templates from config dictionaries."""

    template: Dict[str, Any]
    defaults: Dict[str, Any]

    def _distribute_contracts(self, count: int, total: int) -> List[int]:
        if count <= 0:
            return []
        slots = [0 for _ in range(count)]
        total = total if total > 0 else count
        for idx in range(total):
            slots[idx % count] += 1
        return slots

    def build_orders(self) -> List[Dict[str, Any]]:
        """Generate validated order templates from raw input."""

        orders: List[Dict[str, Any]] = []
        entries = self.template.get("take_profit_orders") or []
        base_contracts = int(self.template.get("contracts") or len(entries) or 0)
        for idx, entry in enumerate(entries):
            ticks = coerce_float(entry.get("ticks"))
            r_multiple = coerce_float(entry.get("r_multiple"))
            price = coerce_float(entry.get("price"))
            if ticks is None and r_multiple is None and price is None:
                continue
            label = entry.get("label") or f"Target {idx + 1}"
            size_fraction = coerce_float(entry.get("size_fraction"))
            size_percent = None
            if size_fraction is not None and 0 <= size_fraction <= 1:
                size_percent = size_fraction * 100

            contracts = int(entry.get("contracts") or 0)
            if contracts <= 0 and size_percent is not None and base_contracts > 0:
                contracts = int(round((size_percent / 100) * base_contracts))
            if contracts <= 0:
                continue
            orders.append(
                {
                    "label": label,
                    "ticks": int(ticks) if ticks is not None else None,
                    "r_multiple": r_multiple,
                    "price": price,
                    "contracts": max(contracts, 1),
                    "size_percent": size_percent,
                    "id": entry.get("id"),
                }
            )
        if orders:
            return orders

        fallback_targets: Sequence[int] = (
            self.template.get("targets")
            or self.defaults.get("targets")
            or [20, 40, 60]
        )
        total_contracts = int(self.template.get("contracts") or len(fallback_targets) or 1)
        distribution = self._distribute_contracts(len(fallback_targets), total_contracts)
        built: List[Dict[str, Any]] = []
        for idx, ticks in enumerate(fallback_targets):
            built.append(
                {
                    "label": f"TP +{int(ticks)}",
                    "ticks": int(ticks),
                    "contracts": distribution[idx] if idx < len(distribution) else 1,
                    "id": f"tp-{idx + 1}",
                }
            )
        return built

    def with_total_contracts(self, total_contracts: Any) -> List[Dict[str, Any]]:
        """Scale configured targets to a total contract size."""

        base_orders = self.build_orders()
        if total_contracts in (None, 0) or not base_orders:
            return base_orders

        total = max(int(round(total_contracts)), len(base_orders))
        distribution = self._distribute_contracts(len(base_orders), total)
        scaled: List[Dict[str, Any]] = []
        for idx, order in enumerate(base_orders):
            payload = dict(order)
            payload["contracts"] = distribution[idx] if idx < len(distribution) else max(
                int(round(total / len(base_orders))), 1
            )
            scaled.append(payload)
        return scaled
