from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class FakeCoinbaseExchange:
    fills: list[dict[str, object]] = field(default_factory=list)
    rejections: list[dict[str, object]] = field(default_factory=list)
    cancellations: list[str] = field(default_factory=list)

    def submit_fill(self, *, qty: float, price: float, status: str = "filled") -> dict[str, object]:
        payload = {"qty": qty, "price": price, "status": status}
        self.fills.append(payload)
        return payload

    def reject(self, reason: str) -> None:
        self.rejections.append({"reason": reason})

    def cancel(self, order_id: str) -> None:
        self.cancellations.append(order_id)


@dataclass
class InMemoryRuntimeSink:
    events: list[dict[str, object]] = field(default_factory=list)

    def append(self, event: dict[str, object]) -> None:
        self.events.append(dict(event))
