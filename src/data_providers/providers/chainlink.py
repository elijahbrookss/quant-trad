"""Read-only public-EVM Chainlink AggregatorV3 exact-numeric adapter."""

from __future__ import annotations

import logging
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Protocol, Sequence

import requests

from data_providers.numeric_facts import (
    NumericAcquisitionBudget,
    NumericFactBinding,
    NumericFactProviderError,
    ProviderNumericBatch,
    ProviderNumericGap,
    ProviderNumericObservation,
)
from data_providers.facts import ProviderReserveStateSnapshot
from data_providers.structured_facts import StructuredFactBinding
from market_data.fact_registry import get_fact_contract


logger = logging.getLogger(__name__)

CHAINLINK_ADAPTER_ID = "chainlink_aggregator_v3.v1"
CHAINLINK_INTERFACE_VERSION = "chainlink.aggregator_v3.v1"
CHAINLINK_MVR_ADAPTER_ID = "chainlink_mvr_bundle.v1"
CHAINLINK_MVR_INTERFACE_VERSION = "chainlink.bundle_aggregator_proxy.v1"
_DECIMALS = "0x313ce567"
_DESCRIPTION = "0x7284e416"
_VERSION = "0x54fd4d50"
_LATEST_ROUND_DATA = "0xfeaf968c"
_GET_ROUND_DATA = "0x9a6fc8f5"
_AGGREGATOR = "0x245a7bfc"
_PHASE_ID = "0x58303b10"
_PHASE_AGGREGATORS = "0xc1597304"
_ANSWER_UPDATED_TOPIC = (
    "0x0559884fd3a460db3073b7fc896cc779"
    "86f16e378210ded43186175bf646fc5f"
)
_LATEST_BUNDLE = "0x9198274f"
_BUNDLE_DECIMALS = "0x9d91348d"
_LATEST_BUNDLE_TIMESTAMP = "0xa3d610cc"


class ChainlinkProviderError(NumericFactProviderError):
    pass


class ChainlinkBudgetExceeded(ChainlinkProviderError):
    pass


class ChainlinkRpcError(ChainlinkProviderError):
    pass


class JsonRpcTransport(Protocol):
    def call(self, method: str, params: Sequence[Any]) -> Any:
        ...


class HttpJsonRpcTransport:
    """Paced one-request JSON-RPC transport; retry policy belongs to the budget."""

    def __init__(
        self,
        endpoint: str,
        *,
        timeout_seconds: float = 20.0,
        min_request_interval_seconds: float = 0.5,
    ) -> None:
        endpoint = str(endpoint or "").strip()
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("chainlink_rpc_invalid: endpoint must be HTTP(S)")
        self._endpoint = endpoint
        self._timeout_seconds = float(timeout_seconds)
        self._min_request_interval_seconds = float(
            min_request_interval_seconds
        )
        if self._min_request_interval_seconds < 0:
            raise ValueError(
                "chainlink_rpc_invalid: min request interval must be nonnegative"
            )
        self._last_request_started_at: float | None = None
        self._request_id = 0

    def call(self, method: str, params: Sequence[Any]) -> Any:
        now = time.monotonic()
        if self._last_request_started_at is not None:
            remaining = (
                self._min_request_interval_seconds
                - (now - self._last_request_started_at)
            )
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_started_at = time.monotonic()
        self._request_id += 1
        try:
            response = requests.post(
                self._endpoint,
                json={
                    "jsonrpc": "2.0",
                    "id": self._request_id,
                    "method": method,
                    "params": list(params),
                },
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise ChainlinkRpcError(
                f"chainlink_rpc_failed: method={method} error={exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise ChainlinkRpcError(
                f"chainlink_rpc_invalid_response: method={method}"
            )
        if payload.get("error") is not None:
            error = payload["error"]
            raise ChainlinkRpcError(
                f"chainlink_rpc_error: method={method} error={error}"
            )
        if "result" not in payload:
            raise ChainlinkRpcError(
                f"chainlink_rpc_invalid_response: method={method} missing=result"
            )
        return payload["result"]


@dataclass(frozen=True)
class _ChainlinkConfig:
    chain_id: int
    network: str
    proxy_address: str
    deployment_block: int
    history_start: datetime
    confirmations: int
    max_log_span: int
    current_lookback_blocks: int
    expected_decimals: int
    expected_description: str
    expected_version: int | None
    max_staleness_seconds: int

    @classmethod
    def from_binding(cls, binding: NumericFactBinding) -> "_ChainlinkConfig":
        if binding.adapter != CHAINLINK_ADAPTER_ID:
            raise ValueError(
                f"chainlink_binding_invalid: adapter={binding.adapter}"
            )
        allowed = {
            "chain_id",
            "network",
            "proxy_address",
            "deployment_block",
            "history_start",
            "confirmations",
            "max_log_span",
            "current_lookback_blocks",
            "expected_decimals",
            "expected_description",
            "expected_version",
        }
        config = dict(binding.config)
        unexpected = sorted(set(config) - allowed)
        missing = sorted(
            {
                "chain_id",
                "network",
                "proxy_address",
                "deployment_block",
                "confirmations",
                "max_log_span",
                "current_lookback_blocks",
                "expected_decimals",
                "expected_description",
                "history_start",
            }
            - set(config)
        )
        if unexpected or missing:
            raise ValueError(
                "chainlink_binding_invalid: "
                f"unexpected={','.join(unexpected)} missing={','.join(missing)}"
            )
        proxy = _address(config["proxy_address"])
        numeric_fields: dict[str, int] = {}
        for key in (
            "chain_id",
            "deployment_block",
            "confirmations",
            "max_log_span",
            "current_lookback_blocks",
            "expected_decimals",
        ):
            value = config[key]
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(
                    f"chainlink_binding_invalid: {key} must be an integer"
                )
            numeric_fields[key] = value
        if numeric_fields["chain_id"] <= 0:
            raise ValueError("chainlink_binding_invalid: chain_id")
        if numeric_fields["deployment_block"] < 0:
            raise ValueError("chainlink_binding_invalid: deployment_block")
        if numeric_fields["confirmations"] < 0:
            raise ValueError("chainlink_binding_invalid: confirmations")
        if numeric_fields["max_log_span"] <= 0:
            raise ValueError("chainlink_binding_invalid: max_log_span")
        if numeric_fields["current_lookback_blocks"] <= 0:
            raise ValueError("chainlink_binding_invalid: current_lookback_blocks")
        if not 0 <= numeric_fields["expected_decimals"] <= 255:
            raise ValueError("chainlink_binding_invalid: expected_decimals")
        description = str(config["expected_description"] or "").strip()
        network = str(config["network"] or "").strip().lower()
        if not description or not network:
            raise ValueError("chainlink_binding_invalid: network/description")
        expected_version = config.get("expected_version")
        if expected_version is not None and (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version <= 0
        ):
            raise ValueError(
                "chainlink_binding_invalid: expected_version must be a positive integer or null"
            )
        history_start_raw = str(config["history_start"] or "").strip()
        if history_start_raw.endswith("Z"):
            history_start_raw = f"{history_start_raw[:-1]}+00:00"
        try:
            history_start = datetime.fromisoformat(history_start_raw)
        except ValueError as exc:
            raise ValueError(
                "chainlink_binding_invalid: history_start must be ISO-8601"
            ) from exc
        if history_start.tzinfo is None:
            raise ValueError(
                "chainlink_binding_invalid: history_start must include timezone"
            )
        return cls(
            chain_id=numeric_fields["chain_id"],
            network=network,
            proxy_address=proxy,
            deployment_block=numeric_fields["deployment_block"],
            history_start=history_start.astimezone(timezone.utc),
            confirmations=numeric_fields["confirmations"],
            max_log_span=numeric_fields["max_log_span"],
            current_lookback_blocks=numeric_fields["current_lookback_blocks"],
            expected_decimals=numeric_fields["expected_decimals"],
            expected_description=description,
            expected_version=(
                expected_version if expected_version is not None else None
            ),
            max_staleness_seconds=int(
                binding.quality_policy["max_staleness_seconds"]
            ),
        )


class _Budget:
    def __init__(
        self,
        transport: JsonRpcTransport,
        budget: NumericAcquisitionBudget,
        *,
        binding_id: str,
    ) -> None:
        self.transport = transport
        self.budget = budget
        self.binding_id = binding_id
        self.requests = 0
        self.logs = 0

    def call(self, method: str, params: Sequence[Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.budget.max_retries + 1):
            if self.requests >= self.budget.max_requests:
                raise ChainlinkBudgetExceeded(
                    "chainlink_budget_exceeded: "
                    f"binding={self.binding_id} requests={self.requests}"
                )
            self.requests += 1
            try:
                return self.transport.call(method, params)
            except ChainlinkRpcError as exc:
                last_error = exc
                if attempt >= self.budget.max_retries:
                    raise
                retry_delay_seconds = min(8.0, 0.5 * (2**attempt))
                logger.warning(
                    "chainlink_rpc_retry | binding_id=%s method=%s "
                    "attempt=%s max_retries=%s retry_delay_seconds=%s error=%s",
                    self.binding_id,
                    method,
                    attempt + 1,
                    self.budget.max_retries,
                    retry_delay_seconds,
                    exc,
                )
                time.sleep(retry_delay_seconds)
        assert last_error is not None
        raise last_error

    def add_logs(self, count: int) -> None:
        self.logs += int(count)
        if self.logs > self.budget.max_logs:
            raise ChainlinkBudgetExceeded(
                "chainlink_budget_exceeded: "
                f"binding={self.binding_id} logs={self.logs}"
            )


def _address(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if (
        len(raw) != 42
        or not raw.startswith("0x")
        or any(character not in "0123456789abcdef" for character in raw[2:])
    ):
        raise ValueError(f"chainlink_address_invalid: {value}")
    return raw


def _hash32(value: Any, *, field_name: str) -> str:
    raw = str(value or "").strip().lower()
    if (
        len(raw) != 66
        or not raw.startswith("0x")
        or any(character not in "0123456789abcdef" for character in raw[2:])
    ):
        raise ChainlinkProviderError(
            f"chainlink_provenance_invalid: field={field_name}"
        )
    return raw


def _hex_int(value: Any) -> int:
    raw = str(value or "").strip().lower()
    if not raw.startswith("0x"):
        raise ChainlinkRpcError(f"chainlink_rpc_invalid_hex: {value}")
    try:
        return int(raw, 16)
    except ValueError as exc:
        raise ChainlinkRpcError(f"chainlink_rpc_invalid_hex: {value}") from exc


def _word(value: str, index: int) -> bytes:
    raw = str(value or "")
    if not raw.startswith("0x"):
        raise ChainlinkRpcError("chainlink_abi_invalid: result is not hex")
    try:
        payload = bytes.fromhex(raw[2:])
    except ValueError as exc:
        raise ChainlinkRpcError("chainlink_abi_invalid: malformed hex") from exc
    start = int(index) * 32
    if len(payload) < start + 32:
        raise ChainlinkRpcError("chainlink_abi_invalid: short result")
    return payload[start : start + 32]


def _uint_word(value: str, index: int = 0) -> int:
    return int.from_bytes(_word(value, index), byteorder="big", signed=False)


def _int_word(value: str, index: int = 0) -> int:
    return int.from_bytes(_word(value, index), byteorder="big", signed=True)


def _address_word(value: str, index: int = 0) -> str:
    return _address("0x" + _word(value, index)[12:].hex())


def _string_result(value: str) -> str:
    offset = _uint_word(value, 0)
    raw = bytes.fromhex(str(value)[2:])
    if offset + 32 > len(raw):
        raise ChainlinkRpcError("chainlink_abi_invalid: string offset")
    length = int.from_bytes(raw[offset : offset + 32], "big")
    data = raw[offset + 32 : offset + 32 + length]
    if len(data) != length:
        raise ChainlinkRpcError("chainlink_abi_invalid: string length")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ChainlinkRpcError("chainlink_abi_invalid: string encoding") from exc


def _bytes_result(value: str) -> bytes:
    offset = _uint_word(value, 0)
    try:
        raw = bytes.fromhex(str(value)[2:])
    except ValueError as exc:
        raise ChainlinkRpcError("chainlink_abi_invalid: malformed bytes result") from exc
    if offset % 32 or offset + 32 > len(raw):
        raise ChainlinkRpcError("chainlink_abi_invalid: bytes offset")
    length = int.from_bytes(raw[offset : offset + 32], "big")
    data = raw[offset + 32 : offset + 32 + length]
    if len(data) != length:
        raise ChainlinkRpcError("chainlink_abi_invalid: bytes length")
    return data


def _uint8_array_result(value: str) -> tuple[int, ...]:
    offset = _uint_word(value, 0)
    try:
        raw = bytes.fromhex(str(value)[2:])
    except ValueError as exc:
        raise ChainlinkRpcError("chainlink_abi_invalid: malformed array result") from exc
    if offset % 32 or offset + 32 > len(raw):
        raise ChainlinkRpcError("chainlink_abi_invalid: array offset")
    length = int.from_bytes(raw[offset : offset + 32], "big")
    if length > 256 or offset + 32 + (length * 32) > len(raw):
        raise ChainlinkRpcError("chainlink_abi_invalid: array length")
    values = tuple(
        int.from_bytes(raw[offset + 32 + index * 32 : offset + 64 + index * 32], "big")
        for index in range(length)
    )
    if any(value > 255 for value in values):
        raise ChainlinkRpcError("chainlink_abi_invalid: uint8 array value")
    return values


def _string_uint256_bundle(value: bytes) -> tuple[str, int]:
    if len(value) < 96 or len(value) % 32:
        raise ChainlinkRpcError("chainlink_mvr_bundle_invalid: tuple length")
    offset = int.from_bytes(value[:32], "big")
    quantity = int.from_bytes(value[32:64], "big")
    if offset != 64 or offset + 32 > len(value):
        raise ChainlinkRpcError("chainlink_mvr_bundle_invalid: string offset")
    length = int.from_bytes(value[offset : offset + 32], "big")
    data = value[offset + 32 : offset + 32 + length]
    if len(data) != length:
        raise ChainlinkRpcError("chainlink_mvr_bundle_invalid: string length")
    expected_length = offset + 32 + ((length + 31) // 32) * 32
    if expected_length != len(value):
        raise ChainlinkRpcError("chainlink_mvr_bundle_invalid: trailing tuple material")
    try:
        report_id = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ChainlinkRpcError("chainlink_mvr_bundle_invalid: string encoding") from exc
    if not report_id:
        raise ChainlinkRpcError("chainlink_mvr_bundle_invalid: empty report ID")
    return report_id, quantity


def _encode_uint(selector: str, value: int) -> str:
    if value < 0 or value >= 2**256:
        raise ValueError("chainlink_abi_invalid: uint argument")
    return selector + value.to_bytes(32, "big").hex()


def _round_result(value: str) -> tuple[int, int, int, int, int]:
    return (
        _uint_word(value, 0),
        _int_word(value, 1),
        _uint_word(value, 2),
        _uint_word(value, 3),
        _uint_word(value, 4),
    )


def _datetime_from_epoch(value: int) -> datetime:
    if int(value) <= 0:
        raise ChainlinkProviderError(
            f"chainlink_round_invalid: timestamp={value}"
        )
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


class ChainlinkAggregatorV3Provider:
    """Bounded, phase-aware, read-only adapter over public EVM JSON-RPC."""

    adapter_id = CHAINLINK_ADAPTER_ID

    def __init__(self, transport: JsonRpcTransport, *, endpoint_ref: str) -> None:
        endpoint_ref = str(endpoint_ref or "").strip()
        if not endpoint_ref:
            raise ValueError("chainlink_rpc_invalid: endpoint_ref is required")
        self._transport = transport
        self._endpoint_ref = endpoint_ref

    def _rpc(
        self,
        tracker: _Budget,
        method: str,
        params: Sequence[Any],
    ) -> Any:
        return tracker.call(method, params)

    def _eth_call(
        self,
        tracker: _Budget,
        *,
        address: str,
        data: str,
        block: str = "latest",
    ) -> str:
        result = self._rpc(
            tracker,
            "eth_call",
            [{"to": _address(address), "data": data}, block],
        )
        return str(result)

    def _block(
        self,
        tracker: _Budget,
        block_number: int,
    ) -> Mapping[str, Any]:
        result = self._rpc(
            tracker,
            "eth_getBlockByNumber",
            [hex(int(block_number)), False],
        )
        if not isinstance(result, dict):
            raise ChainlinkRpcError(
                f"chainlink_archive_unavailable: block={block_number}"
            )
        return result

    def _block_time(self, tracker: _Budget, block_number: int) -> datetime:
        timestamp = _hex_int(self._block(tracker, block_number)["timestamp"])
        if int(block_number) == 0 and timestamp == 0:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        return _datetime_from_epoch(timestamp)

    def _confirmed_context(
        self,
        tracker: _Budget,
        config: _ChainlinkConfig,
    ) -> tuple[int, int, Mapping[str, Any]]:
        chain_id = _hex_int(self._rpc(tracker, "eth_chainId", []))
        if chain_id != config.chain_id:
            raise ChainlinkProviderError(
                "chainlink_chain_mismatch: "
                f"expected={config.chain_id} actual={chain_id}"
            )
        head = _hex_int(self._rpc(tracker, "eth_blockNumber", []))
        confirmed_head = head - config.confirmations
        if confirmed_head < config.deployment_block:
            raise ChainlinkProviderError(
                "chainlink_finality_unavailable: confirmed head precedes deployment"
            )
        deployment = self._block(tracker, config.deployment_block)
        confirmed = self._block(tracker, confirmed_head)
        confirmed_head_time = _datetime_from_epoch(
            _hex_int(confirmed["timestamp"])
        )
        confirmed_head_hash = _hash32(
            confirmed.get("hash"), field_name="confirmed_head_hash"
        )
        capabilities = {
            "schema_version": "chainlink.rpc_capabilities.v1",
            "chain_id": chain_id,
            "archive_block_read": bool(deployment),
            "bounded_logs": True,
            "head_block": head,
            "confirmed_head_block": confirmed_head,
            "confirmations": config.confirmations,
            "confirmed_head_hash": confirmed_head_hash,
            "confirmed_head_time": confirmed_head_time.isoformat(),
        }
        return head, confirmed_head, capabilities

    def _feed_metadata(
        self,
        tracker: _Budget,
        config: _ChainlinkConfig,
    ) -> dict[str, Any]:
        decimals = _uint_word(
            self._eth_call(tracker, address=config.proxy_address, data=_DECIMALS)
        )
        description = _string_result(
            self._eth_call(tracker, address=config.proxy_address, data=_DESCRIPTION)
        )
        version = _uint_word(
            self._eth_call(tracker, address=config.proxy_address, data=_VERSION)
        )
        phase_id = _uint_word(
            self._eth_call(tracker, address=config.proxy_address, data=_PHASE_ID)
        )
        aggregator = _address_word(
            self._eth_call(tracker, address=config.proxy_address, data=_AGGREGATOR)
        )
        if (
            decimals != config.expected_decimals
            or description != config.expected_description
            or (
                config.expected_version is not None
                and version != config.expected_version
            )
        ):
            raise ChainlinkProviderError(
                "chainlink_feed_mismatch: "
                f"proxy={config.proxy_address} decimals={decimals} "
                f"description={description!r} version={version}"
            )
        if not 0 < phase_id < 2**16:
            raise ChainlinkProviderError(
                f"chainlink_phase_invalid: phase_id={phase_id}"
            )
        return {
            "decimals": decimals,
            "description": description,
            "version": version,
            "phase_id": phase_id,
            "aggregator": aggregator,
        }

    def _phase_aggregators(
        self,
        tracker: _Budget,
        config: _ChainlinkConfig,
        phase_id: int,
        current_aggregator: str,
        required_phase_ids: Sequence[int],
    ) -> tuple[dict[int, str], tuple[dict[str, Any], ...]]:
        phases: dict[int, str] = {}
        failures: list[dict[str, Any]] = []
        for phase in sorted({int(item) for item in required_phase_ids}):
            try:
                result = self._eth_call(
                    tracker,
                    address=config.proxy_address,
                    data=_encode_uint(_PHASE_AGGREGATORS, phase),
                )
                aggregator = _address_word(result)
                if aggregator == "0x" + ("0" * 40):
                    raise ChainlinkProviderError(
                        f"chainlink_phase_missing: phase_id={phase}"
                    )
                if phase == int(phase_id) and aggregator != current_aggregator:
                    raise ChainlinkProviderError(
                        "chainlink_phase_aggregator_mismatch: "
                        f"phase_id={phase} phase_aggregator={aggregator} "
                        f"current_aggregator={current_aggregator}"
                    )
            except ChainlinkProviderError as exc:
                failures.append({"phase_id": phase, "error": str(exc)})
                if phase == int(phase_id):
                    phases[phase] = current_aggregator
                continue
            phases[phase] = aggregator
        return phases, tuple(failures)

    def _logs(
        self,
        tracker: _Budget,
        *,
        address: str,
        start_block: int,
        end_block: int,
        max_log_span: int,
    ) -> list[Mapping[str, Any]]:
        logs: list[Mapping[str, Any]] = []
        cursor = int(start_block)
        while cursor <= int(end_block):
            chunk_end = min(int(end_block), cursor + int(max_log_span) - 1)
            result = self._rpc(
                tracker,
                "eth_getLogs",
                [
                    {
                        "address": _address(address),
                        "fromBlock": hex(cursor),
                        "toBlock": hex(chunk_end),
                        "topics": [_ANSWER_UPDATED_TOPIC],
                    }
                ],
            )
            if not isinstance(result, list):
                raise ChainlinkRpcError("chainlink_rpc_invalid_response: eth_getLogs")
            tracker.add_logs(len(result))
            logs.extend(item for item in result if isinstance(item, dict))
            cursor = chunk_end + 1
        return logs

    def _block_at_or_after(
        self,
        tracker: _Budget,
        *,
        target: datetime,
        low: int,
        high: int,
    ) -> int:
        target = target.astimezone(timezone.utc)
        if self._block_time(tracker, high) < target:
            raise ChainlinkProviderError(
                "chainlink_range_unconfirmed: requested time follows confirmed head"
            )
        if self._block_time(tracker, low) >= target:
            return low
        left, right = int(low), int(high)
        while left < right:
            middle = (left + right) // 2
            if self._block_time(tracker, middle) < target:
                left = middle + 1
            else:
                right = middle
        return left

    def _observation(
        self,
        tracker: _Budget,
        *,
        binding: NumericFactBinding,
        config: _ChainlinkConfig,
        metadata: Mapping[str, Any],
        capabilities: Mapping[str, Any],
        request: Mapping[str, Any],
        phase_id: int,
        aggregator: str,
        log: Mapping[str, Any],
    ) -> ProviderNumericObservation:
        if bool(log.get("removed", False)):
            raise ChainlinkProviderError("chainlink_log_removed")
        topics = list(log.get("topics") or [])
        if len(topics) < 3:
            raise ChainlinkProviderError("chainlink_log_invalid: topics")
        local_round_id = _hex_int(topics[2])
        proxy_round_id = (int(phase_id) << 64) | local_round_id
        round_data = _round_result(
            self._eth_call(
                tracker,
                address=config.proxy_address,
                data=_encode_uint(_GET_ROUND_DATA, proxy_round_id),
            )
        )
        round_id, answer, _started_at, updated_at, answered_in_round = round_data
        event_answer = _int_word(str(topics[1]), 0)
        event_updated_at = _uint_word(str(log.get("data") or ""), 0)
        if (
            round_id != proxy_round_id
            or answer != event_answer
            or updated_at != event_updated_at
            or answered_in_round < round_id
        ):
            raise ChainlinkProviderError(
                "chainlink_round_reconciliation_failed: "
                f"phase_id={phase_id} local_round_id={local_round_id}"
            )
        if updated_at <= 0:
            raise ChainlinkProviderError(
                "chainlink_round_invalid: updated_at must be positive"
            )
        block_number = _hex_int(log.get("blockNumber"))
        block_hash = _hash32(log.get("blockHash"), field_name="block_hash")
        transaction_hash = _hash32(
            log.get("transactionHash"), field_name="transaction_hash"
        )
        transaction_index = _hex_int(log.get("transactionIndex"))
        log_index = _hex_int(log.get("logIndex"))
        confirmation_block = block_number + config.confirmations
        if confirmation_block > int(capabilities["head_block"]):
            raise ChainlinkProviderError(
                "chainlink_round_unconfirmed: "
                f"block={block_number} confirmation_block={confirmation_block}"
            )
        publication = self._block(tracker, block_number)
        publication_hash = _hash32(
            publication.get("hash"), field_name="publication_block_hash"
        )
        if publication_hash != block_hash:
            raise ChainlinkProviderError(
                "chainlink_log_block_mismatch: "
                f"block={block_number} log_hash={block_hash} "
                f"canonical_hash={publication_hash}"
            )
        confirmation = (
            publication
            if confirmation_block == block_number
            else self._block(tracker, confirmation_block)
        )
        source_published_at = _datetime_from_epoch(
            _hex_int(publication["timestamp"])
        )
        known_at = _datetime_from_epoch(_hex_int(confirmation["timestamp"]))
        confirmation_hash = _hash32(
            confirmation.get("hash"), field_name="confirmation_block_hash"
        )
        effective_at = _datetime_from_epoch(updated_at)
        group_key = (
            f"evm:{config.chain_id}:{config.proxy_address}:{proxy_round_id}"
        )
        raw_answer = str(answer)
        value = Decimal(answer).scaleb(-int(metadata["decimals"]))
        try:
            get_fact_contract(binding.fact_type).validate_numeric_value(
                value=value,
                unit=binding.unit,
                dimensions=binding.dimensions,
            )
        except ValueError as exc:
            raise ChainlinkProviderError(
                "chainlink_answer_contract_invalid: "
                f"fact_type={binding.fact_type} raw_answer={raw_answer}"
            ) from exc
        source_event_material = {
            "schema_version": "chainlink.aggregator_v3.event_material.v1",
            "chain_id": config.chain_id,
            "network": config.network,
            "proxy_address": config.proxy_address,
            "aggregator_address": aggregator,
            "phase_id": phase_id,
            "local_round_id": local_round_id,
            "proxy_round_id": proxy_round_id,
            "answered_in_round": answered_in_round,
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_hash": transaction_hash,
            "transaction_index": transaction_index,
            "log_index": log_index,
            "removed": bool(log.get("removed", False)),
            "confirmation_block": confirmation_block,
            "confirmation_block_hash": confirmation_hash,
            "raw_answer": raw_answer,
            "decimals": int(metadata["decimals"]),
            "description": str(metadata["description"]),
            "version": int(metadata["version"]),
        }
        return ProviderNumericObservation(
            value=value,
            raw_value=raw_answer,
            effective_at=effective_at,
            effective_at_method="chainlink_round_updated_at",
            source_published_at=source_published_at,
            known_at=known_at,
            known_at_method="evm_confirmation_block",
            source_event_key=f"{group_key}:answer",
            source_event_group_key=group_key,
            source_event_component_key="answer",
            provenance={
                "schema_version": "chainlink.aggregator_v3.provenance.v1",
                "chain_id": config.chain_id,
                "network": config.network,
                "proxy_address": config.proxy_address,
                "aggregator_address": aggregator,
                "phase_id": phase_id,
                "local_round_id": local_round_id,
                "proxy_round_id": proxy_round_id,
                "answered_in_round": answered_in_round,
                "block_number": block_number,
                "block_hash": block_hash,
                "transaction_hash": transaction_hash,
                "transaction_index": transaction_index,
                "log_index": log_index,
                "removed": bool(log.get("removed", False)),
                "confirmation_block": confirmation_block,
                "confirmation_block_hash": confirmation_hash,
                "raw_answer": raw_answer,
                "decimals": int(metadata["decimals"]),
                "description": str(metadata["description"]),
                "version": int(metadata["version"]),
                "endpoint_ref": self._endpoint_ref,
                "capabilities": dict(capabilities),
                "request": dict(request),
                "manifest": {
                    "id": binding.manifest_id,
                    "hash": binding.manifest_hash,
                    "binding_id": binding.id,
                    "interface_version": CHAINLINK_INTERFACE_VERSION,
                },
                "binding_policy": {
                    "instrument_role": binding.instrument_role,
                    "schedule": dict(binding.schedule),
                    "quality_policy": dict(binding.quality_policy),
                    "risk": dict(binding.risk),
                },
            },
            source_event_material=source_event_material,
        )

    def fetch_history(
        self,
        binding: NumericFactBinding,
        *,
        start: datetime,
        end: datetime,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        config = _ChainlinkConfig.from_binding(binding)
        if binding.endpoint_ref != self._endpoint_ref:
            raise ChainlinkProviderError(
                "chainlink_endpoint_mismatch: binding endpoint_ref differs from adapter"
            )
        start = start if start.tzinfo is not None else start.replace(tzinfo=timezone.utc)
        end = end if end.tzinfo is not None else end.replace(tzinfo=timezone.utc)
        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)
        if end <= start:
            raise ValueError("chainlink_range_invalid: end must follow start")
        if start < config.history_start:
            raise ValueError(
                "chainlink_range_invalid: start precedes manifest history_start "
                f"start={start.isoformat()} history_start={config.history_start.isoformat()}"
            )
        tracker = _Budget(self._transport, budget, binding_id=binding.id)
        _head, confirmed_head, capabilities = self._confirmed_context(tracker, config)
        confirmed_head_time = datetime.fromisoformat(
            str(capabilities["confirmed_head_time"])
        ).astimezone(timezone.utc)
        metadata = self._feed_metadata(tracker, config)
        start_block = (
            confirmed_head
            if start > confirmed_head_time
            else self._block_at_or_after(
                tracker,
                target=start,
                low=config.deployment_block,
                high=confirmed_head,
            )
        )
        end_block = (
            confirmed_head
            if end > confirmed_head_time
            else self._block_at_or_after(
                tracker,
                target=end,
                low=start_block,
                high=confirmed_head,
            )
        )
        if end_block - start_block + 1 > budget.max_blocks:
            raise ChainlinkBudgetExceeded(
                "chainlink_budget_exceeded: "
                f"binding={binding.id} blocks={end_block - start_block + 1}"
            )
        current_phase_id = int(metadata["phase_id"])
        try:
            start_phase_id = _uint_word(
                self._eth_call(
                    tracker,
                    address=config.proxy_address,
                    data=_PHASE_ID,
                    block=hex(start_block),
                )
            )
            end_phase_id = _uint_word(
                self._eth_call(
                    tracker,
                    address=config.proxy_address,
                    data=_PHASE_ID,
                    block=hex(end_block),
                )
            )
            if not (
                0 < start_phase_id <= end_phase_id <= current_phase_id
            ):
                raise ChainlinkProviderError(
                    "chainlink_archive_phase_invalid: "
                    f"start_phase={start_phase_id} end_phase={end_phase_id} "
                    f"current_phase={current_phase_id}"
                )
            required_phase_ids = tuple(
                range(start_phase_id, end_phase_id + 1)
            )
            phase_selection: dict[str, Any] = {
                "method": "proxy_phase_at_bounded_blocks",
                "start_phase_id": start_phase_id,
                "end_phase_id": end_phase_id,
                "required_phase_ids": list(required_phase_ids),
            }
        except ChainlinkProviderError as exc:
            required_phase_ids = tuple(range(1, current_phase_id + 1))
            phase_selection = {
                "method": "all_phases_fallback",
                "required_phase_ids": list(required_phase_ids),
                "archive_phase_error": str(exc),
            }
            logger.warning(
                "chainlink_archive_phase_fallback | binding_id=%s "
                "start_block=%s end_block=%s current_phase_id=%s error=%s",
                binding.id,
                start_block,
                end_block,
                current_phase_id,
                exc,
            )
        phases, phase_failures = self._phase_aggregators(
            tracker,
            config,
            current_phase_id,
            str(metadata["aggregator"]),
            required_phase_ids,
        )
        request = {
            "mode": "historical",
            "start": start.isoformat(),
            "end": end.isoformat(),
            "start_block": start_block,
            "end_block": end_block,
            "confirmed_head_block": confirmed_head,
            "phase_selection": phase_selection,
        }
        observations: dict[str, ProviderNumericObservation] = {}
        gaps: list[ProviderNumericGap] = []
        if end > confirmed_head_time:
            gaps.append(
                ProviderNumericGap(
                    classification="chainlink_range_unconfirmed",
                    start=max(start, confirmed_head_time),
                    end=end,
                    evidence={
                        "confirmed_head_block": confirmed_head,
                        "confirmed_head_time": confirmed_head_time.isoformat(),
                        "requested_end": end.isoformat(),
                        "confirmations": config.confirmations,
                    },
                )
            )
        gaps.extend(
            ProviderNumericGap(
                classification="chainlink_phase_unavailable",
                start=start,
                end=end,
                evidence=dict(failure),
            )
            for failure in phase_failures
        )
        for phase_id, aggregator in sorted(phases.items()):
            try:
                logs = self._logs(
                    tracker,
                    address=aggregator,
                    start_block=start_block,
                    end_block=end_block,
                    max_log_span=config.max_log_span,
                )
            except (ChainlinkRpcError, ChainlinkBudgetExceeded) as exc:
                gaps.append(
                    ProviderNumericGap(
                        classification="chainlink_log_range_unavailable",
                        start=start,
                        end=end,
                        evidence={
                            "phase_id": phase_id,
                            "aggregator_address": aggregator,
                            "error": str(exc),
                        },
                    )
                )
                continue
            for raw_log in logs:
                try:
                    observation = self._observation(
                        tracker,
                        binding=binding,
                        config=config,
                        metadata=metadata,
                        capabilities=capabilities,
                        request=request,
                        phase_id=phase_id,
                        aggregator=aggregator,
                        log=raw_log,
                    )
                except ChainlinkProviderError as exc:
                    block_number: int | None = None
                    gap_start = start
                    gap_location_error: str | None = None
                    try:
                        block_number = _hex_int(
                            raw_log.get("blockNumber") or "0x0"
                        )
                        gap_start = self._block_time(tracker, block_number)
                    except ChainlinkProviderError as location_exc:
                        gap_location_error = str(location_exc)
                        try:
                            gap_start = _datetime_from_epoch(
                                _uint_word(str(raw_log.get("data") or ""), 0)
                            )
                        except ChainlinkProviderError as event_time_exc:
                            gap_location_error = (
                                f"{gap_location_error}; "
                                f"event_time_error={event_time_exc}"
                            )
                    evidence: dict[str, Any] = {
                        "phase_id": phase_id,
                        "block_number": block_number,
                        "error": str(exc),
                    }
                    if gap_location_error is not None:
                        evidence["gap_location_error"] = gap_location_error
                    gaps.append(
                        ProviderNumericGap(
                            classification="chainlink_round_unresolved",
                            start=gap_start,
                            end=gap_start + timedelta(microseconds=1),
                            evidence=evidence,
                        )
                    )
                    continue
                if not start <= observation.effective_at < end:
                    continue
                existing = observations.get(observation.source_event_key)
                if existing is not None and existing != observation:
                    raise ChainlinkProviderError(
                        "chainlink_source_event_conflict: "
                        f"source_event_key={observation.source_event_key}"
                    )
                observations[observation.source_event_key] = observation
        ordered = tuple(
            sorted(
                observations.values(),
                key=lambda item: (item.effective_at, item.source_event_key),
            )
        )
        reconciliation: dict[str, Any]
        try:
            latest_round = _round_result(
                self._eth_call(
                    tracker,
                    address=config.proxy_address,
                    data=_LATEST_ROUND_DATA,
                )
            )
            latest_round_id, _answer, _started_at, latest_updated_at, answered_in = (
                latest_round
            )
            latest_effective_at = _datetime_from_epoch(latest_updated_at)
            latest_in_range = start <= latest_effective_at < end
            matching = next(
                (
                    item
                    for item in ordered
                    if int(item.provenance["proxy_round_id"]) == latest_round_id
                ),
                None,
            )
            reconciliation = {
                "latest_round_id": latest_round_id,
                "latest_updated_at": latest_effective_at.isoformat(),
                "answered_in_round": answered_in,
                "highest_acquired_round_id": (
                    max(
                        int(item.provenance["proxy_round_id"])
                        for item in ordered
                    )
                    if ordered
                    else None
                ),
                "status": (
                    "matched"
                    if matching is not None
                    else "not_evaluable_due_to_existing_gaps"
                    if latest_in_range and gaps
                    else "unreconciled_in_range"
                    if latest_in_range
                    else "outside_requested_range"
                ),
            }
            if latest_in_range and matching is None and not gaps:
                gaps.append(
                    ProviderNumericGap(
                        classification="chainlink_latest_round_unreconciled",
                        start=latest_effective_at,
                        end=latest_effective_at + timedelta(microseconds=1),
                        evidence=dict(reconciliation),
                    )
                )
        except ChainlinkProviderError as exc:
            reconciliation = {"status": "unavailable", "error": str(exc)}
            gaps.append(
                ProviderNumericGap(
                    classification="chainlink_latest_round_unavailable",
                    start=start,
                    end=end,
                    evidence=dict(reconciliation),
                )
            )
        return ProviderNumericBatch(
            observations=ordered,
            gaps=tuple(gaps),
            range_start=start,
            range_end=end,
            source_position_start=str(start_block),
            source_position_end=str(end_block),
            source_position_head=str(confirmed_head),
            status="complete" if not gaps else "partial",
            capabilities=capabilities,
            request={
                **request,
                "rpc_requests": tracker.requests,
                "logs": tracker.logs,
                "latest_round_reconciliation": reconciliation,
            },
            budget_requests_used=tracker.requests,
            budget_logs_used=tracker.logs,
            budget_blocks_scanned=end_block - start_block + 1,
        )

    def fetch_current(
        self,
        binding: NumericFactBinding,
        *,
        budget: NumericAcquisitionBudget,
    ) -> ProviderNumericBatch:
        config = _ChainlinkConfig.from_binding(binding)
        if binding.endpoint_ref != self._endpoint_ref:
            raise ChainlinkProviderError(
                "chainlink_endpoint_mismatch: binding endpoint_ref differs from adapter"
            )
        tracker = _Budget(self._transport, budget, binding_id=binding.id)
        _head, confirmed_head, capabilities = self._confirmed_context(tracker, config)
        metadata = self._feed_metadata(tracker, config)
        phase_id = int(metadata["phase_id"])
        aggregator = str(metadata["aggregator"])
        start_block = max(
            config.deployment_block,
            confirmed_head - config.current_lookback_blocks + 1,
        )
        if confirmed_head - start_block + 1 > budget.max_blocks:
            raise ChainlinkBudgetExceeded(
                "chainlink_budget_exceeded: current lookback exceeds max_blocks"
            )
        logs = self._logs(
            tracker,
            address=aggregator,
            start_block=start_block,
            end_block=confirmed_head,
            max_log_span=config.max_log_span,
        )
        if not logs:
            raise ChainlinkProviderError(
                "chainlink_current_unavailable: no confirmed AnswerUpdated event "
                f"within {config.current_lookback_blocks} blocks"
            )
        latest_log = max(
            logs,
            key=lambda item: (
                _hex_int(item.get("blockNumber") or "0x0"),
                _hex_int(item.get("logIndex") or "0x0"),
            ),
        )
        range_start = self._block_time(tracker, start_block)
        range_end = self._block_time(tracker, confirmed_head) + timedelta(microseconds=1)
        request = {
            "mode": "current",
            "start_block": start_block,
            "end_block": confirmed_head,
            "confirmed_head_block": confirmed_head,
        }
        observation = self._observation(
            tracker,
            binding=binding,
            config=config,
            metadata=metadata,
            capabilities=capabilities,
            request=request,
            phase_id=phase_id,
            aggregator=aggregator,
            log=latest_log,
        )
        latest_round = _round_result(
            self._eth_call(
                tracker,
                address=config.proxy_address,
                data=_LATEST_ROUND_DATA,
            )
        )[0]
        gaps_list: list[ProviderNumericGap] = []
        if latest_round != int(
            observation.provenance["proxy_round_id"]
        ):
            gaps_list.append(
                ProviderNumericGap(
                    classification="chainlink_latest_round_unconfirmed",
                    start=observation.effective_at,
                    end=observation.effective_at + timedelta(microseconds=1),
                    evidence={
                        "latest_round_id": latest_round,
                        "latest_confirmed_round_id": observation.provenance[
                            "proxy_round_id"
                        ],
                    },
                )
            )
        confirmed_head_time = datetime.fromisoformat(
            str(capabilities["confirmed_head_time"])
        ).astimezone(timezone.utc)
        age_seconds = int(
            (confirmed_head_time - observation.effective_at).total_seconds()
        )
        if age_seconds > config.max_staleness_seconds:
            gaps_list.append(
                ProviderNumericGap(
                    classification="chainlink_latest_round_stale",
                    start=observation.effective_at,
                    end=confirmed_head_time + timedelta(microseconds=1),
                    evidence={
                        "age_seconds": age_seconds,
                        "max_staleness_seconds": config.max_staleness_seconds,
                        "stale_behavior": "gap",
                    },
                )
            )
        gaps = tuple(gaps_list)
        return ProviderNumericBatch(
            observations=(observation,),
            gaps=gaps,
            range_start=range_start,
            range_end=range_end,
            source_position_start=str(start_block),
            source_position_end=str(confirmed_head),
            source_position_head=str(confirmed_head),
            status="complete" if not gaps else "partial",
            capabilities=capabilities,
            request={
                **request,
                "rpc_requests": tracker.requests,
                "logs": tracker.logs,
            },
            budget_requests_used=tracker.requests,
            budget_logs_used=tracker.logs,
            budget_blocks_scanned=confirmed_head - start_block + 1,
        )


@dataclass(frozen=True)
class _ChainlinkMvrReserveConfig:
    chain_id: int
    network: str
    proxy_address: str
    feed_id: str
    expected_description: str
    expected_version: int
    confirmations: int
    subject_id: str
    reserve_asset: str
    field_decimals: tuple[int, int]
    max_staleness_seconds: int

    @classmethod
    def from_binding(
        cls, binding: StructuredFactBinding
    ) -> "_ChainlinkMvrReserveConfig":
        if binding.adapter != CHAINLINK_MVR_ADAPTER_ID:
            raise ValueError(
                f"chainlink_mvr_binding_invalid: adapter={binding.adapter}"
            )
        expected_keys = {
            "chain_id",
            "network",
            "proxy_address",
            "feed_id",
            "expected_description",
            "expected_version",
            "confirmations",
            "subject_id",
            "reserve_asset",
            "expected_bundle_fields",
        }
        config = dict(binding.config)
        if set(config) != expected_keys:
            raise ValueError(
                "chainlink_mvr_binding_invalid: config fields must match the reserve v1 schema"
            )
        integer_fields: dict[str, int] = {}
        for field_name in ("chain_id", "expected_version", "confirmations"):
            value = config[field_name]
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(
                    f"chainlink_mvr_binding_invalid: {field_name} must be integer"
                )
            integer_fields[field_name] = value
        if (
            integer_fields["chain_id"] <= 0
            or integer_fields["expected_version"] <= 0
            or integer_fields["confirmations"] < 0
        ):
            raise ValueError("chainlink_mvr_binding_invalid: numeric config")
        network = str(config["network"] or "").strip().lower()
        description = str(config["expected_description"] or "").strip()
        subject_id = str(config["subject_id"] or "").strip()
        reserve_asset = str(config["reserve_asset"] or "").strip().upper()
        if not all((network, description, subject_id, reserve_asset)):
            raise ValueError("chainlink_mvr_binding_invalid: semantic identity")
        if binding.dimensions != {"reserve_asset": reserve_asset}:
            raise ValueError(
                "chainlink_mvr_binding_invalid: reserve asset disagrees with series dimensions"
            )
        feed_id = str(config["feed_id"] or "").strip().lower()
        if (
            len(feed_id) != 34
            or not feed_id.startswith("0x")
            or any(character not in "0123456789abcdef" for character in feed_id[2:])
        ):
            raise ValueError("chainlink_mvr_binding_invalid: feed_id must be bytes16 hex")
        fields = config["expected_bundle_fields"]
        if not isinstance(fields, list) or len(fields) != 2:
            raise ValueError(
                "chainlink_mvr_binding_invalid: reserve bundle requires two fields"
            )
        normalized_fields: list[dict[str, Any]] = []
        for field in fields:
            if not isinstance(field, Mapping) or set(field) != {
                "name",
                "type",
                "decimals",
            }:
                raise ValueError(
                    "chainlink_mvr_binding_invalid: bundle field schema"
                )
            normalized_fields.append(dict(field))
        expected_layout = (
            ("ID", "string", 0),
            ("TotalReserve", "uint256", normalized_fields[1]["decimals"]),
        )
        actual_layout = tuple(
            (field["name"], field["type"], field["decimals"])
            for field in normalized_fields
        )
        if actual_layout != expected_layout:
            raise ValueError(
                "chainlink_mvr_binding_invalid: expected (ID:string, TotalReserve:uint256)"
            )
        reserve_decimals = normalized_fields[1]["decimals"]
        if (
            isinstance(reserve_decimals, bool)
            or not isinstance(reserve_decimals, int)
            or not 0 <= reserve_decimals <= 77
        ):
            raise ValueError("chainlink_mvr_binding_invalid: reserve decimals")
        return cls(
            chain_id=integer_fields["chain_id"],
            network=network,
            proxy_address=_address(config["proxy_address"]),
            feed_id=feed_id,
            expected_description=description,
            expected_version=integer_fields["expected_version"],
            confirmations=integer_fields["confirmations"],
            subject_id=subject_id,
            reserve_asset=reserve_asset,
            field_decimals=(0, reserve_decimals),
            max_staleness_seconds=int(
                binding.quality_policy["max_staleness_seconds"]
            ),
        )


class ChainlinkMvrReserveProvider:
    """Read the latest two-field MVR reserve bundle at a finalized EVM head."""

    adapter_id = CHAINLINK_MVR_ADAPTER_ID

    def __init__(self, transport: JsonRpcTransport, *, endpoint_ref: str) -> None:
        endpoint_ref = str(endpoint_ref or "").strip()
        if not endpoint_ref:
            raise ValueError("chainlink_mvr_rpc_invalid: endpoint_ref is required")
        self._transport = transport
        self._endpoint_ref = endpoint_ref

    def _call(self, method: str, params: Sequence[Any]) -> Any:
        return self._transport.call(method, params)

    def _eth_call(self, *, address: str, data: str, block: str) -> str:
        return str(
            self._call(
                "eth_call",
                [{"to": _address(address), "data": data}, block],
            )
        )

    def fetch_reserve_state(
        self, binding: StructuredFactBinding
    ) -> ProviderReserveStateSnapshot:
        config = _ChainlinkMvrReserveConfig.from_binding(binding)
        chain_id = _hex_int(self._call("eth_chainId", []))
        if chain_id != config.chain_id:
            raise ChainlinkProviderError(
                "chainlink_mvr_chain_mismatch: "
                f"expected={config.chain_id} actual={chain_id}"
            )
        head = _hex_int(self._call("eth_blockNumber", []))
        confirmed_head = head - config.confirmations
        if confirmed_head < 0:
            raise ChainlinkProviderError(
                "chainlink_mvr_finality_unavailable: confirmed head is negative"
            )
        block_tag = hex(confirmed_head)
        block = self._call("eth_getBlockByNumber", [block_tag, False])
        if not isinstance(block, Mapping):
            raise ChainlinkProviderError(
                f"chainlink_mvr_block_unavailable: block={confirmed_head}"
            )
        confirmed_head_hash = _hash32(
            block.get("hash"), field_name="confirmed_head_hash"
        )
        confirmed_head_time = _datetime_from_epoch(_hex_int(block.get("timestamp")))
        description = _string_result(
            self._eth_call(
                address=config.proxy_address,
                data=_DESCRIPTION,
                block=block_tag,
            )
        )
        version = _uint_word(
            self._eth_call(
                address=config.proxy_address,
                data=_VERSION,
                block=block_tag,
            )
        )
        aggregator = _address_word(
            self._eth_call(
                address=config.proxy_address,
                data=_AGGREGATOR,
                block=block_tag,
            )
        )
        decimals = _uint8_array_result(
            self._eth_call(
                address=config.proxy_address,
                data=_BUNDLE_DECIMALS,
                block=block_tag,
            )
        )
        report_timestamp = _uint_word(
            self._eth_call(
                address=config.proxy_address,
                data=_LATEST_BUNDLE_TIMESTAMP,
                block=block_tag,
            )
        )
        bundle = _bytes_result(
            self._eth_call(
                address=config.proxy_address,
                data=_LATEST_BUNDLE,
                block=block_tag,
            )
        )
        if (
            description != config.expected_description
            or version != config.expected_version
            or decimals != config.field_decimals
        ):
            raise ChainlinkProviderError(
                "chainlink_mvr_feed_mismatch: "
                f"proxy={config.proxy_address} description={description!r} "
                f"version={version} decimals={decimals}"
            )
        report_id, raw_quantity = _string_uint256_bundle(bundle)
        if report_id != config.subject_id:
            raise ChainlinkProviderError(
                "chainlink_mvr_subject_mismatch: "
                f"expected={config.subject_id} actual={report_id}"
            )
        observation_time = _datetime_from_epoch(report_timestamp)
        received_at = datetime.now(timezone.utc)
        if observation_time > confirmed_head_time or observation_time > received_at:
            raise ChainlinkProviderError(
                "chainlink_mvr_timestamp_invalid: report follows confirmed observation"
            )
        age_seconds = int((received_at - observation_time).total_seconds())
        if age_seconds > config.max_staleness_seconds:
            raise ChainlinkProviderError(
                "chainlink_mvr_report_stale: "
                f"age_seconds={age_seconds} "
                f"max_staleness_seconds={config.max_staleness_seconds}"
            )
        bundle_hex = f"0x{bundle.hex()}"
        bundle_hash = hashlib.sha256(bundle).hexdigest()
        response_material = {
            "schema_version": "chainlink.mvr_response.v1",
            "chain_id": chain_id,
            "proxy_address": config.proxy_address,
            "feed_id": config.feed_id,
            "aggregator": aggregator,
            "description": description,
            "version": version,
            "bundle_decimals": list(decimals),
            "report_timestamp": report_timestamp,
            "bundle": bundle_hex,
        }
        response_hash = hashlib.sha256(
            json.dumps(
                response_material,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        return ProviderReserveStateSnapshot(
            subject_id=config.subject_id,
            report_id=report_id,
            reserve_asset=config.reserve_asset,
            reserve_quantity=Decimal(raw_quantity).scaleb(-decimals[1]),
            raw_reserve_quantity=str(raw_quantity),
            observation_time=observation_time,
            received_at=received_at,
            response_hash=response_hash,
            source_path=(
                f"evm://{config.chain_id}/{config.proxy_address}/latestBundle"
            ),
            source_event_key=(
                f"{config.feed_id}:{report_timestamp}:{bundle_hash}"
            ),
            metadata={
                **response_material,
                "interface_version": CHAINLINK_MVR_INTERFACE_VERSION,
                "network": config.network,
                "endpoint_ref": self._endpoint_ref,
                "head_block": head,
                "confirmed_head_block": confirmed_head,
                "confirmed_head_hash": confirmed_head_hash,
                "confirmed_head_time": confirmed_head_time.isoformat(),
                "confirmations": config.confirmations,
                "bundle_hash": bundle_hash,
                "raw_reserve_quantity": str(raw_quantity),
                "age_seconds_at_receipt": age_seconds,
            },
        )


__all__ = [
    "CHAINLINK_ADAPTER_ID",
    "CHAINLINK_INTERFACE_VERSION",
    "CHAINLINK_MVR_ADAPTER_ID",
    "CHAINLINK_MVR_INTERFACE_VERSION",
    "ChainlinkAggregatorV3Provider",
    "ChainlinkBudgetExceeded",
    "ChainlinkProviderError",
    "ChainlinkRpcError",
    "ChainlinkMvrReserveProvider",
    "HttpJsonRpcTransport",
    "JsonRpcTransport",
]
