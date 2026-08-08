from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import pytest

from data_providers.numeric_facts import (
    NumericAcquisitionBudget,
    NumericFactBinding,
    load_numeric_fact_manifest,
)
from data_providers.providers import chainlink


_ROOT = Path(__file__).resolve().parents[2]
_MANIFEST_DIR = _ROOT / "config" / "market-data" / "numeric-facts"
_VERIFIED_FIXTURE = (
    _ROOT
    / "tests"
    / "fixtures"
    / "providers"
    / "chainlink"
    / "aggregator_v3"
    / "verified_current.json"
)
_ENDPOINT_REF = "CHAINLINK_ETHEREUM_RPC_URL"
_BASE_EPOCH = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp())


@dataclass(frozen=True)
class _Round:
    phase_id: int
    local_round_id: int
    answer: int
    block_number: int

    @property
    def proxy_round_id(self) -> int:
        return (self.phase_id << 64) | self.local_round_id

    @property
    def updated_at(self) -> int:
        return _block_epoch(self.block_number) - 3


class _FixtureRpc:
    def __init__(
        self,
        *,
        proxy_address: str,
        decimals: int,
        description: str,
        phase_id: int,
        aggregators: dict[int, str],
        rounds: Sequence[_Round],
        head: int = 120,
        version: int = 6,
        latest_round_id: int | None = None,
        transient_failures: dict[str, int] | None = None,
        deny_log_addresses: set[str] | None = None,
    ) -> None:
        self.proxy_address = proxy_address.lower()
        self.decimals = decimals
        self.description = description
        self.phase_id = phase_id
        self.aggregators = {
            int(phase): address.lower() for phase, address in aggregators.items()
        }
        self.rounds = {item.proxy_round_id: item for item in rounds}
        self.head = head
        self.version = version
        self.latest_round_id = latest_round_id or max(self.rounds)
        self.transient_failures = dict(transient_failures or {})
        self.deny_log_addresses = {
            address.lower() for address in (deny_log_addresses or set())
        }
        self.calls: list[tuple[str, list[Any]]] = []

    def call(self, method: str, params: Sequence[Any]) -> Any:
        copied_params = list(params)
        self.calls.append((method, copied_params))
        remaining = self.transient_failures.get(method, 0)
        if remaining > 0:
            self.transient_failures[method] = remaining - 1
            raise chainlink.ChainlinkRpcError(
                f"fixture_rpc_denied: method={method}"
            )

        if method == "eth_chainId":
            return "0x1"
        if method == "eth_blockNumber":
            return hex(self.head)
        if method == "eth_getBlockByNumber":
            block_number = int(str(params[0]), 16)
            return {
                "number": hex(block_number),
                "hash": _hash(block_number),
                "timestamp": hex(_block_epoch(block_number)),
            }
        if method == "eth_getLogs":
            request = dict(params[0])
            address = str(request["address"]).lower()
            if address in self.deny_log_addresses:
                raise chainlink.ChainlinkRpcError(
                    f"fixture_rpc_denied: method=eth_getLogs address={address}"
                )
            start = int(str(request["fromBlock"]), 16)
            end = int(str(request["toBlock"]), 16)
            phase = next(
                phase
                for phase, aggregator in self.aggregators.items()
                if aggregator == address
            )
            return [
                _log(item, log_index=index)
                for index, item in enumerate(self.rounds.values())
                if item.phase_id == phase and start <= item.block_number <= end
            ]
        if method == "eth_call":
            request = dict(params[0])
            address = str(request["to"]).lower()
            data = str(request["data"]).lower()
            selector = data[:10]
            if address != self.proxy_address:
                raise AssertionError(f"unexpected eth_call address: {address}")
            if selector == chainlink._DECIMALS:
                return _uint_result(self.decimals)
            if selector == chainlink._DESCRIPTION:
                return _string_result(self.description)
            if selector == chainlink._VERSION:
                return _uint_result(self.version)
            if selector == chainlink._PHASE_ID:
                return _uint_result(self.phase_id)
            if selector == chainlink._AGGREGATOR:
                return _address_result(self.aggregators[self.phase_id])
            if selector == chainlink._PHASE_AGGREGATORS:
                phase = int(data[10:], 16)
                return _address_result(self.aggregators[phase])
            if selector == chainlink._GET_ROUND_DATA:
                round_id = int(data[10:], 16)
                return _round_result(self.rounds[round_id])
            if selector == chainlink._LATEST_ROUND_DATA:
                return _round_result(self.rounds[self.latest_round_id])
            raise AssertionError(f"unexpected eth_call selector: {selector}")
        raise AssertionError(f"unexpected RPC method: {method}")


def test_history_is_phase_aware_exact_and_pages_bounded_log_ranges() -> None:
    binding = _binding("eth-usd", phase_id=2, max_log_span=3)
    rounds = (
        _Round(phase_id=1, local_round_id=4, answer=180_000_000_000, block_number=94),
        _Round(phase_id=2, local_round_id=7, answer=191_428_523_541, block_number=106),
    )
    rpc = _rpc(binding, phase_id=2, rounds=rounds)
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(90),
        end=_block_time(110),
        budget=_budget(),
    )

    assert batch.status == "complete"
    assert batch.gaps == ()
    assert batch.source_position_start == "90"
    assert batch.source_position_end == "110"
    assert [item.value for item in batch.observations] == [
        Decimal("1800"),
        Decimal("1914.28523541"),
    ]
    assert [item.raw_value for item in batch.observations] == [
        "180000000000",
        "191428523541",
    ]
    assert [item.provenance["phase_id"] for item in batch.observations] == [1, 2]
    assert [item.provenance["proxy_round_id"] for item in batch.observations] == [
        (1 << 64) | 4,
        (2 << 64) | 7,
    ]
    assert batch.observations[1].source_event_key == (
        f"evm:1:{binding.config['proxy_address'].lower()}:{(2 << 64) | 7}:answer"
    )

    log_calls = _log_calls(rpc)
    assert log_calls
    assert all(
        int(call["toBlock"], 16) - int(call["fromBlock"], 16) + 1 <= 3
        for call in log_calls
    )
    for aggregator in rpc.aggregators.values():
        ranges = [
            (int(call["fromBlock"], 16), int(call["toBlock"], 16))
            for call in log_calls
            if str(call["address"]).lower() == aggregator
        ]
        assert ranges[0][0] == 90
        assert ranges[-1][1] == 110
        assert all(left_end + 1 == right_start for (_, left_end), (right_start, _) in zip(ranges, ranges[1:]))


def test_exact_large_reserve_value_and_verified_proxy_round_reconcile() -> None:
    verified = _verified("tusd-reserves")
    binding = _binding("tusd-reserves", phase_id=int(verified["phase_id"]))
    round_ = _Round(
        phase_id=int(verified["phase_id"]),
        local_round_id=int(verified["local_round_id"]),
        answer=int(verified["raw_answer"]),
        block_number=115,
    )
    rpc = _rpc(binding, phase_id=round_.phase_id, rounds=(round_,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_current(binding, budget=_budget())

    assert batch.status == "complete"
    assert batch.gaps == ()
    observation = batch.observations[0]
    assert observation.raw_value == verified["raw_answer"]
    assert observation.value == Decimal(verified["normalized_answer"])
    assert not isinstance(observation.value, float)
    assert observation.provenance["local_round_id"] == verified["local_round_id"]
    assert str(observation.provenance["proxy_round_id"]) == verified["proxy_round_id"]
    assert observation.provenance["decimals"] == verified["decimals"]
    assert observation.provenance["description"] == verified["description"]


def test_block_publication_and_confirmation_clocks_remain_distinct() -> None:
    binding = _binding("eth-usd", phase_id=1, confirmations=2)
    round_ = _Round(
        phase_id=1,
        local_round_id=9,
        answer=191_428_523_541,
        block_number=105,
    )
    rpc = _rpc(binding, phase_id=1, rounds=(round_,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=_budget(),
    )

    observation = batch.observations[0]
    assert observation.effective_at == datetime.fromtimestamp(
        round_.updated_at, tz=UTC
    )
    assert observation.source_published_at == _block_time(round_.block_number)
    assert observation.known_at == _block_time(round_.block_number + 2)
    assert observation.effective_at < observation.source_published_at < observation.known_at
    assert observation.known_at_method == "evm_confirmation_block"


def test_current_reconciliation_marks_newer_unconfirmed_proxy_round_partial() -> None:
    binding = _binding("eth-usd", phase_id=1, confirmations=2)
    confirmed = _Round(1, 9, 191_428_523_541, 115)
    latest = _Round(1, 10, 191_500_000_000, 119)
    rpc = _rpc(
        binding,
        phase_id=1,
        rounds=(confirmed, latest),
        latest_round_id=latest.proxy_round_id,
    )
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_current(binding, budget=_budget())

    assert batch.status == "partial"
    assert len(batch.observations) == 1
    assert batch.observations[0].provenance["proxy_round_id"] == confirmed.proxy_round_id
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_latest_round_unconfirmed"
    ]
    assert batch.gaps[0].evidence == {
        "latest_round_id": latest.proxy_round_id,
        "latest_confirmed_round_id": confirmed.proxy_round_id,
    }


def test_metadata_mismatch_quarantines_feed_before_log_acquisition() -> None:
    binding = _binding("eth-usd", phase_id=1)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _rpc(binding, phase_id=1, rounds=(round_,), decimals=18)
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    with pytest.raises(chainlink.ChainlinkProviderError, match="chainlink_feed_mismatch"):
        provider.fetch_history(
            binding,
            start=_block_time(100),
            end=_block_time(110),
            budget=_budget(),
        )

    assert _log_calls(rpc) == []


def test_transient_rpc_denial_retries_same_bounded_log_page() -> None:
    binding = _binding("eth-usd", phase_id=1, max_log_span=20)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _rpc(
        binding,
        phase_id=1,
        rounds=(round_,),
        transient_failures={"eth_getLogs": 1},
    )
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=_budget(max_retries=1),
    )

    assert batch.status == "complete"
    assert len(batch.observations) == 1
    assert len(_log_calls(rpc)) == 2
    assert _log_calls(rpc)[0] == _log_calls(rpc)[1]


def test_persistent_log_capability_denial_is_explicit_partial_gap() -> None:
    binding = _binding("eth-usd", phase_id=1, max_log_span=20)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _rpc(binding, phase_id=1, rounds=(round_,))
    rpc.deny_log_addresses.add(rpc.aggregators[1])
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=_budget(max_retries=1),
    )

    assert batch.status == "partial"
    assert batch.observations == ()
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_log_range_unavailable"
    ]
    assert batch.gaps[0].evidence["phase_id"] == 1
    assert "fixture_rpc_denied" in batch.gaps[0].evidence["error"]


def _binding(
    binding_id: str,
    *,
    phase_id: int,
    confirmations: int = 2,
    max_log_span: int = 4,
) -> NumericFactBinding:
    filename = (
        "chainlink-eth-usd.reference.json"
        if binding_id == "eth-usd"
        else "chainlink-tusd-reserves.reference.json"
    )
    binding = load_numeric_fact_manifest(_MANIFEST_DIR / filename).binding(
        binding_id, require_enabled=False
    )
    config = {
        **binding.config,
        "deployment_block": 90,
        "history_start": _block_time(90).isoformat(),
        "confirmations": confirmations,
        "max_log_span": max_log_span,
        "current_lookback_blocks": 20,
        "expected_version": 6,
    }
    assert phase_id >= 1
    return replace(binding, enabled=True, config=config)


def _rpc(
    binding: NumericFactBinding,
    *,
    phase_id: int,
    rounds: Sequence[_Round],
    decimals: int | None = None,
    latest_round_id: int | None = None,
    transient_failures: dict[str, int] | None = None,
) -> _FixtureRpc:
    aggregators = {
        phase: f"0x{phase:040x}" for phase in range(1, phase_id + 1)
    }
    return _FixtureRpc(
        proxy_address=str(binding.config["proxy_address"]),
        decimals=(
            int(binding.config["expected_decimals"])
            if decimals is None
            else decimals
        ),
        description=str(binding.config["expected_description"]),
        phase_id=phase_id,
        aggregators=aggregators,
        rounds=rounds,
        latest_round_id=latest_round_id,
        transient_failures=transient_failures,
    )


def _budget(*, max_retries: int = 0) -> NumericAcquisitionBudget:
    return NumericAcquisitionBudget(
        max_requests=250,
        max_logs=100,
        max_blocks=100,
        max_retries=max_retries,
    )


def _verified(binding_id: str) -> dict[str, Any]:
    payload = json.loads(_VERIFIED_FIXTURE.read_text(encoding="utf-8"))
    return next(
        item for item in payload["observations"] if item["binding_id"] == binding_id
    )


def _block_epoch(block_number: int) -> int:
    return _BASE_EPOCH + int(block_number) * 12


def _block_time(block_number: int) -> datetime:
    return datetime.fromtimestamp(_block_epoch(block_number), tz=UTC)


def _hash(value: int) -> str:
    return "0x" + int(value).to_bytes(32, "big").hex()


def _uint_word(value: int) -> str:
    return int(value).to_bytes(32, "big", signed=False).hex()


def _int_word(value: int) -> str:
    return int(value).to_bytes(32, "big", signed=True).hex()


def _uint_result(value: int) -> str:
    return "0x" + _uint_word(value)


def _address_result(address: str) -> str:
    return "0x" + ("00" * 12) + address.lower()[2:]


def _string_result(value: str) -> str:
    encoded = value.encode("utf-8")
    padded_length = ((len(encoded) + 31) // 32) * 32
    return (
        "0x"
        + _uint_word(32)
        + _uint_word(len(encoded))
        + encoded.hex().ljust(padded_length * 2, "0")
    )


def _round_result(round_: _Round) -> str:
    return (
        "0x"
        + _uint_word(round_.proxy_round_id)
        + _int_word(round_.answer)
        + _uint_word(round_.updated_at - 20)
        + _uint_word(round_.updated_at)
        + _uint_word(round_.proxy_round_id)
    )


def _log(round_: _Round, *, log_index: int) -> dict[str, Any]:
    return {
        "address": f"0x{round_.phase_id:040x}",
        "topics": [
            chainlink._ANSWER_UPDATED_TOPIC,
            "0x" + _int_word(round_.answer),
            "0x" + _uint_word(round_.local_round_id),
        ],
        "data": "0x" + _uint_word(round_.updated_at),
        "blockNumber": hex(round_.block_number),
        "blockHash": _hash(round_.block_number),
        "transactionHash": _hash(round_.proxy_round_id),
        "transactionIndex": "0x0",
        "logIndex": hex(log_index),
        "removed": False,
    }


def _log_calls(rpc: _FixtureRpc) -> list[dict[str, Any]]:
    return [dict(params[0]) for method, params in rpc.calls if method == "eth_getLogs"]
