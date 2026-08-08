from __future__ import annotations

from dataclasses import replace
from typing import Any, Sequence

import pytest

from data_providers.numeric_facts import NumericAcquisitionBudget, NumericFactBinding
from data_providers.providers import chainlink
from tests.test_data_providers.test_chainlink_provider import (
    _ENDPOINT_REF,
    _FixtureRpc,
    _Round,
    _address_result,
    _binding,
    _block_time,
    _budget,
    _round_result,
    _rpc,
)


class _EdgeFixtureRpc(_FixtureRpc):
    def __init__(
        self,
        *,
        missing_phases: set[int] | None = None,
        unavailable_round_ids: set[int] | None = None,
        round_overrides: dict[int, _Round] | None = None,
        log_block_hash_override: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.missing_phases = set(missing_phases or set())
        self.unavailable_round_ids = set(unavailable_round_ids or set())
        self.round_overrides = dict(round_overrides or {})
        self.log_block_hash_override = log_block_hash_override

    def call(self, method: str, params: Sequence[Any]) -> Any:
        if method == "eth_call":
            request = dict(params[0])
            data = str(request["data"]).lower()
            selector = data[:10]
            if selector == chainlink._PHASE_AGGREGATORS:
                phase = int(data[10:], 16)
                if phase in self.missing_phases:
                    self.calls.append((method, list(params)))
                    return _address_result("0x" + ("0" * 40))
            if selector == chainlink._GET_ROUND_DATA:
                round_id = int(data[10:], 16)
                if round_id in self.unavailable_round_ids:
                    self.calls.append((method, list(params)))
                    raise chainlink.ChainlinkRpcError(
                        f"fixture_round_unavailable: round_id={round_id}"
                    )
                if round_id in self.round_overrides:
                    self.calls.append((method, list(params)))
                    return _round_result(self.round_overrides[round_id])
        if method == "eth_getLogs" and self.log_block_hash_override is not None:
            result = super().call(method, params)
            return [
                {**item, "blockHash": self.log_block_hash_override}
                for item in result
            ]
        return super().call(method, params)


def _edge_rpc(
    binding: NumericFactBinding,
    *,
    phase_id: int,
    rounds: Sequence[_Round],
    missing_phases: set[int] | None = None,
    unavailable_round_ids: set[int] | None = None,
    round_overrides: dict[int, _Round] | None = None,
    log_block_hash_override: str | None = None,
) -> _EdgeFixtureRpc:
    missing = set(missing_phases or set())
    aggregators = {
        phase: f"0x{phase:040x}"
        for phase in range(1, phase_id + 1)
        if phase not in missing
    }
    return _EdgeFixtureRpc(
        proxy_address=str(binding.config["proxy_address"]),
        decimals=int(binding.config["expected_decimals"]),
        description=str(binding.config["expected_description"]),
        phase_id=phase_id,
        aggregators=aggregators,
        rounds=rounds,
        missing_phases=missing,
        unavailable_round_ids=unavailable_round_ids,
        round_overrides=round_overrides,
        log_block_hash_override=log_block_hash_override,
    )


def test_current_stale_round_is_returned_with_explicit_partial_gap() -> None:
    binding = _binding("eth-usd", phase_id=1, confirmations=2)
    binding = replace(
        binding,
        quality_policy={
            "max_staleness_seconds": 60,
            "stale_behavior": "gap",
        },
    )
    stale = _Round(1, 9, 191_428_523_541, 100)
    rpc = _rpc(binding, phase_id=1, rounds=(stale,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_current(binding, budget=_budget())

    assert batch.status == "partial"
    assert len(batch.observations) == 1
    assert batch.observations[0].provenance["proxy_round_id"] == stale.proxy_round_id
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_latest_round_stale"
    ]
    assert batch.gaps[0].evidence == {
        "age_seconds": 219,
        "max_staleness_seconds": 60,
        "stale_behavior": "gap",
    }


def test_round_at_confirmed_event_boundary_applies_confirmation_depth_once() -> None:
    binding = _binding("eth-usd", phase_id=1, confirmations=2)
    confirmed_at_boundary = _Round(1, 10, 191_428_523_541, 118)
    rpc = _rpc(binding, phase_id=1, rounds=(confirmed_at_boundary,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_current(binding, budget=_budget())

    assert batch.status == "complete"
    assert len(batch.observations) == 1
    observation = batch.observations[0]
    assert observation.provenance["block_number"] == 118
    assert observation.provenance["confirmation_block"] == 120
    assert observation.known_at == _block_time(120)


def test_history_beyond_confirmed_head_preserves_confirmed_data_and_marks_range_partial() -> None:
    binding = _binding("eth-usd", phase_id=1, confirmations=2, max_log_span=20)
    confirmed = _Round(1, 9, 191_428_523_541, 115)
    rpc = _rpc(binding, phase_id=1, rounds=(confirmed,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )
    requested_end = _block_time(121)

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=requested_end,
        budget=_budget(),
    )

    assert batch.status == "partial"
    assert [item.provenance["proxy_round_id"] for item in batch.observations] == [
        confirmed.proxy_round_id
    ]
    assert batch.range_end == requested_end
    assert batch.source_position_end == "118"
    unconfirmed = [
        gap for gap in batch.gaps if gap.classification == "chainlink_range_unconfirmed"
    ]
    assert len(unconfirmed) == 1
    assert unconfirmed[0].start == _block_time(118)
    assert unconfirmed[0].end == requested_end


def test_history_rejects_start_before_manifest_history_without_rpc() -> None:
    binding = _binding("eth-usd", phase_id=1)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _rpc(binding, phase_id=1, rounds=(round_,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    with pytest.raises(
        ValueError,
        match="start precedes manifest history_start",
    ):
        provider.fetch_history(
            binding,
            start=_block_time(89),
            end=_block_time(110),
            budget=_budget(),
        )

    assert rpc.calls == []


def test_provider_rejects_lossy_numeric_config_coercion_without_rpc() -> None:
    binding = _binding("eth-usd", phase_id=1)
    binding = replace(
        binding,
        config={**dict(binding.config), "confirmations": 12.5},
    )
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _rpc(binding, phase_id=1, rounds=(round_,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    with pytest.raises(ValueError, match="confirmations must be an integer"):
        provider.fetch_current(binding, budget=_budget())

    assert rpc.calls == []


@pytest.mark.parametrize("round_failure", ["missing", "invalid"])
def test_unresolvable_round_is_an_explicit_gap_without_synthesized_observation(
    round_failure: str,
) -> None:
    binding = _binding("eth-usd", phase_id=1, max_log_span=20)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _edge_rpc(
        binding,
        phase_id=1,
        rounds=(round_,),
        unavailable_round_ids=(
            {round_.proxy_round_id} if round_failure == "missing" else None
        ),
        round_overrides=(
            {
                round_.proxy_round_id: replace(
                    round_,
                    answer=round_.answer + 1,
                )
            }
            if round_failure == "invalid"
            else None
        ),
    )
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=_budget(),
    )

    assert batch.status == "partial"
    assert batch.observations == ()
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_round_unresolved"
    ]
    assert batch.gaps[0].evidence["phase_id"] == 1
    assert batch.gaps[0].evidence["block_number"] == 105
    assert (
        "fixture_round_unavailable"
        if round_failure == "missing"
        else "chainlink_round_reconciliation_failed"
    ) in batch.gaps[0].evidence["error"]


def test_log_block_hash_mismatch_is_an_explicit_gap() -> None:
    binding = _binding("eth-usd", phase_id=1, max_log_span=20)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _edge_rpc(
        binding,
        phase_id=1,
        rounds=(round_,),
        log_block_hash_override="0x" + ("f" * 64),
    )
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=_budget(),
    )

    assert batch.status == "partial"
    assert batch.observations == ()
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_round_unresolved"
    ]
    assert "chainlink_log_block_mismatch" in batch.gaps[0].evidence["error"]


@pytest.mark.parametrize("answer", [0, -1])
def test_reference_price_outside_contract_domain_is_an_explicit_gap(
    answer: int,
) -> None:
    binding = _binding("eth-usd", phase_id=1, max_log_span=20)
    round_ = _Round(1, 1, answer, 105)
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

    assert batch.status == "partial"
    assert batch.observations == ()
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_round_unresolved"
    ]
    assert "chainlink_answer_contract_invalid" in batch.gaps[0].evidence["error"]


def test_missing_historical_phase_is_an_explicit_partial_phase_gap() -> None:
    binding = _binding("eth-usd", phase_id=2, max_log_span=20)
    current_phase_round = _Round(2, 7, 191_428_523_541, 106)
    rpc = _edge_rpc(
        binding,
        phase_id=2,
        rounds=(current_phase_round,),
        missing_phases={1},
    )
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=_budget(),
    )

    assert batch.status == "partial"
    assert [item.provenance["phase_id"] for item in batch.observations] == [2]
    assert [gap.classification for gap in batch.gaps] == [
        "chainlink_phase_unavailable"
    ]
    assert batch.gaps[0].evidence == {
        "phase_id": 1,
        "error": "chainlink_phase_missing: phase_id=1",
    }


def test_history_batch_exposes_coherent_range_status_and_budget_usage() -> None:
    binding = _binding("eth-usd", phase_id=1, max_log_span=20)
    round_ = _Round(1, 1, 191_428_523_541, 105)
    rpc = _rpc(binding, phase_id=1, rounds=(round_,))
    provider = chainlink.ChainlinkAggregatorV3Provider(
        rpc, endpoint_ref=_ENDPOINT_REF
    )
    budget = _budget()

    batch = provider.fetch_history(
        binding,
        start=_block_time(100),
        end=_block_time(110),
        budget=budget,
    )

    assert batch.status == "complete"
    assert batch.range_start == _block_time(100)
    assert batch.range_end == _block_time(110)
    assert batch.source_position_start == "100"
    assert batch.source_position_end == "110"
    assert batch.source_position_head == "118"
    assert 0 < batch.budget_requests_used <= budget.max_requests
    assert batch.budget_logs_used == 1
    assert batch.budget_blocks_scanned == 11


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"max_requests": 0}, "max_requests must be positive"),
        ({"max_logs": 0}, "max_logs must be positive"),
        ({"max_blocks": 0}, "max_blocks must be positive"),
        ({"max_retries": -1}, "max_retries must be nonnegative"),
        ({"max_requests": "1"}, "max_requests must be positive integer"),
    ],
)
def test_acquisition_budget_rejects_nonpositive_primitives(
    overrides: dict[str, object],
    message: str,
) -> None:
    values = {
        "max_requests": 1,
        "max_logs": 1,
        "max_blocks": 1,
        "max_retries": 0,
        **overrides,
    }

    with pytest.raises(ValueError, match=message):
        NumericAcquisitionBudget(**values)
