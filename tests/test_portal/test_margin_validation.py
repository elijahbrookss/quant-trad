"""Tests for margin-based wallet validation for futures instruments.

Uses the example instrument from the task:
- Instrument: BIP-20DEC30-CDE
- instrument_type: "future"
- contract_size: 0.01
- tick_size: 5
- tick_value: 0.05
- can_short: true
- short_requires_borrow: false
- intraday_margin_rate: { long: 0.1000185, short: 0.1000008 }
- overnight_margin_rate: { long: 0.245625, short: 0.306375 }

Observed log values:
- price: 110660
- qty: 8.06451613
- notional: 8924.19354839
- fee: 5.35451613
"""

import pytest

from engines.bot_runtime.core.margin import (
    MarginRates,
    MarginSessionType,
    SpotMarginCalculator,
    FuturesMarginCalculator,
    MaxQtyByMargin,
    extract_margin_rates,
    resolve_instrument_type,
    create_margin_calculator,
    calculate_max_qty_by_margin,
    InstrumentType,
)
from engines.bot_runtime.core.wallet import (
    WalletLedger,
    WalletState,
    wallet_can_apply,
)


# Example Coinbase futures instrument from the task
COINBASE_FUTURE_INSTRUMENT = {
    "symbol": "BIP-20DEC30-CDE",
    "instrument_type": "future",
    "contract_size": 0.01,
    "tick_size": 5,
    "tick_value": 0.05,
    "can_short": True,
    "short_requires_borrow": False,
    "quote_currency": "USD",
    "metadata": {
        "base_currency": "BTC",
        "info": {
            "future_product_details": {
                "intraday_margin_rate": {
                    "long_margin_rate": "0.1000185",
                    "short_margin_rate": "0.1000008",
                },
                "overnight_margin_rate": {
                    "long_margin_rate": "0.245625",
                    "short_margin_rate": "0.306375",
                },
            }
        },
    },
}

# Example spot instrument (no margin rates)
SPOT_INSTRUMENT = {
    "symbol": "BTC/USD",
    "instrument_type": "spot",
    "contract_size": 1.0,
    "tick_size": 0.01,
    "tick_value": 0.01,
    "can_short": True,
    "short_requires_borrow": False,
    "quote_currency": "USD",
    "metadata": {
        "base_currency": "BTC",
    },
}

# Misconfigured future (missing margin rates)
MISCONFIGURED_FUTURE = {
    "symbol": "BROKEN-FUTURE",
    "instrument_type": "future",
    "contract_size": 0.01,
    "tick_size": 5,
    "can_short": True,
    "short_requires_borrow": False,
    "metadata": {
        "base_currency": "BTC",
        # Missing info.future_product_details with margin rates
    },
}


class TestMarginRates:
    """Test MarginRates dataclass and rate selection."""

    def test_extract_margin_rates_from_coinbase_future(self):
        """Verify margin rates are correctly extracted from Coinbase futures metadata."""
        rates = extract_margin_rates(COINBASE_FUTURE_INSTRUMENT)
        assert rates is not None
        assert rates.intraday_long == pytest.approx(0.1000185)
        assert rates.intraday_short == pytest.approx(0.1000008)
        assert rates.overnight_long == pytest.approx(0.245625)
        assert rates.overnight_short == pytest.approx(0.306375)

    def test_extract_margin_rates_returns_none_for_spot(self):
        """Spot instruments should return None for margin rates."""
        rates = extract_margin_rates(SPOT_INSTRUMENT)
        assert rates is None

    def test_extract_margin_rates_returns_none_for_missing_metadata(self):
        """Missing metadata should return None."""
        rates = extract_margin_rates({})
        assert rates is None

    def test_margin_rate_selection_intraday_short(self):
        """Intraday short rate should be selected correctly."""
        rates = MarginRates(
            intraday_long=0.10,
            intraday_short=0.1000008,
            overnight_long=0.25,
            overnight_short=0.306375,
        )
        rate = rates.get_rate("short", MarginSessionType.INTRADAY)
        assert rate == pytest.approx(0.1000008)

    def test_margin_rate_selection_overnight_short(self):
        """Overnight short rate should be selected correctly."""
        rates = MarginRates(
            intraday_long=0.10,
            intraday_short=0.10,
            overnight_long=0.25,
            overnight_short=0.306375,
        )
        rate = rates.get_rate("short", MarginSessionType.OVERNIGHT)
        assert rate == pytest.approx(0.306375)

    def test_margin_rate_selection_unknown_defaults_to_overnight(self):
        """UNKNOWN session should default to overnight (conservative)."""
        rates = MarginRates(
            intraday_long=0.10,
            intraday_short=0.10,
            overnight_long=0.25,
            overnight_short=0.306375,
        )
        rate = rates.get_rate("short", MarginSessionType.UNKNOWN)
        assert rate == pytest.approx(0.306375)


class TestInstrumentTypeResolution:
    """Test instrument type resolution."""

    def test_resolve_future_type(self):
        """Future instrument type should resolve correctly."""
        assert resolve_instrument_type(COINBASE_FUTURE_INSTRUMENT) == InstrumentType.FUTURE

    def test_resolve_spot_type(self):
        """Spot instrument type should resolve correctly."""
        assert resolve_instrument_type(SPOT_INSTRUMENT) == InstrumentType.SPOT

    def test_resolve_unknown_type(self):
        """Unknown instrument type should resolve to UNKNOWN."""
        assert resolve_instrument_type({}) == InstrumentType.UNKNOWN
        assert resolve_instrument_type({"instrument_type": "option"}) == InstrumentType.UNKNOWN


class TestMarginCalculators:
    """Test margin calculators."""

    def test_spot_calculator_requires_full_notional(self):
        """Spot calculator should require full notional."""
        calc = SpotMarginCalculator()
        result = calc.calculate(
            notional=8924.19,
            fee=5.35,
            direction="short",
            session=MarginSessionType.INTRADAY,
        )
        assert result.calculation_method == "SPOT_CASH_SHORT_COVER"
        assert result.margin_rate == 1.0
        assert result.required_margin == pytest.approx(8924.19)
        assert result.fee_buffer == pytest.approx(5.35 * 2)  # Double fee
        assert result.total_required == pytest.approx(8924.19 + 10.70)

    def test_futures_calculator_uses_margin_rate(self):
        """Futures calculator should use margin rate."""
        rates = MarginRates(
            intraday_long=0.10,
            intraday_short=0.1000008,
            overnight_long=0.25,
            overnight_short=0.306375,
        )
        calc = FuturesMarginCalculator(rates, safety_multiplier=1.05)

        # Test with observed values from task
        result = calc.calculate(
            notional=8924.19354839,
            fee=5.35451613,
            direction="short",
            session=MarginSessionType.INTRADAY,
        )

        assert result.calculation_method == "FUTURES_MARGIN_INTRADAY"
        assert result.margin_rate == pytest.approx(0.1000008)
        expected_margin = 8924.19354839 * 0.1000008  # ~892.43
        assert result.required_margin == pytest.approx(expected_margin)
        assert result.session_type == "intraday"

    def test_futures_calculator_overnight_is_more_conservative(self):
        """Overnight margin should be higher than intraday."""
        rates = extract_margin_rates(COINBASE_FUTURE_INSTRUMENT)
        calc = FuturesMarginCalculator(rates, safety_multiplier=1.05)

        intraday = calc.calculate(
            notional=8924.19,
            fee=5.35,
            direction="short",
            session=MarginSessionType.INTRADAY,
        )
        overnight = calc.calculate(
            notional=8924.19,
            fee=5.35,
            direction="short",
            session=MarginSessionType.OVERNIGHT,
        )

        # Overnight short rate (0.306375) > Intraday short rate (0.1000008)
        assert overnight.required_margin > intraday.required_margin
        assert overnight.margin_rate > intraday.margin_rate


class TestCreateMarginCalculator:
    """Test the calculator factory function."""

    def test_future_instrument_creates_futures_calculator(self):
        """Future instrument should create FuturesMarginCalculator."""
        calc, calc_type = create_margin_calculator(COINBASE_FUTURE_INSTRUMENT)
        assert calc_type == "margin"
        assert isinstance(calc, FuturesMarginCalculator)

    def test_spot_instrument_creates_spot_calculator(self):
        """Spot instrument should create SpotMarginCalculator."""
        calc, calc_type = create_margin_calculator(SPOT_INSTRUMENT)
        assert calc_type == "full_notional"
        assert isinstance(calc, SpotMarginCalculator)

    def test_misconfigured_future_raises_error(self):
        """Future without margin rates should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            create_margin_calculator(MISCONFIGURED_FUTURE)
        assert "no margin rates found" in str(exc_info.value)
        assert "future" in str(exc_info.value)


class TestWalletValidationWithMargin:
    """Test wallet validation with margin-based requirements.

    Uses the exact values from the observed logs:
    - price: 110660
    - qty: 8.06451613
    - notional: 8924.19354839
    - fee: 5.35451613
    - required (old): 8934.90258065 (full notional + 2x fee)
    - available: 500
    """

    def test_futures_short_with_intraday_margin_succeeds(self):
        """Futures short with sufficient margin should succeed.

        With intraday margin (~10%), we need ~$892.43 margin, not $8924.
        With $1000 available, this should pass.
        """
        ledger = WalletLedger()
        ledger.deposit({"USD": 1000})
        state = ledger.project()

        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=8.06451613,
            notional=8924.19354839,
            fee=5.35451613,
            short_requires_borrow=False,
            instrument=COINBASE_FUTURE_INSTRUMENT,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert allowed is True
        assert reason is None

    def test_futures_short_with_insufficient_margin_fails(self):
        """Futures short with insufficient margin should fail with WALLET_INSUFFICIENT_MARGIN."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 500})  # Not enough even for ~10% margin
        state = ledger.project()

        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=8.06451613,
            notional=8924.19354839,
            fee=5.35451613,
            short_requires_borrow=False,
            instrument=COINBASE_FUTURE_INSTRUMENT,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert allowed is False
        assert reason == "WALLET_INSUFFICIENT_MARGIN"
        assert payload["margin_type"] == "FUTURES_MARGIN_INTRADAY"
        assert payload["session"] == "intraday"
        # Margin should be ~10% of notional, not full notional
        assert payload["required"] < 8924.19354839  # Much less than full notional

    def test_futures_short_with_overnight_margin_requires_more(self):
        """Overnight margin requires more collateral than intraday."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 2500})  # Enough for intraday, might not be enough for overnight
        state = ledger.project()

        # First check intraday passes
        allowed_intraday, _, _ = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=8.06451613,
            notional=8924.19354839,
            fee=5.35451613,
            short_requires_borrow=False,
            instrument=COINBASE_FUTURE_INSTRUMENT,
            margin_session=MarginSessionType.INTRADAY,
        )
        assert allowed_intraday is True

        # Now check overnight might fail (overnight short rate is ~30.6%)
        allowed_overnight, reason, payload = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=8.06451613,
            notional=8924.19354839,
            fee=5.35451613,
            short_requires_borrow=False,
            instrument=COINBASE_FUTURE_INSTRUMENT,
            margin_session=MarginSessionType.OVERNIGHT,
        )

        # Overnight requires ~30.6% = ~$2732, which is > $2500
        assert allowed_overnight is False
        assert reason == "WALLET_INSUFFICIENT_MARGIN"
        assert payload["margin_rate"] == pytest.approx(0.306375)

    def test_spot_short_requires_full_notional(self):
        """Spot short should require full notional (cash-secured)."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 5000})  # Not enough for full notional of ~$8924
        state = ledger.project()

        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=8.06451613,
            notional=8924.19354839,
            fee=5.35451613,
            short_requires_borrow=False,
            instrument=SPOT_INSTRUMENT,
        )

        assert allowed is False
        assert reason == "WALLET_INSUFFICIENT_CASH_FOR_SHORT_COVER"
        # For spot, required should be full notional + 2x fee
        expected_required = 8924.19354839 + (5.35451613 * 2)
        assert payload["required"] == pytest.approx(expected_required)

    def test_misconfigured_instrument_fails_loud(self):
        """Misconfigured future should fail with WALLET_INSTRUMENT_MISCONFIGURED."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 100000})  # Plenty of cash
        state = ledger.project()

        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=8.06451613,
            notional=8924.19354839,
            fee=5.35451613,
            short_requires_borrow=False,
            instrument=MISCONFIGURED_FUTURE,
        )

        assert allowed is False
        assert reason == "WALLET_INSTRUMENT_MISCONFIGURED"
        assert "no margin rates found" in payload["error"]

    def test_backward_compatibility_without_instrument(self):
        """Without instrument, should fall back to legacy spot-style validation."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 5000})
        state = ledger.project()

        # No instrument passed - should use legacy calculation
        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            notional=8924.19,
            fee=5.35,
            short_requires_borrow=False,
            instrument=None,  # No instrument
        )

        assert allowed is False
        assert reason == "WALLET_INSUFFICIENT_CASH_FOR_SHORT_COVER"
        # Legacy calculation: notional + 2x fee
        assert payload["required"] == pytest.approx(8924.19 + 10.70)

    def test_buy_validation_uses_margin_for_futures(self):
        """Buy/long validation should use margin for futures instruments."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 1000})
        state = ledger.project()

        # Buy with insufficient margin collateral
        allowed, reason, _ = wallet_can_apply(
            state=state,
            side="buy",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            notional=5000,
            fee=5.0,
            short_requires_borrow=False,
            instrument=COINBASE_FUTURE_INSTRUMENT,
        )

        assert allowed is False
        assert reason == "WALLET_INSUFFICIENT_MARGIN"

    def test_borrow_based_short_unchanged(self):
        """Short with borrow requirement should still check base currency."""
        ledger = WalletLedger()
        ledger.deposit({"USD": 100000, "BTC": 0.5})
        state = ledger.project()

        # Need 1.0 BTC but only have 0.5
        allowed, reason, _ = wallet_can_apply(
            state=state,
            side="short",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            notional=50000,
            fee=50.0,
            short_requires_borrow=True,  # This takes precedence
            instrument=COINBASE_FUTURE_INSTRUMENT,
        )

        assert allowed is False
        assert reason == "WALLET_INSUFFICIENT_QTY"

    def test_futures_long_margin_required_used_matches_margin_total(self):
        """Long futures should compare against margin_total_required, not full notional."""
        instrument = {
            "symbol": "BUG-FUTURE",
            "instrument_type": "future",
            "contract_size": 1.0,
            "tick_size": 1.0,
            "tick_value": 1.0,
            "can_short": True,
            "short_requires_borrow": False,
            "quote_currency": "USD",
            "metadata": {
                "base_currency": "BTC",
                "info": {
                    "future_product_details": {
                        "intraday_margin_rate": {
                            "long_margin_rate": "0.246636",
                            "short_margin_rate": "0.246636",
                        },
                        "overnight_margin_rate": {
                            "long_margin_rate": "0.246636",
                            "short_margin_rate": "0.246636",
                        },
                    }
                },
            },
        }
        notional = 1935.26
        fee = 0.0

        ledger = WalletLedger()
        ledger.deposit({"USD": 1000})
        state = ledger.project()

        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="long",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            notional=notional,
            fee=fee,
            short_requires_borrow=False,
            instrument=instrument,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert allowed is True
        assert reason is None
        assert payload == {}

        ledger = WalletLedger()
        ledger.deposit({"USD": 400})
        state = ledger.project()

        allowed, reason, payload = wallet_can_apply(
            state=state,
            side="long",
            base_currency="BTC",
            quote_currency="USD",
            qty=1.0,
            notional=notional,
            fee=fee,
            short_requires_borrow=False,
            instrument=instrument,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert allowed is False
        assert reason == "WALLET_INSUFFICIENT_MARGIN"
        expected_margin_total = notional * 0.246636 * 1.05
        assert payload["margin_total_required"] == pytest.approx(expected_margin_total, rel=1e-6)
        assert payload["required"] == pytest.approx(payload["margin_total_required"])
        assert payload["required_used"] == pytest.approx(payload["margin_total_required"])


class TestMarginCalculationAccuracy:
    """Test margin calculation accuracy with exact values from task."""

    def test_intraday_margin_calculation_for_observed_values(self):
        """Verify exact margin calculation for the observed trade values."""
        rates = extract_margin_rates(COINBASE_FUTURE_INSTRUMENT)
        calc = FuturesMarginCalculator(rates, safety_multiplier=1.05)

        result = calc.calculate(
            notional=8924.19354839,
            fee=5.35451613,
            direction="short",
            session=MarginSessionType.INTRADAY,
        )

        # Intraday short rate: 0.1000008
        expected_base_margin = 8924.19354839 * 0.1000008  # ~892.42
        expected_safety = expected_base_margin * 0.05  # 5% safety
        expected_total = expected_base_margin + expected_safety + 5.35451613

        assert result.required_margin == pytest.approx(expected_base_margin, rel=1e-4)
        assert result.safety_buffer == pytest.approx(expected_safety, rel=1e-4)
        assert result.total_required == pytest.approx(expected_total, rel=1e-4)

    def test_overnight_margin_calculation_for_observed_values(self):
        """Verify exact margin calculation for overnight session."""
        rates = extract_margin_rates(COINBASE_FUTURE_INSTRUMENT)
        calc = FuturesMarginCalculator(rates, safety_multiplier=1.05)

        result = calc.calculate(
            notional=8924.19354839,
            fee=5.35451613,
            direction="short",
            session=MarginSessionType.OVERNIGHT,
        )

        # Overnight short rate: 0.306375
        expected_base_margin = 8924.19354839 * 0.306375  # ~2734.29
        expected_safety = expected_base_margin * 0.05  # 5% safety
        expected_total = expected_base_margin + expected_safety + 5.35451613

        assert result.required_margin == pytest.approx(expected_base_margin, rel=1e-4)
        assert result.total_required == pytest.approx(expected_total, rel=1e-4)

    def test_margin_vs_full_notional_savings(self):
        """Demonstrate the difference between margin and full notional."""
        rates = extract_margin_rates(COINBASE_FUTURE_INSTRUMENT)
        futures_calc = FuturesMarginCalculator(rates, safety_multiplier=1.05)
        spot_calc = SpotMarginCalculator()

        notional = 8924.19354839
        fee = 5.35451613

        futures_result = futures_calc.calculate(
            notional=notional,
            fee=fee,
            direction="short",
            session=MarginSessionType.INTRADAY,
        )

        spot_result = spot_calc.calculate(
            notional=notional,
            fee=fee,
            direction="short",
            session=MarginSessionType.INTRADAY,
        )

        # Futures should require ~10x less capital
        capital_savings = spot_result.total_required - futures_result.total_required
        savings_ratio = futures_result.total_required / spot_result.total_required

        assert savings_ratio < 0.15  # Futures requires <15% of spot
        assert capital_savings > 7500  # Saves >$7500 in capital


class TestMaxQtyByMargin:
    """Test calculate_max_qty_by_margin for position sizing caps.

    Corrected formula:
        notional_per_contract = price * contract_size = 110660 * 0.01 = 1106.60
        margin_per_contract = notional * margin_rate = 1106.60 * 0.1000008 = ~110.67
        fee_per_contract = notional * fee_rate * 2 (round-trip) = 1106.60 * 0.0006 * 2 = ~1.33
        cost_per_contract = (margin + fees) * safety = (110.67 + 1.33) * 1.05 = ~117.60
        max_qty = available / cost_per_contract = 500 / 117.60 = ~4.25
    """

    def test_futures_max_qty_calculation(self):
        """Calculate max qty for futures with margin - round-trip fees, safety on total."""
        result = calculate_max_qty_by_margin(
            available_collateral=500.0,
            price=110660.0,
            contract_size=0.01,  # notional_per_contract = 1106.60
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,  # 0.06% taker fee
            safety_multiplier=1.05,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert result.calculation_method == "FUTURES_MARGIN_INTRADAY"
        assert result.margin_rate == pytest.approx(0.1000008)

        # Verify breakdown
        notional = 110660.0 * 0.01  # 1106.60
        expected_margin = notional * 0.1000008  # ~110.67
        expected_fees = notional * 0.0006 * 2  # ~1.33 (round-trip)
        expected_cost = (expected_margin + expected_fees) * 1.05  # ~117.60

        assert result.margin_per_contract == pytest.approx(expected_margin, rel=1e-4)
        assert result.fee_per_contract == pytest.approx(expected_fees, rel=1e-4)
        assert result.cost_per_contract == pytest.approx(expected_cost, rel=1e-4)

        # max_qty = 500 / 117.60 = ~4.25
        assert result.max_qty == pytest.approx(500.0 / expected_cost, rel=1e-4)

    def test_futures_max_qty_vs_risk_qty_capped(self):
        """When risk_qty > max_margin_qty, qty should be capped."""
        risk_qty = 8.0

        result = calculate_max_qty_by_margin(
            available_collateral=500.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.INTRADAY,
        )

        # Max by margin is ~4.25, so risk_qty of 8 would be capped
        assert result.max_qty < risk_qty
        final_qty = min(risk_qty, result.max_qty)
        assert final_qty == pytest.approx(result.max_qty)

    def test_futures_max_qty_vs_risk_qty_not_capped(self):
        """When risk_qty < max_margin_qty, qty is not capped."""
        risk_qty = 2.0

        result = calculate_max_qty_by_margin(
            available_collateral=500.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert result.max_qty > risk_qty
        final_qty = min(risk_qty, result.max_qty)
        assert final_qty == risk_qty

    def test_spot_max_qty_full_notional(self):
        """Spot instruments require full notional + round-trip fees, all with safety."""
        result = calculate_max_qty_by_margin(
            available_collateral=500.0,
            price=110660.0,
            contract_size=0.01,  # notional_per_contract = 1106.60
            direction="short",
            instrument=SPOT_INSTRUMENT,
            fee_rate=0.0006,
            safety_multiplier=1.05,
            margin_session=MarginSessionType.INTRADAY,
        )

        assert result.calculation_method == "SPOT_CASH_SHORT_COVER"
        assert result.margin_rate == 1.0

        # cost_per_contract = (1106.60 + 1106.60 * 0.0006 * 2) * 1.05 = ~1163.32
        notional = 1106.60
        fees = notional * 0.0006 * 2
        expected_cost = (notional + fees) * 1.05

        assert result.cost_per_contract == pytest.approx(expected_cost, rel=1e-4)
        # max_qty = 500 / 1163.32 = ~0.43
        assert result.max_qty == pytest.approx(500.0 / expected_cost, rel=1e-4)

    def test_misconfigured_future_raises(self):
        """Misconfigured future should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            calculate_max_qty_by_margin(
                available_collateral=500.0,
                price=110660.0,
                contract_size=0.01,
                direction="short",
                instrument=MISCONFIGURED_FUTURE,
                fee_rate=0.0006,
            )
        assert "no margin rates found" in str(exc_info.value)

    def test_overnight_margin_more_conservative(self):
        """Overnight margin allows fewer contracts than intraday."""
        intraday_result = calculate_max_qty_by_margin(
            available_collateral=500.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.INTRADAY,
        )

        overnight_result = calculate_max_qty_by_margin(
            available_collateral=500.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.OVERNIGHT,
        )

        # Overnight rate (~30.6%) > intraday (~10%), so max_qty is lower
        assert overnight_result.max_qty < intraday_result.max_qty
        assert overnight_result.margin_rate > intraday_result.margin_rate

    def test_cost_breakdown_is_transparent(self):
        """Verify the result provides full transparency into the calculation."""
        result = calculate_max_qty_by_margin(
            available_collateral=1000.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            safety_multiplier=1.05,
            margin_session=MarginSessionType.INTRADAY,
        )

        # Should be able to reconstruct the calculation
        reconstructed_cost = (result.margin_per_contract + result.fee_per_contract) * 1.05
        assert result.cost_per_contract == pytest.approx(reconstructed_cost, rel=1e-6)

        reconstructed_max_qty = result.available_collateral / result.cost_per_contract
        assert result.max_qty == pytest.approx(reconstructed_max_qty, rel=1e-6)


class TestQtyRoundingAfterMarginCap:
    """Test that qty rounding after margin cap respects exchange constraints."""

    def test_qty_rounds_below_min_order_size_should_reject(self):
        """After rounding, if qty < min_order_size, trade should be rejected."""
        result = calculate_max_qty_by_margin(
            available_collateral=100.0,  # Very limited
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.INTRADAY,
            qty_step=1.0,
            min_order_size=1.0,
        )

        # After rounding and min size enforcement, qty should be rejected
        assert result.max_qty == 0.0

    def test_qty_rounds_to_step_correctly(self):
        """Qty should round down to qty_step correctly."""
        raw_result = calculate_max_qty_by_margin(
            available_collateral=1000.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.INTRADAY,
        )

        qty_step = 0.1
        stepped_result = calculate_max_qty_by_margin(
            available_collateral=1000.0,
            price=110660.0,
            contract_size=0.01,
            direction="short",
            instrument=COINBASE_FUTURE_INSTRUMENT,
            fee_rate=0.0006,
            margin_session=MarginSessionType.INTRADAY,
            qty_step=qty_step,
        )

        import math
        rounded_units = stepped_result.max_qty / qty_step
        assert abs(rounded_units - round(rounded_units)) < 1e-9
        assert stepped_result.max_qty <= raw_result.max_qty + 1e-12
