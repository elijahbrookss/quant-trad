from collections import deque

from engines.bot_runtime.core.domain import StrategySignal
from engines.bot_runtime.runtime.components.signal_consumption import SignalConsumption, consume_signals


def test_signal_consumption_preserves_history():
    signals = deque(
        [
            StrategySignal(epoch=90, direction="long"),
            StrategySignal(epoch=95, direction="short"),
            StrategySignal(epoch=110, direction="long"),
        ]
    )
    consumed, chosen, last_consumed = consume_signals(signals, epoch=100, last_consumed_epoch=0)

    assert consumed == [{"epoch": 90, "direction": "long"}, {"epoch": 95, "direction": "short"}]
    assert chosen == "short"
    assert last_consumed == 95
    assert signals[0].epoch == 110


def test_signal_consumption_log_bounded():
    signal_log = deque(maxlen=500)
    for idx in range(505):
        signal_log.append(
            SignalConsumption(
                epoch=idx,
                consumed_signals=[{"epoch": idx, "direction": "long"}],
                chosen_direction="long",
            )
        )
    assert len(signal_log) == 500


def test_signal_consumption_skips_already_consumed_epochs():
    signals = deque(
        [
            StrategySignal(epoch=95, direction="long"),
            StrategySignal(epoch=100, direction="short"),
            StrategySignal(epoch=105, direction="long"),
        ]
    )

    consumed, chosen, last_consumed = consume_signals(signals, epoch=110, last_consumed_epoch=100)

    assert consumed == [{"epoch": 105, "direction": "long"}]
    assert chosen == "long"
    assert last_consumed == 105
