from portal.backend.service.bot_runtime.run_context import RunContext
from portal.backend.service.bot_runtime.runtime import BotRuntime


def test_run_artifact_payload_contains_wallet_and_trace():
    runtime = BotRuntime("bot-1", {"wallet_config": {"balances": {"USDC": 100}}})
    run_context = RunContext(bot_id="bot-1")
    run_context.wallet_ledger.deposit({"USDC": 100})
    run_context.wallet_ledger.trade_fill(
        side="buy",
        base_currency="ETH",
        quote_currency="USDC",
        qty=0.1,
        price=2000,
        fee=0.2,
        notional=200,
    )
    run_context.decision_trace.append({"event": "signal_accepted"})
    runtime._run_context = run_context
    artifact = runtime._run_artifact_payload("completed")

    assert artifact["bot_id"] == "bot-1"
    assert artifact["status"] == "completed"
    assert artifact["wallet_start"]["balances"]["USDC"] == 100
    assert "wallet_end" in artifact
    assert artifact["decision_trace"][0]["event"] == "signal_accepted"
