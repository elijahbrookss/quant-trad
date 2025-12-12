# Signal registry decorators

The signal engine now supports decorator-based registration so indicator rules and overlays are bound as soon as their modules are imported.

```python
from signals.engine.signal_generator import overlay_adapter, signal_rule

class MyIndicator:
    NAME = "CustomIndicator"

@signal_rule(MyIndicator, rule_id="custom_breakout", label="Breakout")
def breakout_rule(context, payload):
    return [
        {
            "type": "breakout",
            "time": context["df"].index[-1],
            "symbol": context.get("symbol"),
        }
    ]

@overlay_adapter(MyIndicator)
def breakout_overlay(signals, plot_df, **kwargs):
    return [{"type": MyIndicator.NAME, "payload": len(signals)}]
```

Using the decorators removes the need to call `register_indicator_rules` manually. When the module above is imported, the rule and overlay adapter are automatically available to `run_indicator_rules` and `build_signal_overlays`.
