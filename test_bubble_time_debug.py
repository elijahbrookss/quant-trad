"""Debug test to investigate bubble time placement issue."""

import pandas as pd
from signals.rules.market_profile._evaluators.breakout_v2_eval import detect_breakouts_v2

# Create test data: Simple bullish breakout
closes = [95, 96, 97, 101, 102, 103, 104, 105, 106]
index = pd.date_range("2024-08-01 00:00:00", periods=len(closes), freq="1h", tz="UTC")

df = pd.DataFrame({
    "open": closes,
    "high": closes,
    "low": closes,
    "close": closes,
}, index=index)

value_area = {
    "VAH": 100.0,
    "VAL": 90.0,
    "value_area_id": "va-1",
    "formed_at": pd.Timestamp("2024-08-01T00:00:00Z"),
    "session_count": 1,
    "min_merge_sessions": 1,
    "va_start": pd.Timestamp("2024-08-01T00:00:00Z"),
    "va_end": pd.Timestamp("2024-08-01T00:00:00Z"),
}

context = {"df": df}

# Run detection
signals = detect_breakouts_v2(context, value_area, confirm_bars=3)

print("\n" + "="*80)
print("BUBBLE TIME DEBUG TEST")
print("="*80)

if signals:
    sig = signals[0]

    print("\nDataFrame index (timestamps):")
    for i, ts in enumerate(df.index):
        print(f"  [{i}] {ts} - close={df.loc[ts, 'close']}")

    print("\n" + "-"*80)
    print("SIGNAL METADATA:")
    print("-"*80)

    print(f"\nPrior window:")
    prior_times = sig.get("prior_times", [])
    prior_indices = sig.get("prior_indices", [])
    for i, (idx, ts) in enumerate(zip(prior_indices, prior_times)):
        print(f"  [{i+1}] idx={idx}, time={ts}")

    print(f"\nConfirm window:")
    confirm_times = sig.get("confirm_times", [])
    confirm_indices = sig.get("confirm_indices", [])
    for i, (idx, ts) in enumerate(zip(confirm_indices, confirm_times)):
        print(f"  [✓{i+1}] idx={idx}, time={ts}")

    print(f"\nBreakout timing:")
    print(f"  break_time (sig['break_time']): {sig.get('break_time')}")
    print(f"  time (sig['time']):             {sig.get('time')}")
    print(f"  bar_index (sig['bar_index']):   {sig.get('bar_index')}")
    print(f"  trigger_time:                   {sig.get('trigger_time')}")

    # Verify they match
    last_confirm_time = confirm_times[-1]
    last_confirm_idx = confirm_indices[-1]

    print(f"\n" + "-"*80)
    print("VERIFICATION:")
    print("-"*80)
    print(f"Last confirm time:  {last_confirm_time}")
    print(f"Last confirm index: {last_confirm_idx}")
    print(f"Signal time:        {sig.get('time')}")
    print(f"Signal bar_index:   {sig.get('bar_index')}")

    times_match = sig.get('time') == last_confirm_time
    indices_match = sig.get('bar_index') == last_confirm_idx

    print(f"\n✓ Time matches last confirm:  {times_match}")
    print(f"✓ Index matches last confirm: {indices_match}")

    if not times_match:
        print(f"\n⚠️  WARNING: Signal time does NOT match last confirm time!")
        print(f"   Expected: {last_confirm_time}")
        print(f"   Got:      {sig.get('time')}")
        print(f"   Difference: {sig.get('time') - last_confirm_time if isinstance(sig.get('time'), pd.Timestamp) else 'N/A'}")

    if not indices_match:
        print(f"\n⚠️  WARNING: Signal index does NOT match last confirm index!")
        print(f"   Expected: {last_confirm_idx}")
        print(f"   Got:      {sig.get('bar_index')}")

else:
    print("\n⚠️  No signals detected!")

print("\n" + "="*80)
