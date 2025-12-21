"""Test timestamp conversion differences."""

import pandas as pd
from signals.rules.common.utils import to_epoch_seconds

# Create a test timestamp
ts = pd.Timestamp("2024-08-01 05:00:00", tz="UTC")
dt = ts.to_pydatetime()

print("Original timestamp:", ts)
print("Converted to datetime:", dt)
print()

# Check epoch conversion
ts_epoch_direct = int(ts.timestamp())
ts_epoch_util = to_epoch_seconds(ts)
dt_epoch_util = to_epoch_seconds(dt)

print(f"ts.timestamp():         {ts_epoch_direct}")
print(f"to_epoch_seconds(ts):   {ts_epoch_util}")
print(f"to_epoch_seconds(dt):   {dt_epoch_util}")
print()

if ts_epoch_direct == ts_epoch_util == dt_epoch_util:
    print("✓ All conversions produce the same epoch timestamp")
else:
    print("⚠️  MISMATCH DETECTED!")
    if ts_epoch_direct != ts_epoch_util:
        print(f"   ts.timestamp() != to_epoch_seconds(ts): {ts_epoch_direct} != {ts_epoch_util}")
    if ts_epoch_util != dt_epoch_util:
        print(f"   to_epoch_seconds(ts) != to_epoch_seconds(dt): {ts_epoch_util} != {dt_epoch_util}")
