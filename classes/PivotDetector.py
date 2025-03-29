class PivotDetector:
    def __init__(self, df, lookbacks, threshold=0.005):
        self.df = df
        self.lookbacks = lookbacks
        self.threshold = threshold

    def detect_all(self):
        all_pivots = {}
        for lookback in self.lookbacks:
            all_pivots[lookback] = self._find_pivots(lookback)
        return all_pivots

    def _find_pivots(self, lookback):
        highs, lows = [], []

        def is_near_existing(price):
            for _, p in highs + lows:
                if abs(price - p) / p < self.threshold:
                    return True
            return False

        for i in range(lookback, len(self.df) - lookback):
            current = self.df.index[i]
            high = self.df.at[current, 'High']
            low = self.df.at[current, 'Low']

            high_range = self.df['High'].iloc[i - lookback:i + lookback + 1].drop(labels=[current])
            low_range = self.df['Low'].iloc[i - lookback:i + lookback + 1].drop(labels=[current])

            if high > high_range.max() and not is_near_existing(high):
                highs.append((current, high))
            elif low < low_range.min() and not is_near_existing(low):
                lows.append((current, low))

        return highs, lows