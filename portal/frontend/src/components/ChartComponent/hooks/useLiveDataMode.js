import { useCallback, useEffect, useRef, useState } from 'react';

const LIVE_REFRESH_INTERVAL_MS = 15_000;

/**
 * Manage historical/live data mode switching for the chart component.
 *
 * The hook owns the live refresh interval so the UI component can stay
 * declarative – consumers simply provide a refresh callback and toggle the
 * mode between `historical` and `live`.
 */
export function useLiveDataMode({ supportsLive, onRefresh, logger }) {
  const [mode, setMode] = useState('historical');
  const timerRef = useRef(null);

  const clearTimer = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const runRefresh = useCallback(() => {
    if (typeof onRefresh !== 'function') return;
    try {
      const result = onRefresh();
      if (result && typeof result.then === 'function') {
        result.catch((err) => {
          logger?.warn?.('live_refresh_failed', err);
        });
      }
    } catch (err) {
      logger?.warn?.('live_refresh_failed_sync', err);
    }
  }, [onRefresh, logger]);

  useEffect(() => {
    if (mode === 'live' && supportsLive) {
      clearTimer();
      runRefresh();
      timerRef.current = setInterval(runRefresh, LIVE_REFRESH_INTERVAL_MS);
      return () => clearTimer();
    }

    clearTimer();
    return undefined;
  }, [mode, supportsLive, clearTimer, runRefresh]);

  useEffect(() => {
    if (!supportsLive && mode === 'live') {
      setMode('historical');
    }
  }, [supportsLive, mode]);

  useEffect(() => clearTimer, [clearTimer]);

  return { mode, setMode };
}
