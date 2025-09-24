import { useCallback, useEffect, useMemo, useState } from 'react';
import { createLogger } from '../utils/logger.js';

const DATA_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';
const INDICATOR_BASE = import.meta.env.REACT_APP_API_BASE_URL || DATA_BASE;

const TIMEOUT_MS = 7000;

const toISO = (date) => date.toISOString();

async function probeDataFeed(signal, log) {
  const now = new Date();
  const end = toISO(now);
  const start = toISO(new Date(now.getTime() - 60 * 60 * 1000));

  try {
    const res = await fetch(`${DATA_BASE}/api/candles`, {
      signal,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ symbol: 'CL', timeframe: '15m', start, end }),
    });

    if (!res.ok) {
      log.warn('data_probe_failed_status', { status: res.status });
      return { status: 'offline', detail: `${res.status}` };
    }

    const payload = await res.json();
    const candles = Array.isArray(payload?.candles) ? payload.candles : [];

    if (!candles.length) {
      log.info('data_probe_empty');
      return { status: 'degraded', detail: 'No data' };
    }

    return { status: 'online', detail: `${candles.length} bars` };
  } catch (error) {
    if (error?.name === 'AbortError') {
      log.warn('data_probe_timeout');
      return { status: 'offline', detail: 'Timeout' };
    }
    log.error('data_probe_error', error);
    return { status: 'offline', detail: error?.message || 'Error' };
  }
}

async function probeIndicatorFeed(signal, log) {
  try {
    const res = await fetch(`${INDICATOR_BASE}/api/indicators-types/`, { signal });

    if (!res.ok) {
      log.warn('indicator_probe_failed_status', { status: res.status });
      return { status: 'offline', detail: `${res.status}` };
    }

    const payload = await res.json();
    const count = Array.isArray(payload) ? payload.length : 0;
    return { status: 'online', detail: `${count} types` };
  } catch (error) {
    if (error?.name === 'AbortError') {
      log.warn('indicator_probe_timeout');
      return { status: 'offline', detail: 'Timeout' };
    }
    log.error('indicator_probe_error', error);
    return { status: 'offline', detail: error?.message || 'Error' };
  }
}

const PROBES = [
  {
    key: 'data',
    label: 'Market data',
    runner: probeDataFeed,
  },
  {
    key: 'indicators',
    label: 'Indicator API',
    runner: probeIndicatorFeed,
  },
];

export function useFeedStatus({ refreshMs = 60000 } = {}) {
  const logger = useMemo(() => createLogger('FeedStatus'), []);
  const [statuses, setStatuses] = useState(() =>
    PROBES.map((p) => ({ key: p.key, label: p.label, status: 'checking', detail: null }))
  );

  const runProbes = useCallback(async () => {
    logger.info('probe_start');
    setStatuses((prev) => prev.map((s) => ({ ...s, status: 'checking' })));

    const results = await Promise.all(
      PROBES.map(async (probe) => {
        const scoped = logger.child({ probe: probe.key });
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

        try {
          const outcome = await probe.runner(controller.signal, scoped);
          return { key: probe.key, label: probe.label, ...outcome };
        } finally {
          clearTimeout(timer);
        }
      })
    );

    setStatuses(results);
    logger.info('probe_complete', { results });
  }, [logger]);

  useEffect(() => {
    runProbes();

    if (!refreshMs) return undefined;

    const id = setInterval(runProbes, refreshMs);
    return () => clearInterval(id);
  }, [runProbes, refreshMs]);

  return {
    statuses,
    refresh: runProbes,
    isChecking: statuses.some((s) => s.status === 'checking'),
  };
}

export default useFeedStatus;
