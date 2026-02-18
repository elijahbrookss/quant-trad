import { useEffect, useState } from 'react';

const toSec = (value) => {
  if (value == null) return null;
  if (typeof value === 'object') {
    if (typeof value.timestamp === 'function') {
      const ts = Number(value.timestamp());
      return Number.isFinite(ts) ? ts : null;
    }
    if (Number.isFinite(value.timestamp)) {
      return Number(value.timestamp);
    }
  }
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return num > 2e10 ? Math.floor(num / 1000) : num;
};

const nearestDetail = (details = [], epoch) => {
  let best = null;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (const detail of details) {
    const t = Number(detail?.time);
    if (!Number.isFinite(t)) continue;
    const delta = Math.abs(t - epoch);
    if (delta < bestDelta) {
      bestDelta = delta;
      best = detail;
    }
  }
  if (!best || bestDelta > 1) return null;
  return best;
};

export const useSignalTooltip = ({ chartRef, signalDetailsRef }) => {
  const [signalTooltip, setSignalTooltip] = useState(null);

  useEffect(() => {
    if (!chartRef.current) return undefined;
    const handler = (param) => {
      const epoch = toSec(param?.time);
      if (!Number.isFinite(epoch) || !param?.point) {
        setSignalTooltip(null);
        return;
      }
      const details = Array.isArray(signalDetailsRef?.current) ? signalDetailsRef.current : [];
      const exact = details.filter((entry) => Number(entry?.time) === Number(epoch));
      const resolved = exact.length ? exact : (() => {
        const nearest = nearestDetail(details, epoch);
        return nearest ? [nearest] : [];
      })();
      if (!resolved.length) {
        setSignalTooltip(null);
        return;
      }
      const entries = [...new Set(resolved.flatMap((entry) => (Array.isArray(entry?.entries) ? entry.entries : [])))];
      if (!entries.length) {
        setSignalTooltip(null);
        return;
      }
      setSignalTooltip({ x: param.point.x, y: param.point.y, entries });
    };
    chartRef.current.subscribeCrosshairMove(handler);
    return () => chartRef.current?.unsubscribeCrosshairMove?.(handler);
  }, [chartRef, signalDetailsRef]);

  return signalTooltip;
};
