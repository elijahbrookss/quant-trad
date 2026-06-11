import {
  buildSignalInspectionKey,
  formatSignalEventLabel,
  formatSignalIdSuffix,
  formatSignalTimestamp,
  resolveSignalChartEpoch,
  resolveSignalCursorEpoch,
  resolveSignalId,
  sortSignalsNewestFirst,
} from './indicatorSignalDebug.js';

export const flattenSignals = (
  signalEventsByIndicator = {},
  indicatorsById = {},
  bubbleEpochBySignalId = new Map(),
) => {
  const rows = [];
  Object.entries(signalEventsByIndicator || {}).forEach(([indicatorId, signals]) => {
    if (!Array.isArray(signals) || !signals.length) return;
    const indicator = indicatorsById?.[indicatorId] || null;
    sortSignalsNewestFirst(signals).forEach((signal) => {
      const signalId = resolveSignalId(signal);
      const signalKey = buildSignalInspectionKey(signal);
      const outputName = typeof signal?.output_name === 'string' ? signal.output_name.trim() : '';
      const chartEpoch = (signalId && bubbleEpochBySignalId.get(signalId))
        ?? resolveSignalChartEpoch(signal)
        ?? null;
      rows.push({
        indicator,
        indicatorId,
        signal,
        signalId,
        signalKey,
        signalSuffix: formatSignalIdSuffix(signal),
        label: formatSignalEventLabel(signal?.event_key),
        timestamp: formatSignalTimestamp(signal),
        epoch: resolveSignalCursorEpoch(signal) || 0,
        chartEpoch,
        direction: typeof signal?.direction === 'string' ? signal.direction.trim() : '',
        outputName,
        seriesKey: typeof signal?.series_key === 'string' ? signal.series_key.trim() : '',
      });
    });
  });
  return rows.sort((left, right) => right.epoch - left.epoch);
};

export const buildSearchHaystack = (entry) => {
  const tokens = [
    entry?.label,
    entry?.signalId,
    entry?.signalSuffix,
    entry?.direction,
    entry?.outputName,
    entry?.seriesKey,
    entry?.indicator?.name,
    entry?.indicator?.type,
    entry?.timestamp,
  ];
  return tokens
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
};
