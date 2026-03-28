export const toSignalEpochSeconds = (value) => {
  if (value == null) return null;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null;
    return value > 2e10 ? Math.trunc(value / 1000) : Math.trunc(value);
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      return numeric > 2e10 ? Math.trunc(numeric / 1000) : Math.trunc(numeric);
    }
    const parsed = Date.parse(trimmed);
    return Number.isFinite(parsed) ? Math.trunc(parsed / 1000) : null;
  }
  if (typeof value === 'object' && typeof value.timestamp === 'function') {
    const numeric = Number(value.timestamp());
    return Number.isFinite(numeric) ? Math.trunc(numeric) : null;
  }
  return null;
};

export const formatSignalEventLabel = (value) => {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return 'Signal';
  return raw
    .split(/[_-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
};

export const formatSignalReferenceText = (signal) => {
  const reference = signal?.metadata?.reference;
  if (!reference || typeof reference !== 'object') return null;
  const label = reference.label || reference.name || reference.family || reference.kind;
  const price = Number(reference.price);
  if (!label && !Number.isFinite(price)) return null;
  const precision = Number.isFinite(Number(reference.precision))
    ? Math.max(0, Math.min(Number(reference.precision), 8))
    : 2;
  if (Number.isFinite(price)) {
    return `${label || 'Level'} ${price.toFixed(precision)}`;
  }
  return String(label);
};

export const formatSignalTimestamp = (signal) => {
  const epoch = toSignalEpochSeconds(signal?.known_at) ?? toSignalEpochSeconds(signal?.event_time);
  if (!Number.isFinite(epoch)) return null;
  return new Date(epoch * 1000).toLocaleString();
};

export const resolveSignalChartEpoch = (signal) => (
  toSignalEpochSeconds(signal?.event_time) ?? toSignalEpochSeconds(signal?.known_at)
);

export const collectSignalBubbleEpochs = (overlays = []) => {
  const epochsBySignalId = new Map();

  for (const overlay of overlays || []) {
    const payload = overlay?.payload;
    if (!payload || typeof payload !== 'object') continue;

    const bubbleCandidates = [
      ...(Array.isArray(payload?.bubbles) ? payload.bubbles : []),
      ...(Array.isArray(payload?.markers)
        ? payload.markers.filter((marker) => marker?.subtype === 'bubble')
        : []),
    ];

    for (const bubble of bubbleCandidates) {
      const signalId = resolveSignalId(bubble);
      if (!signalId || epochsBySignalId.has(signalId)) continue;
      const epoch = toSignalEpochSeconds(bubble?.time);
      if (!Number.isFinite(epoch)) continue;
      epochsBySignalId.set(signalId, epoch);
    }
  }

  return epochsBySignalId;
};

export const resolveSignalId = (signal) => {
  const direct = typeof signal?.signal_id === 'string' ? signal.signal_id.trim() : '';
  if (direct) return direct;
  const metadataCandidates = [
    signal?.metadata?.signal_id,
    signal?.metadata?.trace_id,
    signal?.pattern_id,
  ];
  for (const candidate of metadataCandidates) {
    const value = typeof candidate === 'string' ? candidate.trim() : '';
    if (value) return value;
  }
  return null;
};

export const formatSignalIdSuffix = (signal, size = 4) => {
  const signalId = resolveSignalId(signal);
  if (!signalId) return null;
  return signalId.slice(-Math.max(1, size));
};

export const formatSignalLabelWithId = (label, signal) => {
  const suffix = formatSignalIdSuffix(signal);
  const base = typeof label === 'string' && label.trim() ? label.trim() : 'Signal';
  return suffix ? `${base} · ${suffix}` : base;
};

export const resolveSignalCursorEpoch = (signal) => (
  toSignalEpochSeconds(signal?.known_at) ?? toSignalEpochSeconds(signal?.event_time)
);

export const buildSignalInspectionKey = (signal) => (
  resolveSignalId(signal) || [
    signal?.indicator_id || '',
    signal?.output_name || '',
    signal?.event_key || '',
    signal?.known_at || signal?.event_time || '',
  ].join(':')
);

export const sortSignalsNewestFirst = (signals = []) => (
  [...signals].sort((left, right) => (
    (resolveSignalCursorEpoch(right) || 0) - (resolveSignalCursorEpoch(left) || 0)
  ))
);
