import { useState, useCallback, useMemo } from 'react';

/**
 * useWindowConfiguration - Manages date range, lookback days, and window modes
 *
 * Extracts window configuration state and logic from ChartComponent.
 * Part of ChartComponent refactoring to reduce complexity.
 */

const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_LOOKBACK_DAYS = 365;
const DEFAULT_LOOKBACK_DAYS = 90;
const HISTORICAL_WINDOW_MODES = {
  LOOKBACK: 'lookback',
  RANGE: 'range',
};

const clampLookbackDays = (value) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return DEFAULT_LOOKBACK_DAYS;
  }
  const rounded = Math.round(numeric);
  return Math.max(1, Math.min(MAX_LOOKBACK_DAYS, rounded));
};

export function useWindowConfiguration({
  savedPrefs = {},
  modeRef,
  dateRangeRef,
}) {
  // Quick lookback presets
  const quickLookbackPresets = useMemo(
    () => [
      { label: '1D', days: 1 },
      { label: '5D', days: 5 },
      { label: '10D', days: 10 },
      { label: '1M', days: 30 },
      { label: '3M', days: 90 },
      { label: '6M', days: 180 },
      { label: '1Y', days: 365 },
    ],
    []
  );

  // Date range state
  const [dateRange, setDateRange] = useState([
    new Date(Date.now() - DEFAULT_LOOKBACK_DAYS * DAY_MS),
    new Date(),
  ]);

  // Historical window mode state
  const [historicalWindowMode, setHistoricalWindowMode] = useState(
    () => savedPrefs?.historicalWindowMode || HISTORICAL_WINDOW_MODES.LOOKBACK
  );

  // Historical lookback state
  const [historicalLookbackDays, setHistoricalLookbackDays] = useState(
    () => savedPrefs?.historicalLookbackDays || DEFAULT_LOOKBACK_DAYS
  );
  const [historicalLookbackInput, setHistoricalLookbackInput] = useState(
    () => String(savedPrefs?.historicalLookbackDays || DEFAULT_LOOKBACK_DAYS)
  );

  // Live lookback state
  const [liveLookbackDays, setLiveLookbackDays] = useState(
    () => savedPrefs?.liveLookbackDays || DEFAULT_LOOKBACK_DAYS
  );
  const [liveLookbackInput, setLiveLookbackInput] = useState(
    () => String(savedPrefs?.liveLookbackDays || DEFAULT_LOOKBACK_DAYS)
  );

  // Historical lookback change handler
  const handleHistoricalLookbackChange = useCallback((days) => {
    const normalized = clampLookbackDays(days);
    setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.LOOKBACK);
    setHistoricalLookbackDays(normalized);
    setHistoricalLookbackInput(String(normalized));
  }, []);

  // Historical lookback input change handler
  const handleHistoricalLookbackInputChange = useCallback((event) => {
    const raw = event?.target?.value ?? '';
    const sanitized = raw.replace(/[^0-9]/g, '');
    setHistoricalLookbackInput(sanitized);
  }, []);

  // Historical lookback commit handler
  const handleHistoricalLookbackCommit = useCallback(() => {
    const parsed = Number.parseInt(historicalLookbackInput, 10);
    const normalized = clampLookbackDays(
      Number.isNaN(parsed) ? historicalLookbackDays : parsed
    );
    if (normalized !== historicalLookbackDays) {
      handleHistoricalLookbackChange(normalized);
    } else {
      setHistoricalLookbackInput(String(normalized));
    }
  }, [handleHistoricalLookbackChange, historicalLookbackInput, historicalLookbackDays]);

  // Live lookback input change handler
  const handleLiveLookbackInputChange = useCallback((event) => {
    const raw = event?.target?.value ?? '';
    const sanitized = raw.replace(/[^0-9]/g, '');
    setLiveLookbackInput(sanitized);
  }, []);

  // Date range selection handler
  const handleDateRangeSelection = useCallback(
    (nextRange) => {
      if (!Array.isArray(nextRange)) return;
      const normalized = nextRange.map((value) => {
        if (value instanceof Date) return value;
        if (!value) return value;
        const converted = new Date(value);
        return Number.isNaN(converted.getTime()) ? null : converted;
      });
      setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.RANGE);
      if (dateRangeRef) {
        dateRangeRef.current = normalized;
      }
      setDateRange(normalized);
    },
    [dateRangeRef]
  );

  // Historical mode toggle handler
  const handleHistoricalModeToggle = useCallback((nextMode) => {
    if (nextMode === HISTORICAL_WINDOW_MODES.RANGE) {
      setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.RANGE);
      return;
    }
    setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.LOOKBACK);
  }, []);

  // Live lookback commit handler (returns nextRange for external apply handling)
  const handleLiveLookbackCommit = useCallback(() => {
    const parsed = Number.parseInt(liveLookbackInput, 10);
    const normalized = clampLookbackDays(Number.isNaN(parsed) ? liveLookbackDays : parsed);

    setLiveLookbackDays(normalized);
    setLiveLookbackInput(String(normalized));

    const now = new Date();
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    if (dateRangeRef) {
      dateRangeRef.current = nextRange;
    }
    setDateRange(nextRange);

    return { nextRange, changed: normalized !== liveLookbackDays };
  }, [liveLookbackInput, liveLookbackDays, dateRangeRef]);

  // Live lookback preset select handler (returns nextRange for external apply handling)
  const handleLiveLookbackPresetSelect = useCallback(
    (days) => {
      const normalized = clampLookbackDays(days);
      setLiveLookbackDays(normalized);
      setLiveLookbackInput(String(normalized));

      const now = new Date();
      const start = new Date(now.getTime() - normalized * DAY_MS);
      const nextRange = [start, now];
      if (dateRangeRef) {
        dateRangeRef.current = nextRange;
      }
      setDateRange(nextRange);

      return { nextRange };
    },
    [dateRangeRef]
  );

  // Computed values
  const isLookbackMode = historicalWindowMode === HISTORICAL_WINDOW_MODES.LOOKBACK;
  const isRangeMode = historicalWindowMode === HISTORICAL_WINDOW_MODES.RANGE;

  const windowSummary = useMemo(() => {
    const liveMode = modeRef?.current === 'live';
    if (liveMode) {
      return `Live window ${clampLookbackDays(liveLookbackDays)}d`;
    }
    if (isRangeMode) {
      const startLabel =
        dateRange?.[0] instanceof Date ? dateRange[0].toLocaleDateString() : 'Start';
      const endLabel = dateRange?.[1] instanceof Date ? dateRange[1].toLocaleDateString() : 'End';
      return `Range ${startLabel} to ${endLabel}`;
    }
    return `Lookback ${clampLookbackDays(historicalLookbackDays)}d`;
  }, [dateRange, historicalLookbackDays, isRangeMode, liveLookbackDays, modeRef]);

  return {
    // State
    dateRange,
    setDateRange,
    historicalWindowMode,
    historicalLookbackDays,
    historicalLookbackInput,
    liveLookbackDays,
    liveLookbackInput,
    quickLookbackPresets,

    // Computed
    isLookbackMode,
    isRangeMode,
    windowSummary,

    // Handlers
    handleHistoricalLookbackChange,
    handleHistoricalLookbackInputChange,
    handleHistoricalLookbackCommit,
    handleLiveLookbackInputChange,
    handleLiveLookbackCommit,
    handleLiveLookbackPresetSelect,
    handleDateRangeSelection,
    handleHistoricalModeToggle,

    // Constants (for external use)
    HISTORICAL_WINDOW_MODES,
  };
}

// Export constants for use in other files
export { HISTORICAL_WINDOW_MODES, clampLookbackDays };
