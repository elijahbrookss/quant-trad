import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { createChart, CandlestickSeries } from 'lightweight-charts';
import { RotateCcw } from 'lucide-react';
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent';
import { DateRangePickerComponent } from './DateTimePickerComponent.jsx';
import { options, seriesOptions } from './ChartOptions';
import { fetchInstrumentCandles } from '../../hooks/useInstrumentCandles.js';
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx';
import { createLogger } from '../../utils/logger.js';
import { PaneViewManager } from '../../chart/paneViews/factory.js';
import { useConnectionMonitor } from '../../hooks/useConnectionMonitor.js';
import DropdownSelect from './DropdownSelect.jsx';
import CredentialsModal from './CredentialsModal.jsx';
import ChartSurface from './ChartSurface.jsx';
import { useLiveDataMode } from './hooks/useLiveDataMode.js';
import { useProviderManagement } from './hooks/useProviderManagement.js';
import { useWindowConfiguration, HISTORICAL_WINDOW_MODES, clampLookbackDays } from './hooks/useWindowConfiguration.js';
import { useOverlaySync } from './hooks/useOverlaySync.js';
import { DATASOURCE_IDS, DEFAULT_DATASOURCE } from '../../constants/datasources.js';

// File-level namespace.
const LOG_NS = 'ChartComponent';
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LOOKBACK_DAYS = 90;
const LIVE_CRYPTO_EXCHANGES = new Set(['binanceus']);

// localStorage helpers for chart preferences
const CHART_PREFS_KEY = 'qt.chartPreferences';

const hasLocalStorage = () => {
  try {
    return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
  } catch {
    return false;
  }
};

const loadChartPreferences = () => {
  if (!hasLocalStorage()) return null;
  try {
    const raw = window.localStorage.getItem(CHART_PREFS_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
};

const saveChartPreferences = (prefs) => {
  if (!hasLocalStorage()) return;
  try {
    window.localStorage.setItem(CHART_PREFS_KEY, JSON.stringify(prefs));
  } catch {
    // Ignore persistence issues (private browsing, quota exceeded, etc.)
  }
};

const deriveTimeScaleOptions = (rawInterval) => {
  const interval = (rawInterval || '').toString().toLowerCase();
  const base = { timeVisible: true, secondsVisible: false };

  if (!interval) return base;

  if (interval.endsWith('s')) {
    return { ...base, secondsVisible: true };
  }

  if (interval.endsWith('m')) {
    return base;
  }

  if (interval.endsWith('h')) {
    return base;
  }

  if (interval.endsWith('d')) {
    return { timeVisible: false, secondsVisible: false };
  }

  if (interval.endsWith('w') || interval.endsWith('mo') || interval.endsWith('y')) {
    return { timeVisible: false, secondsVisible: false };
  }

  return base;
};

const normalizeExchangeId = (value) => (value ?? '').toString().trim().toLowerCase();

const deriveCcxtPriceFormat = (candles = []) => {
  if (!Array.isArray(candles) || candles.length === 0) {
    return null;
  }

  const values = [];
  for (const c of candles) {
    if (!c) continue;
    const { open, high, low, close } = c;
    [open, high, low, close].forEach((v) => {
      if (Number.isFinite(v)) values.push(Math.abs(v));
    });
  }

  if (!values.length) {
    return null;
  }

  const reference = Math.max(...values);

  let precision = 4;
  if (reference >= 1000) precision = 2;
  else if (reference >= 100) precision = 3;
  else if (reference >= 10) precision = 4;
  else if (reference >= 1) precision = 4;
  else if (reference >= 0.1) precision = 5;
  else if (reference >= 0.01) precision = 6;
  else if (reference >= 0.001) precision = 7;
  else precision = 8;

  const minMove = Number((10 ** -precision).toFixed(precision));
  return { type: 'price', precision, minMove };
};

export const ChartComponent = ({ chartId }) => {
  // Logger for this file.
  const logger = useMemo(() => createLogger(LOG_NS, { chartId }), [chartId]);
  const { debug, info, warn, error } = logger;

  // Context wiring.
  const { registerChart, updateChart, bumpRefresh } = useChartState();
  const chartState = useChartValue(chartId);

  // Load saved preferences on mount
  const savedPrefs = useMemo(() => loadChartPreferences(), []);

  // Local UI state with localStorage fallback
  const [symbol, setSymbol] = useState(() => savedPrefs?.symbol || 'CL');
  const [symbolDraft, setSymbolDraft] = useState(() => savedPrefs?.symbol || 'CL');
  const [interval, setInterval] = useState(() => savedPrefs?.interval || '15m');
  const [datasource, setDatasource] = useState(() => savedPrefs?.datasource || DEFAULT_DATASOURCE);
  const [exchange, setExchange] = useState(() => savedPrefs?.exchange || '');

  // Provider management hook
  const providerMgmt = useProviderManagement({
    savedPrefs,
    logger,
    onDatasourceChange: setDatasource,
    onExchangeChange: setExchange,
  });

  const [palOpen, setPalOpen] = useState(false);
  const [dataLoading, setDataLoading] = useState(false);
  const [dataLoaderContext, setDataLoaderContext] = useState(null);
  const [rangeWarning, setRangeWarning] = useState(null);
  const [connectionNotice, setConnectionNotice] = useState(null);
  const [lastRefreshAt, setLastRefreshAt] = useState(null);
  const [chartStateNotice, setChartStateNotice] = useState({
    state: 'idle',
    message: 'Preparing chart…',
  });
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [fullscreenHost, setFullscreenHost] = useState(null);
  const markChartState = useCallback((state, message = null) => {
    setChartStateNotice({ state, message });
  }, []);

  const instrumentIdRef = useRef(null);
  const instrumentKeyRef = useRef(null);
  const modeRef = useRef('historical');
  const dataLoadingRef = useRef(false);
  const dateRangeRef = useRef([
    new Date(Date.now() - DEFAULT_LOOKBACK_DAYS * DAY_MS),
    new Date(),
  ]);

  // Window configuration hook
  const windowConfig = useWindowConfiguration({
    savedPrefs,
    modeRef,
    dateRangeRef,
  });


  useEffect(() => {
    if (!isFullscreen) return undefined;
    if (typeof document === 'undefined') return undefined;

    const node = document.createElement('div');
    node.className = 'fixed inset-0 z-[9999] bg-[#01030e]';
    document.body.appendChild(node);
    setFullscreenHost(node);

    return () => {
      setFullscreenHost((current) => (current === node ? null : current));
      if (node.parentNode) {
        node.parentNode.removeChild(node);
      }
    };
  }, [isFullscreen]);


  // Save chart preferences to localStorage whenever they change
  useEffect(() => {
    const prefs = {
      symbol,
      interval,
      datasource,
      providerId: providerMgmt.providerId,
      venueId: providerMgmt.venueId,
      exchange,
      historicalWindowMode: windowConfig.historicalWindowMode,
      historicalLookbackDays: windowConfig.historicalLookbackDays,
      liveLookbackDays: windowConfig.liveLookbackDays,
    };
    saveChartPreferences(prefs);
  }, [
    symbol,
    interval,
    datasource,
    providerMgmt.providerId,
    providerMgmt.venueId,
    exchange,
    windowConfig.historicalWindowMode,
    windowConfig.historicalLookbackDays,
    windowConfig.liveLookbackDays,
  ]);

  useEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const { body } = document;
    if (!body) return undefined;

    if (isFullscreen) {
      body.classList.add('overflow-hidden');
    } else {
      body.classList.remove('overflow-hidden');
    }

    return () => {
      body.classList.remove('overflow-hidden');
    };
  }, [isFullscreen]);

  useEffect(() => {
    if (!isFullscreen) return undefined;
    if (typeof window === 'undefined') return undefined;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setIsFullscreen(false);
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [isFullscreen, setIsFullscreen]);

  const connection = useConnectionMonitor({ name: 'QuantLab API' });
  const {
    status: connectionStatus,
    message: connectionMessage,
    markAttempt,
    markSuccess,
    markError,
  } = connection;

  const statusStyles = useMemo(() => {
    if (connectionStatus === 'online') {
      return {
        text: 'text-[color:var(--accent-text-soft)]',
      };
    }

    if (connectionStatus === 'connecting' || connectionStatus === 'recovering') {
      return {
        text: 'text-amber-200',
      };
    }

    if (connectionStatus === 'error') {
      return {
        text: 'text-rose-200',
      };
    }

    return {
      text: 'text-slate-300',
    };
  }, [connectionStatus]);

  // Refs for chart and DOM.
  const chartContainerElRef = useRef(null);
  const [chartMountNode, setChartMountNode] = useState(null);
  const attachChartContainerRef = useCallback((node) => {
    chartContainerElRef.current = node;
    setChartMountNode((prev) => (prev === node ? prev : node));
  }, []);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const seededRef = useRef(false); // ensure we seed only once
  const pvMgrRef = useRef(null);
  const lastBarRef = useRef(null);
  const barSpacingRef = useRef(null);
  const timeframeWarningRef = useRef(null);
  const activeSeriesKeyRef = useRef({
    symbol: null,
    interval: null,
    datasource: null,
    provider_id: null,
    venue_id: null,
    exchange: null,
  });
  const symbolRef = useRef(symbol);
  const intervalRef = useRef(interval);
  const datasourceRef = useRef(datasource);
  const exchangeRef = useRef(exchange);
  const providerRef = useRef(providerMgmt.providerId);
  const venueRef = useRef(providerMgmt.venueId);

  const toggleFullscreen = useCallback(() => {
    setIsFullscreen((prev) => !prev);
  }, [setIsFullscreen]);

  const normalizedExchange = useMemo(() => normalizeExchangeId(exchange), [exchange]);
  const supportsLive = useMemo(() => {
    if (datasource === DATASOURCE_IDS.IBKR) {
      return true;
    }
    if (datasource === DATASOURCE_IDS.CCXT) {
      const candidateSlug = normalizedExchange || providerMgmt.venueOptions.find((item) => item.value === providerMgmt.selectedVenueValue)?.slug;
      return candidateSlug ? LIVE_CRYPTO_EXCHANGES.has(candidateSlug) : false;
    }
    return false;
  }, [datasource, normalizedExchange, providerMgmt.selectedVenueValue, providerMgmt.venueOptions]);

  const liveDisabledReason = useMemo(() => {
    if (supportsLive) return null;
    if (datasource === DATASOURCE_IDS.CCXT) {
      if (!normalizedExchange) {
        return 'Select an exchange to enable live updates.';
      }
      if (!LIVE_CRYPTO_EXCHANGES.has(normalizedExchange)) {
        return 'Live crypto updates are currently limited to Binance US.';
      }
    }
    return 'Live updates require a supported real-time datasource.';
  }, [supportsLive, datasource, normalizedExchange]);

  const liveDescription = useMemo(() => {
    if (!supportsLive) return null;
    if (datasource === DATASOURCE_IDS.CCXT) {
      return 'Polling Binance US every ~10s while live mode is active.';
    }
    if (datasource === DATASOURCE_IDS.IBKR) {
      return 'Streaming refreshes roughly every 10s using Interactive Brokers.';
    }
    return 'Live updates poll the selected datasource roughly every 10s.';
  }, [supportsLive, datasource]);

  // Overlay synchronization
  const overlaySync = useOverlaySync({
    chartRef,
    seriesRef,
    pvMgrRef,
    lastBarRef,
    barSpacingRef,
    logger,
    setDataLoading,
  });

  useEffect(() => {
    symbolRef.current = symbol;
  }, [symbol]);

  useEffect(() => {
    setSymbolDraft((prev) => (prev === symbol ? prev : symbol));
  }, [symbol]);

  useEffect(() => {
    intervalRef.current = interval;
  }, [interval]);

  useEffect(() => {
    dataLoadingRef.current = dataLoading;
  }, [dataLoading]);

  useEffect(() => {
    datasourceRef.current = datasource;
  }, [datasource]);

  useEffect(() => {
    exchangeRef.current = exchange;
  }, [exchange]);

  useEffect(() => {
    providerRef.current = providerMgmt.providerId;
  }, [providerMgmt.providerId]);

  useEffect(() => {
    venueRef.current = providerMgmt.venueId;
  }, [providerMgmt.venueId]);

  const showWarning = useCallback((message) => {
    setRangeWarning(message);
    if (timeframeWarningRef.current) clearTimeout(timeframeWarningRef.current);
    timeframeWarningRef.current = setTimeout(() => setRangeWarning(null), 5000);
  }, []);

  const loadChartData = useCallback(async ({
    targetSymbol,
    targetInterval,
    targetRange,
    targetDatasource,
    targetProvider,
    targetVenue,
    targetExchange,
    behavior = 'auto',
    loaderReason,
  } = {}) => {
    const effectiveSymbol = targetSymbol ?? symbolRef.current;
    const effectiveInterval = targetInterval ?? intervalRef.current;
    const effectiveRange = targetRange ?? dateRangeRef.current;
    const effectiveDatasource = (targetDatasource ?? datasourceRef.current) || DEFAULT_DATASOURCE;
    const effectiveProvider = targetProvider ?? providerRef.current;
    const effectiveVenue = targetVenue ?? venueRef.current;
    const effectiveExchangeRaw = targetExchange ?? exchangeRef.current;
    const effectiveExchange = effectiveExchangeRaw ? effectiveExchangeRaw : null;

    const [rangeStartRaw, rangeEndRaw] = effectiveRange || [];
    const startDate = rangeStartRaw instanceof Date ? rangeStartRaw : rangeStartRaw ? new Date(rangeStartRaw) : null;
    let endDate = rangeEndRaw instanceof Date ? rangeEndRaw : rangeEndRaw ? new Date(rangeEndRaw) : null;
    if (modeRef.current === 'live' && !(Array.isArray(targetRange) && targetRange.length >= 2)) {
      endDate = new Date();
    }

    const previousLastBar = lastBarRef.current;
    const canStreamAppend =
      behavior === 'append'
        || (behavior === 'auto' && modeRef.current === 'live' && Boolean(previousLastBar));
    const hasStreamingBaseline = canStreamAppend && previousLastBar?.time != null;
    let hasNewData = false;
    let loadOutcome = 'pending';
    if (!hasStreamingBaseline) {
      const loadingCopy = loaderReason === 'initial'
        ? 'Preparing chart…'
        : behavior === 'append'
          ? 'Fetching live candles…'
          : 'Loading chart window…';
      markChartState('loading', loadingCopy);
    }

    if (!effectiveSymbol || !effectiveInterval || !startDate || !endDate) {
      warn('chart_load_missing_inputs', {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        startISO: startDate?.toISOString(),
        endISO: endDate?.toISOString(),
        datasource: effectiveDatasource,
        exchange: effectiveExchange,
      });
      markChartState('empty', 'Waiting for symbol, timeframe, and window before loading data.');
      loadOutcome = 'blocked';
      return false;
    }

    if (effectiveDatasource === DATASOURCE_IDS.CCXT && !effectiveExchange) {
      warn('chart_load_missing_exchange', { symbol: effectiveSymbol, interval: effectiveInterval });
      showWarning('Select a crypto exchange before loading data.');
      markChartState('empty', 'Choose an exchange to load crypto data.');
      loadOutcome = 'blocked';
      return false;
    }

    const startISO = startDate.toISOString();
    const endISO = endDate.toISOString();

    dataLoadingRef.current = true;
    const loaderContext = loaderReason ?? (!hasStreamingBaseline ? 'default' : null);
    if (loaderContext) {
      setDataLoaderContext(loaderContext);
      setDataLoading(true);
    } else {
      setDataLoaderContext(null);
    }
    try {
      markAttempt();
      const instrumentKey = [
        effectiveDatasource,
        effectiveExchange,
        effectiveProvider,
        effectiveVenue,
        effectiveSymbol,
      ]
        .filter(Boolean)
        .join('|');
      let resolvedInstrumentId = instrumentIdRef.current;
      if (!resolvedInstrumentId || instrumentKeyRef.current !== instrumentKey) {
        instrumentIdRef.current = null;
      }

      info('candles_fetch_start', {
        instrument_id: instrumentIdRef.current,
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        startISO,
        endISO,
        datasource: effectiveDatasource,
        provider_id: effectiveProvider,
        venue_id: effectiveVenue,
        exchange: effectiveExchange,
      });
      let resp;
      try {
        const result = await fetchInstrumentCandles({
          instrumentId: instrumentIdRef.current,
          symbol: effectiveSymbol,
          timeframe: effectiveInterval,
          start: startISO,
          end: endISO,
          datasource: effectiveDatasource,
          providerId: effectiveProvider ?? undefined,
          venueId: effectiveVenue ?? undefined,
          exchange: effectiveExchange ?? undefined,
          resolveIfMissing: true,
        });
        resp = result.candles;
        resolvedInstrumentId = result.instrumentId;
        instrumentIdRef.current = resolvedInstrumentId;
        instrumentKeyRef.current = instrumentKey;
      } catch (resolveError) {
        warn('instrument_resolve_failed', {
          symbol: effectiveSymbol,
          datasource: effectiveDatasource,
          exchange: effectiveExchange,
          provider_id: effectiveProvider,
          venue_id: effectiveVenue,
        });
        showWarning(resolveError?.message || 'Failed to resolve instrument.');
        markChartState('empty', resolveError?.message || 'Unable to resolve instrument for this symbol.');
        loadOutcome = 'error';
        return { ok: false, reason: 'instrument_resolve_failed' };
      }

      if (!Array.isArray(resp) || resp.length === 0) {
        warn('no data', { symbol: effectiveSymbol, interval: effectiveInterval, behavior });
        markSuccess();
        if (!canStreamAppend) {
          showWarning('No candles found for the selected window. Try a different symbol, range, or datasource.');
          markChartState('empty', 'No candles found for this window. Try a different symbol, range, or datasource.');
        } else {
          debug('live_refresh_empty_batch', { startISO, endISO });
        }
        loadOutcome = 'empty';
        return { ok: false, reason: 'empty' };
      }

      const data = resp
        .filter((c) => c && typeof c.time === 'number')
        .map((c) => ({
          time: c.time,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }));

      if (!seriesRef.current) {
        warn('series missing');
        return { ok: false, reason: 'series_missing' };
      }

      const shouldApplyPriceFormat = effectiveDatasource === DATASOURCE_IDS.CCXT && !hasStreamingBaseline;
      if (shouldApplyPriceFormat) {
        const format = deriveCcxtPriceFormat(resp);
        if (format) {
          seriesRef.current.applyOptions({ priceFormat: format });
        }
      }

      let appendedBars = 0;
      let touchedBars = 0;
      let minStep = canStreamAppend ? barSpacingRef.current ?? Infinity : Infinity;

      if (hasStreamingBaseline) {
        const prevTimeBaseline = previousLastBar.time;
        const incremental = data.filter((candle) => candle.time >= prevTimeBaseline);
        if (incremental.length) hasNewData = true;
        if (incremental.length === 0) {
          debug('live_refresh_no_changes', { symbol: effectiveSymbol, interval: effectiveInterval, prevTime: prevTimeBaseline });
        }
        for (const candle of incremental) {
          const priorTime = lastBarRef.current?.time ?? prevTimeBaseline;
          seriesRef.current.update(candle);
          if (Number.isFinite(priorTime)) {
            const step = candle.time - priorTime;
            if (Number.isFinite(step) && step > 0 && step < minStep) {
              minStep = step;
            }
          }
          if (Number.isFinite(priorTime) && candle.time > priorTime) {
            appendedBars += 1;
          } else if (Number.isFinite(priorTime) && candle.time === priorTime) {
            touchedBars += 1;
          }
          lastBarRef.current = candle;
        }
        // Preserve the current viewport position so incremental updates feel seamless.
      } else {
        seriesRef.current.setData(data);
        lastBarRef.current = data.at(-1) ?? null;
        hasNewData = data.length > 0;

        if (data.length > 1) {
          for (let i = 1; i < data.length; i += 1) {
            const step = data[i].time - data[i - 1].time;
            if (Number.isFinite(step) && step > 0 && step < minStep) {
              minStep = step;
            }
          }
        } else {
          minStep = Infinity;
        }

        if (!previousLastBar) {
          const first = data[0]?.time;
          const last = data.at(-1)?.time;
          if (chartRef.current && Number.isFinite(first) && Number.isFinite(last)) {
            const span = Math.max(1, last - first);
            const pad = Math.max(1, Math.floor(span * 0.05));
            const scaleApi = chartRef.current.timeScale();
            scaleApi.setVisibleRange({ from: first - pad, to: last + pad });
            scaleApi.scrollToPosition(0, false);
          }
        }

        try {
          const priceScaleApi = typeof seriesRef.current?.priceScale === 'function'
            ? seriesRef.current.priceScale()
            : null;
          priceScaleApi?.applyOptions?.({ autoScale: true });
          priceScaleApi?.setAutoScale?.(true);
        } catch (scaleErr) {
          debug('price_scale_autoscale_failed', scaleErr);
        }
      }

      let nextSpacing = barSpacingRef.current;
      if (Number.isFinite(minStep) && minStep > 0 && minStep !== Infinity) {
        nextSpacing = minStep;
      } else if (!hasStreamingBaseline) {
        nextSpacing = data.length > 1 ? nextSpacing : null;
      }
      barSpacingRef.current = nextSpacing;

      pvMgrRef.current?.updateVABlockContext({
        lastSeriesTime: lastBarRef.current?.time,
        barSpacing: barSpacingRef.current,
      });

      const refreshAt = new Date();
      info('candles_fetch_success', {
        instrument_id: instrumentIdRef.current,
        points: data.length,
        first: data[0]?.time,
        last: data.at(-1)?.time,
        appendedBars,
        touchedBars,
        behavior,
        streaming: hasStreamingBaseline,
      });

      markSuccess();
      setLastRefreshAt(refreshAt);
      updateChart?.(chartId, {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        dateRange: [startDate, endDate],
        datasource: effectiveDatasource,
        provider_id: effectiveProvider,
        venue_id: effectiveVenue,
        exchange: effectiveExchange,
        instrument_id: instrumentIdRef.current,
        lastUpdatedAt: refreshAt.toISOString(),
      });

      activeSeriesKeyRef.current = {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        datasource: effectiveDatasource,
        provider_id: effectiveProvider,
        venue_id: effectiveVenue,
        exchange: effectiveExchange,
        instrument_id: instrumentIdRef.current,
      };

      loadOutcome = 'success';
      return {
        ok: true,
        appended: appendedBars > 0,
        touched: touchedBars > 0,
        replaced: !(canStreamAppend && previousLastBar?.time != null),
        points: data.length,
      };
    } catch (e) {
      markError(e);
      error('candles_fetch_failed', e);
      markChartState('error', 'Unable to load chart data. Please retry or adjust inputs.');
      loadOutcome = 'error';
      return { ok: false, reason: 'error' };
    } finally {
      dataLoadingRef.current = false;
      if (loaderContext) {
        setDataLoading(false);
        setDataLoaderContext(null);
      }
      if (loadOutcome === 'success' || hasNewData) {
        markChartState('ready', null);
      }
    }
  }, [info, warn, error, markAttempt, markSuccess, markError, updateChart, chartId, showWarning, debug, setLastRefreshAt, markChartState]);

  const refreshLive = useCallback(async () => {
    if (!supportsLive) {
      return false;
    }

    if (dataLoadingRef.current) {
      debug('live_refresh_skipped_busy');
      return false;
    }

    const now = new Date();
    const lookbackMs = clampLookbackDays(windowConfig.liveLookbackDays) * DAY_MS;
    const fallbackStart = new Date(now.getTime() - lookbackMs);

    const [rangeStartRaw] = dateRangeRef.current || [];
    const candidateStart = rangeStartRaw instanceof Date
      ? rangeStartRaw
      : rangeStartRaw
        ? new Date(rangeStartRaw)
        : null;
    const startDate = candidateStart && candidateStart <= now
      ? (candidateStart < fallbackStart ? fallbackStart : candidateStart)
      : fallbackStart;

    dateRangeRef.current = [startDate, now];

    const result = await loadChartData({
      targetSymbol: symbolRef.current,
      targetInterval: intervalRef.current,
      targetRange: [startDate, now],
      targetDatasource: datasourceRef.current,
      targetExchange: exchangeRef.current,
      behavior: 'append',
    });

    if (result?.ok) {
      if (result?.appended) {
        debug('live_refresh_appended', { appended: result.appended, touched: result.touched });
      }
      if (result?.replaced || result?.appended) {
        bumpRefresh?.(chartId);
      }
    }

    return Boolean(result?.ok);
  }, [supportsLive, loadChartData, debug, bumpRefresh, chartId, windowConfig.liveLookbackDays]);

  const { mode, setMode } = useLiveDataMode({ supportsLive, onRefresh: refreshLive, logger });

  useEffect(() => {
    modeRef.current = mode;
  }, [mode]);

  useEffect(() => {
    if (mode === 'live') {
      return;
    }
    if (windowConfig.historicalWindowMode !== HISTORICAL_WINDOW_MODES.LOOKBACK) {
      return;
    }
    const now = new Date();
    const normalized = clampLookbackDays(windowConfig.historicalLookbackDays);
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    dateRangeRef.current = nextRange;
    windowConfig.setDateRange(nextRange);
  }, [mode, windowConfig]);

  useEffect(() => {
    if (mode !== 'live') {
      return;
    }
    const now = new Date();
    const normalized = clampLookbackDays(windowConfig.liveLookbackDays);
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    dateRangeRef.current = nextRange;
    windowConfig.setDateRange(nextRange);
  }, [mode, windowConfig]);

  const lastModeRef = useRef('historical');
  useEffect(() => {
    if (lastModeRef.current === 'live' && mode !== 'live' && !supportsLive) {
      const baseReason = liveDisabledReason?.replace(/\.*$/, '');
      const message = `${baseReason || 'Live mode is not available for the selected datasource'}. Reverting to historical mode.`;
      showWarning(message);
    }
    lastModeRef.current = mode;
  }, [mode, supportsLive, showWarning, liveDisabledReason]);

  useEffect(() => {
    if (mode === 'live') {
      setRangeWarning(null);
    }
  }, [mode]);

  // Create chart once.
  useEffect(() => {
    const el = chartMountNode;
    if (!el || chartRef.current) return;

    const initialInterval = intervalRef.current;
    const initialSymbol = symbolRef.current;
    const initialRange = dateRangeRef.current;

    chartRef.current = createChart(el, {
      ...options,
      width: el.clientWidth,
      height: el.clientHeight || 400,
      timeScale: deriveTimeScaleOptions(initialInterval),
    });

    const series = chartRef.current.addSeries(CandlestickSeries, {
      ...seriesOptions,
      priceScaleId: 'right',
    });
    seriesRef.current = series;

    // Create pane view manager.
    pvMgrRef.current = new PaneViewManager(chartRef.current);


    registerChart?.(chartId, {
      get chart() { return chartRef.current; },
      get series() { return seriesRef.current; }
    });

    void loadChartData({ loaderReason: 'initial' });

    if (!seededRef.current) {
      updateChart?.(chartId, {
        symbol: initialSymbol,
        interval: initialInterval,
        dateRange: initialRange,
        datasource: datasourceRef.current,
        provider_id: providerRef.current,
        venue_id: venueRef.current,
        exchange: exchangeRef.current || null,
      });
      bumpRefresh?.(chartId); // trigger initial indicator load
      seededRef.current = true;
    }

    info('chart_created');

    const overlayHandles = overlaySync.overlayHandlesRef.current;

    return () => {
      try {
        overlayHandles?.priceLines?.forEach(h => {
          try {
            seriesRef.current?.removePriceLine(h);
          } catch {
            // ignore failures when price line already removed
          }
        });
        pvMgrRef.current?.destroy();
        pvMgrRef.current = null;
        chartRef.current?.remove();
        chartRef.current = null;
        seriesRef.current = null;
        info('chart_removed');
      } catch (e) {
        error('cleanup failed', e);
      }
    };
  }, [chartId, registerChart, updateChart, bumpRefresh, info, error, loadChartData, chartMountNode]);

  useEffect(() => {
    if (!chartRef.current) return;
    const scaleOpts = deriveTimeScaleOptions(interval);
    chartRef.current.applyOptions({ timeScale: scaleOpts });
    debug('time_scale_updated', {
      interval,
      timeVisible: scaleOpts.timeVisible,
      secondsVisible: scaleOpts.secondsVisible,
    });
  }, [interval, debug]);

  // Resize via ResizeObserver.
  useEffect(() => {
    const el = chartMountNode;
    if (!el || !chartRef.current) return;

    const ro = new ResizeObserver(([entry]) => {
      const r = entry?.contentRect; if (!r) return;
      chartRef.current.applyOptions({ width: r.width, height: r.height });
      debug('chart_resize', { width: r.width, height: r.height });
    });

    ro.observe(el);
    return () => ro.disconnect();
  }, [debug, chartMountNode]);

  useEffect(() => {
    const onKey = (e) => {
      if (e.key === '/' && !e.metaKey && !e.ctrlKey && !e.altKey) {
        const el = e.target;
        const tag = (el?.tagName || '').toLowerCase();
        const editable = el?.isContentEditable || tag === 'input' || tag === 'textarea';
        if (!editable) { e.preventDefault(); setPalOpen(true); }
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  useEffect(() => {
    if (connectionStatus === 'error') {
      setConnectionNotice(connectionMessage);
    } else {
      setConnectionNotice(null);
    }
  }, [connectionStatus, connectionMessage]);

  useEffect(() => {
    updateChart?.(chartId, {
      connectionStatus,
      connectionMessage,
    });
  }, [chartId, connectionStatus, connectionMessage, updateChart]);

  useEffect(() => () => {
    if (timeframeWarningRef.current) {
      clearTimeout(timeframeWarningRef.current);
    }
  }, []);

  const handleSymbolInputChange = useCallback((raw) => {
    const next = (raw ?? '').toString().toUpperCase();
    setSymbolDraft(next);
  }, []);


  // React to overlay changes.
  useEffect(() => {
    if (!chartState) return;
    overlaySync.syncOverlays(chartState.overlays || []);
  }, [chartState, overlaySync]);

  // Apply handler.
  const handleApply = useCallback(async (overrides = {}, options = {}) => {
    const nextSymbol = overrides.symbol ?? symbol;
    const nextInterval = overrides.interval ?? interval;
    const nextProvider = overrides.provider_id ?? providerMgmt.providerId;
    const nextVenue = overrides.venue_id ?? providerMgmt.venueId;
    const fallbackRange = modeRef.current === 'live' ? dateRangeRef.current : windowConfig.dateRange;
    const rawRange = overrides.dateRange ?? fallbackRange;
    const normalizedRange = Array.isArray(rawRange)
      ? rawRange.map((value) => (value instanceof Date ? value : value ? new Date(value) : value))
      : [];
    const nextDatasource = overrides.datasource ?? datasource;
    const nextExchange = overrides.exchange ?? exchange;
    const start = normalizedRange[0] instanceof Date && !Number.isNaN(normalizedRange[0]?.getTime())
      ? normalizedRange[0]
      : null;
    const end = normalizedRange[1] instanceof Date && !Number.isNaN(normalizedRange[1]?.getTime())
      ? normalizedRange[1]
      : null;
    if (nextDatasource === 'CCXT' && !nextExchange) {
      warn('apply_missing_exchange', { chartId, symbol: nextSymbol });
      showWarning('Select a crypto exchange before loading data.');
      markChartState('empty', 'Pick an exchange to load this crypto pair.');
      return null;
    }

    if (start && end) {
      dateRangeRef.current = [start, end];
    }

    const effectiveRange = start && end ? [start, end] : normalizedRange;

    setRangeWarning(null);
    info('apply', {
      chartId,
      symbol: nextSymbol,
      interval: nextInterval,
      dateRange: effectiveRange,
      datasource: nextDatasource,
      provider_id: nextProvider,
      venue_id: nextVenue,
      exchange: nextExchange,
    });
    const prevKey = activeSeriesKeyRef.current;
    const symbolChanged = prevKey.symbol !== nextSymbol;
    const isSeriesChange =
      symbolChanged
      || prevKey.interval !== nextInterval
      || prevKey.datasource !== nextDatasource
      || prevKey.provider_id !== nextProvider
      || prevKey.venue_id !== nextVenue
      || prevKey.exchange !== nextExchange;

    if (isSeriesChange) {
      lastBarRef.current = null;
      barSpacingRef.current = null;
      try {
        seriesRef.current?.setData?.([]);
      } catch {
        // ignore failures caused by interim series resets
      }
    }
    overlaySync.syncOverlays([]); // clear overlays on apply
    updateChart?.(chartId, {
      symbol: nextSymbol,
      interval: nextInterval,
      dateRange: effectiveRange,
      datasource: nextDatasource,
      provider_id: nextProvider,
      venue_id: nextVenue,
      exchange: nextExchange || null,
      overlays: [],
      overlayLoading: false,
    });

    const behavior = options.behavior ?? 'replace';
    const result = await loadChartData({
      targetSymbol: nextSymbol,
      targetInterval: nextInterval,
      targetRange: effectiveRange,
      targetDatasource: nextDatasource,
      targetProvider: nextProvider,
      targetVenue: nextVenue,
      targetExchange: nextExchange,
      behavior,
      loaderReason: symbolChanged ? 'symbol-change' : undefined,
    });

    if (result?.ok && (result.replaced || result.appended)) {
      bumpRefresh?.(chartId);
    }

    return result;
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, windowConfig.dateRange, datasource, exchange, warn, overlaySync, showWarning, providerMgmt.providerId, providerMgmt.venueId, markChartState]);

  const handleSymbolInputCommit = useCallback(() => {
    const sanitized = (symbolDraft ?? '').toString().trim().toUpperCase();
    if (!sanitized) {
      setSymbolDraft(symbol);
      return;
    }

    const changed = sanitized !== symbol;
    setSymbolDraft(sanitized);

    if (!changed) {
      return;
    }

    setSymbol(sanitized);

    if (modeRef.current !== 'live') {
      void handleApply({ symbol: sanitized }, { behavior: 'replace' });
    }
  }, [symbolDraft, symbol, handleApply]);

  const applySymbol = useCallback((input) => {
    const payload = typeof input === 'string' ? { symbol: input } : (input ?? {});
    const rawSymbol = payload.symbol ?? payload.s;
    const sanitized = (rawSymbol ?? '').toString().trim().toUpperCase();
    if (!sanitized) {
      setPalOpen(false);
      return;
    }

    const normalizedInterval = (payload.timeframe ?? payload.interval ?? '').toString().trim();
    const normalizedDatasource = (payload.datasource ?? '').toString().trim().toUpperCase();
    const normalizedExchange = (payload.exchange ?? '').toString().trim().toUpperCase();

    const overrides = { symbol: sanitized };
    let changed = sanitized !== symbol;

    if (normalizedInterval) {
      overrides.interval = normalizedInterval;
      if (normalizedInterval !== interval) {
        changed = true;
        setInterval(normalizedInterval);
      }
    }

    if (normalizedDatasource) {
      overrides.datasource = normalizedDatasource;
      if (normalizedDatasource !== datasource) {
        changed = true;
        setDatasource(normalizedDatasource);
      }
    }

    if (normalizedExchange) {
      overrides.exchange = normalizedExchange;
      if (normalizedExchange !== exchange) {
        changed = true;
        setExchange(normalizedExchange);
      }
    }

    setSymbolDraft(sanitized);
    setSymbol(sanitized);
    setPalOpen(false);

    if (changed && modeRef.current !== 'live') {
      void handleApply(overrides, { behavior: 'replace' });
    }
  }, [symbol, interval, datasource, exchange, handleApply]);

  const handleLiveLookbackCommit = useCallback(() => {
    const result = windowConfig.handleLiveLookbackCommit();
    if (modeRef.current === 'live' && supportsLive && result?.changed) {
      void handleApply({ dateRange: result.nextRange }, { behavior: 'replace' });
    }
  }, [windowConfig, supportsLive, handleApply]);

  const handleLiveLookbackPresetSelect = useCallback((days) => {
    const result = windowConfig.handleLiveLookbackPresetSelect(days);
    if (modeRef.current === 'live' && supportsLive) {
      void handleApply({ dateRange: result.nextRange }, { behavior: 'replace' });
    }
  }, [windowConfig, supportsLive, handleApply]);

  const lastAppliedParamsRef = useRef({ symbol, interval, datasource, exchange });

  useEffect(() => {
    const prev = lastAppliedParamsRef.current;
    const changed =
      prev.symbol !== symbol
      || prev.interval !== interval
      || prev.datasource !== datasource
      || prev.exchange !== exchange;

    if (!changed) {
      return;
    }

    lastAppliedParamsRef.current = { symbol, interval, datasource, exchange };

    if (mode !== 'live' || !supportsLive) {
      return;
    }

    const liveRange = Array.isArray(dateRangeRef.current) && dateRangeRef.current.length === 2
      ? dateRangeRef.current
      : windowConfig.dateRange;

    void handleApply({
      symbol,
      interval,
      datasource,
      exchange,
      dateRange: liveRange,
    }, { behavior: 'replace' });
  }, [mode, symbol, interval, datasource, exchange, handleApply, windowConfig.dateRange, supportsLive]);

  useEffect(() => {
    if (mode === 'live') {
      return;
    }

    const current = dateRangeRef.current;
    if (!Array.isArray(current) || current.length !== 2) {
      return;
    }
    const [start, end] = current;
    if (!(start instanceof Date) || !(end instanceof Date)) {
      return;
    }

    windowConfig.setDateRange((prev) => {
      const prevStart = prev?.[0] instanceof Date ? prev[0].getTime() : null;
      const prevEnd = prev?.[1] instanceof Date ? prev[1].getTime() : null;
      if (prevStart === start.getTime() && prevEnd === end.getTime()) {
        return prev;
      }
      return [start, end];
    });
  }, [mode, windowConfig]);

  function useBusyDelay(busy, ms = 250) {
    const [show, setShow] = useState(false);
    useEffect(() => {
      if (busy) {
        const t = setTimeout(() => setShow(true), ms);
        return () => clearTimeout(t);
      }
      setShow(false);
      return undefined;
    }, [busy, ms]);
    return show;
  }

  const busyState = chartState?.overlayLoading || chartState?.signalsLoading || dataLoading;
  const loaderActive = useBusyDelay(busyState, dataLoaderContext === 'symbol-change' ? 0 : 250);
  const loaderMessage = chartState?.signalsLoading
    ? 'Generating signals…'
    : chartState?.overlayLoading
      ? 'Loading overlays…'
      : dataLoading
        ? dataLoaderContext === 'symbol-change'
          ? 'Loading new instrument…'
          : dataLoaderContext === 'initial'
            ? 'Preparing chart…'
            : 'Loading chart…'
        : mode === 'live'
          ? 'Streaming latest data…'
          : 'Loading chart…';

  const statusTextClass = statusStyles.text ?? 'text-slate-300';

  const lastRefreshCopy = useMemo(() => {
    if (dataLoading) {
      return mode === 'live' ? 'Streaming latest data…' : 'Refreshing data…';
    }

    if (!lastRefreshAt) {
      return mode === 'live'
        ? 'Live mode armed — awaiting first tick.'
        : 'No data fetched yet.';
    }

    try {
      const formatted = new Intl.DateTimeFormat(undefined, {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).format(lastRefreshAt);
      return mode === 'live'
        ? `Live mode — last update ${formatted}`
        : `Last refreshed ${formatted}`;
    } catch {
      const fallback = lastRefreshAt instanceof Date
        ? lastRefreshAt.toLocaleTimeString()
        : new Date(lastRefreshAt).toLocaleTimeString();
      return mode === 'live'
        ? `Live mode — last update ${fallback}`
        : `Last refreshed ${fallback}`;
    }
  }, [dataLoading, lastRefreshAt, mode]);

  const liveMode = mode === 'live';
  const symbolDisplay = (symbol || '—').toString().toUpperCase();
  const intervalDisplay = (interval ? interval.toString() : '—').toUpperCase();
  const datasourceDisplay = useMemo(() => {
    const entry = providerMgmt.providerOptions.find((item) => item.value === providerMgmt.providerId);
    return entry?.label || 'Markets data';
  }, [providerMgmt.providerId, providerMgmt.providerOptions]);

  const venueDisplay = useMemo(() => {
    const entry = providerMgmt.venueOptions.find((item) => item.value === providerMgmt.selectedVenueValue);
    return entry?.label || null;
  }, [providerMgmt.selectedVenueValue, providerMgmt.venueOptions]);

  const instrumentMeta = useMemo(() => {
    const parts = [datasourceDisplay, venueDisplay].filter(Boolean);
    return parts.join(' • ');
  }, [datasourceDisplay, venueDisplay]);

  const chartSurface = (
    <ChartSurface
      containerRef={attachChartContainerRef}
      isFullscreen={isFullscreen}
      toggleFullscreen={toggleFullscreen}
      symbolDisplay={symbolDisplay}
      intervalDisplay={intervalDisplay}
      instrumentMeta={instrumentMeta}
      chartStateNotice={chartStateNotice}
      windowSummary={windowConfig.windowSummary}
      palOpen={palOpen}
      setPalOpen={setPalOpen}
      applySymbol={applySymbol}
      loaderActive={loaderActive}
      loaderMessage={loaderMessage}
    />
  );

  const renderedChartSurface = isFullscreen && fullscreenHost
    ? createPortal(chartSurface, fullscreenHost)
    : chartSurface;

  return (
    <>
      <div className="space-y-4">
        {connectionNotice && (
          <div className="flex items-start gap-3 rounded-[22px] border border-rose-500/40 bg-rose-500/10 px-5 py-4 text-sm text-rose-100 shadow-lg shadow-rose-900/40">
            <span className="mt-0.5 text-lg">⚠️</span>
            <div>
              <p className="font-semibold tracking-tight">Connection issue</p>
              <p className="text-xs text-rose-100/80">{connectionNotice}</p>
            </div>
          </div>
        )}

        {rangeWarning && (
          <div className="flex items-center gap-2 rounded-[22px] border border-amber-400/35 bg-amber-500/10 px-5 py-4 text-sm text-amber-100 shadow-lg shadow-amber-900/30">
            <span className="text-lg">⚠️</span>
            <span className="font-medium tracking-tight">{rangeWarning}</span>
          </div>
        )}

        {/* Trading UI control bar */}
        <div className="rounded-3xl border border-white/8 bg-gradient-to-r from-[#0d111c]/95 via-[#0f1626]/95 to-[#0d111c]/95 shadow-[0_30px_120px_-80px_rgba(0,0,0,0.8)]">
          <div className="flex flex-wrap items-end gap-4 border-b border-white/5 px-5 pb-4 pt-4">
            <div className="flex flex-wrap items-end gap-4">
              <div className="flex flex-col gap-1">
                <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">Symbol</span>
                <div className="relative">
                  <input
                    type="text"
                    value={symbolDraft}
                    onChange={(e) => handleSymbolInputChange(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') handleSymbolInputCommit();
                      if (e.key === 'Escape') handleSymbolInputChange(symbol || '');
                    }}
                    placeholder="CL, ES, BTC..."
                    className="w-40 rounded-xl border border-white/10 bg-[#0a0f1a]/80 px-3 py-2 font-mono text-sm font-semibold uppercase tracking-[0.22em] text-slate-100 placeholder-slate-600 shadow-[0_10px_30px_rgba(0,0,0,0.35)] outline-none transition focus:border-[color:var(--accent-alpha-40)] focus:ring-2 focus:ring-[color:var(--accent-ring-strong)]"
                  />
                  <button
                    type="button"
                    onClick={() => setPalOpen(true)}
                    className="absolute right-2 top-1/2 -translate-y-1/2 rounded-full bg-white/5 p-1.5 text-slate-400 transition hover:text-[color:var(--accent-text-soft)]"
                    title="Symbol palette"
                  >
                    <svg className="h-3.5 w-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 13m-5.5 0a5.5 5.5 0 11-11 0 5.5 5.5 0 0111 0z" />
                    </svg>
                  </button>
                </div>
              </div>

              <div className="flex flex-col gap-1">
                <span className="text-[11px] uppercase tracking-[0.28em] text-slate-500">Timeframe</span>
                <div className="flex items-center gap-1 rounded-2xl border border-white/10 bg-black/30 p-1">
                  {['1m', '5m', '15m', '1h', '4h', '1d', '1w'].map((tf) => (
                    <button
                      key={tf}
                      onClick={() => setInterval(tf)}
                      className={`rounded-xl px-3 py-1 text-xs font-semibold tracking-[0.22em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                        interval === tf
                          ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] shadow-inner ring-1 ring-[color:var(--accent-ring)]'
                          : 'text-slate-400 hover:text-slate-100 hover:bg-white/5'
                      }`}
                    >
                      {tf.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="ml-auto flex flex-wrap items-end gap-3">
              <div className="w-48 min-w-[12rem]">
                <DropdownSelect
                  label="Provider"
                  value={providerMgmt.providerId}
                  onChange={providerMgmt.handleProviderChange}
                  options={providerMgmt.providerOptions}
                  placeholder="Select provider"
                  disabled={providerMgmt.providersLoading}
                />
              </div>
              <div className="w-48 min-w-[12rem]">
                <DropdownSelect
                  label="Venue"
                  value={providerMgmt.selectedVenueValue}
                  onChange={providerMgmt.handleVenueChange}
                  options={providerMgmt.venueOptions}
                  placeholder="Select venue"
                  disabled={!providerMgmt.providerId || providerMgmt.venueOptions.length === 0}
                />
              </div>
            </div>
          </div>

          {(providerMgmt.selectedVenueStatus.state !== 'available' || providerMgmt.selectedProviderStatus.state !== 'available') && (
            <div className="flex items-center justify-between gap-3 border-b border-white/5 bg-black/20 px-5 py-3 text-sm text-slate-200">
              <div className="flex flex-wrap items-center gap-2">
                <span className="rounded-full bg-amber-500/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.24em] text-amber-200">Credentials</span>
                <span>
                  {providerMgmt.selectedVenueStatus.state !== 'available'
                    ? `Missing secrets for ${providerMgmt.selectedVenueValue || 'venue'}: ${(providerMgmt.selectedVenueStatus.missing || []).join(', ')}`
                    : `Missing secrets for ${providerMgmt.providerId || 'provider'}: ${(providerMgmt.selectedProviderStatus.missing || []).join(', ')}`}
                </span>
              </div>
              <button
                type="button"
                onClick={() => providerMgmt.openCredentialsModal(providerMgmt.providerId, providerMgmt.selectedVenueValue, providerMgmt.selectedVenueStatus.required?.length ? providerMgmt.selectedVenueStatus.required : providerMgmt.selectedProviderStatus.required)}
                className="inline-flex items-center gap-2 rounded-full border border-[color:var(--accent-alpha-40)] px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.22em] text-[color:var(--accent-text-soft)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-10)]"
              >
                Add API keys
              </button>
            </div>
          )}

          <div className="flex flex-wrap items-center gap-4 px-5 py-3">
            <div className="flex flex-wrap items-center gap-3">
              <div className="flex items-center gap-1 rounded-2xl border border-white/10 bg-[#0b111e]/80 p-1">
                <button
                  onClick={() => setMode('historical')}
                  className={`rounded-xl px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.25em] transition ${
                    mode === 'historical'
                      ? 'bg-white/10 text-slate-100 ring-1 ring-[color:var(--accent-ring)]'
                      : 'text-slate-500 hover:text-slate-200'
                  }`}
                >
                  Hist
                </button>
                <button
                  onClick={() => setMode('live')}
                  disabled={!supportsLive}
                  className={`rounded-xl px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.25em] transition ${
                    mode === 'live'
                      ? 'bg-[color:var(--accent-alpha-25)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring-strong)]'
                      : 'text-slate-500 hover:text-slate-200 disabled:opacity-30'
                  }`}
                  title={liveDisabledReason || ''}
                >
                  Live
                </button>
              </div>

              <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-[#0b111e]/80 px-3 py-2">
                <div className="flex items-center gap-1 rounded-xl border border-white/10 bg-black/30 p-1">
                  <button
                    onClick={() => windowConfig.handleHistoricalModeToggle(windowConfig.HISTORICAL_WINDOW_MODES.LOOKBACK)}
                    className={`rounded-lg px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.22em] transition ${
                      windowConfig.isLookbackMode || liveMode
                        ? 'bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring)]'
                        : 'text-slate-400 hover:text-slate-100'
                    }`}
                  >
                    Lookback
                  </button>
                  {!liveMode && (
                    <button
                      onClick={() => windowConfig.handleHistoricalModeToggle(windowConfig.HISTORICAL_WINDOW_MODES.RANGE)}
                      className={`rounded-lg px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.22em] transition ${
                        windowConfig.isRangeMode && !liveMode
                          ? 'bg-white/10 text-slate-100 ring-1 ring-[color:var(--accent-ring)]'
                          : 'text-slate-400 hover:text-slate-100'
                      }`}
                    >
                      Range
                    </button>
                  )}
                </div>
                <span className="text-xs font-semibold tracking-tight text-slate-200">{windowConfig.windowSummary}</span>
              </div>

              {(liveMode || windowConfig.isLookbackMode) && (
                <div className="flex items-center gap-2 rounded-2xl border border-white/10 bg-[#0b111e]/80 px-3 py-2">
                  <span className="text-[11px] uppercase tracking-[0.25em] text-slate-500">Presets</span>
                  {windowConfig.quickLookbackPresets.slice(0, 5).map((preset) => (
                    <button
                      key={preset.label}
                      onClick={() => (liveMode ? windowConfig.handleLiveLookbackPresetSelect(preset.days) : windowConfig.handleHistoricalLookbackChange(preset.days))}
                      className={`rounded-lg px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.22em] transition ${
                        (liveMode ? windowConfig.liveLookbackDays : windowConfig.historicalLookbackDays) === preset.days
                          ? 'bg-[color:var(--accent-alpha-20)] text-[color:var(--accent-text-strong)] ring-1 ring-[color:var(--accent-ring)]'
                          : 'text-slate-400 hover:text-slate-100'
                      }`}
                    >
                      {preset.label}
                    </button>
                  ))}
                  <div className="flex items-center gap-1 rounded-lg border border-white/10 bg-black/30 px-2 py-1">
                    <input
                      type="text"
                      inputMode="numeric"
                      value={liveMode ? windowConfig.liveLookbackInput : windowConfig.historicalLookbackInput}
                      onChange={liveMode ? windowConfig.handleLiveLookbackInputChange : windowConfig.handleHistoricalLookbackInputChange}
                      onBlur={liveMode ? windowConfig.handleLiveLookbackCommit : windowConfig.handleHistoricalLookbackCommit}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter') (liveMode ? windowConfig.handleLiveLookbackCommit : windowConfig.handleHistoricalLookbackCommit)();
                      }}
                      className="w-12 bg-transparent text-center text-xs font-semibold tracking-[0.22em] text-slate-100 outline-none placeholder-slate-600"
                      placeholder="90"
                    />
                    <span className="text-[10px] uppercase tracking-[0.24em] text-slate-500">Days</span>
                  </div>
                </div>
              )}
            </div>

            <div className="ml-auto flex flex-wrap items-center gap-3">
              <div className="flex items-center gap-2 rounded-full border border-white/10 bg-black/30 px-3 py-1.5 text-[11px] font-mono text-slate-400 shadow-inner">
                <span className={`inline-block h-2 w-2 rounded-full ${dataLoading ? 'animate-pulse bg-amber-400' : 'bg-[color:var(--accent-base)]'}`} />
                <span className={statusTextClass}>{lastRefreshCopy}</span>
              </div>
              <button
                onClick={() => { void handleApply(); }}
                disabled={providerBlocked}
                className="inline-flex items-center gap-2 rounded-full bg-[color:var(--accent-alpha-25)] px-4 py-2 text-[11px] font-semibold uppercase tracking-[0.25em] text-[color:var(--accent-text-bright)] shadow-[0_10px_40px_-12px_var(--accent-shadow-strong)] transition hover:bg-[color:var(--accent-alpha-30)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] disabled:cursor-not-allowed disabled:opacity-50"
                title="Refresh data"
              >
                <RotateCcw className="h-4 w-4" />
                Load window
              </button>
            </div>
          </div>

          {windowConfig.isRangeMode && !liveMode && (
            <div className="border-t border-white/5 bg-[#0b101b]/80 px-5 py-3">
              <DateRangePickerComponent
                dateRange={windowConfig.dateRange}
                setDateRange={windowConfig.handleDateRangeSelection}
                disabled={liveMode || !windowConfig.isRangeMode}
              />
            </div>
          )}
        </div>

        {connectionStatus === 'error' && connectionMessage ? (
          <div className="px-5 py-2 text-xs bg-rose-500/10 border-b border-rose-500/30 text-rose-300">
            {connectionMessage}
          </div>
        ) : null}

        {renderedChartSurface}
      </div>
      <CredentialsModal
        isOpen={providerMgmt.credentialsModal.open}
        providerId={providerMgmt.credentialsModal.providerId}
        venueId={providerMgmt.credentialsModal.venueId}
        requiredFields={providerMgmt.credentialsModal.required}
        inputs={providerMgmt.credentialsInputs}
        onInputChange={providerMgmt.setCredentialsInputs}
        saving={providerMgmt.credentialsSaving}
        error={providerMgmt.credentialsError}
        onClose={providerMgmt.closeCredentialsModal}
        onSave={providerMgmt.handleSaveCredentials}
      />
    </>
  )
};
