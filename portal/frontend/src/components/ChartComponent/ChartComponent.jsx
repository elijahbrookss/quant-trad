import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createChart, CandlestickSeries, createSeriesMarkers } from 'lightweight-charts';
import { RotateCcw } from 'lucide-react';
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent';
import { DateRangePickerComponent } from './DateTimePickerComponent';
import { options, seriesOptions } from './ChartOptions';
import { fetchCandleData } from '../../adapters/candle.adapter';
import { useChartState, useChartValue } from '../../contexts/ChartStateContext.jsx';
import { createLogger } from '../../utils/logger.js';
import { PaneViewManager } from '../../chart/paneViews/factory.js';
import { adaptPayload, getPaneViewsFor } from '../../chart/indicators/registry.js';
import LoadingOverlay from '../LoadingOverlay.jsx';
import HotkeyHint from '../HotkeyHint.jsx';
import SymbolPalette from '../SymbolPalette.jsx';
import { useConnectionMonitor } from '../../hooks/useConnectionMonitor.js';
import DropdownSelect from './DropdownSelect.jsx';
import DataModeToggle from './DataModeToggle.jsx';
import { useLiveDataMode } from './hooks/useLiveDataMode.js';
import {
  DATASOURCE_OPTIONS,
  DATASOURCE_IDS,
  MARKET_PROVIDERS,
  CRYPTO_EXCHANGES,
  IB_EXCHANGES,
  DEFAULT_DATASOURCE,
  DEFAULT_MARKET_PROVIDER,
  DEFAULT_CRYPTO_EXCHANGE,
  DEFAULT_IB_EXCHANGE,
} from '../../constants/datasources.js';

// File-level namespace.
const LOG_NS = 'ChartComponent';
const LIVE_LOOKBACK_MS = 90 * 24 * 60 * 60 * 1000;

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

const toRgba = (hex, alpha = 0.12) => {
  if (typeof hex !== 'string') return null;
  const trimmed = hex.trim().replace('#', '');
  if (!(trimmed.length === 3 || trimmed.length === 6)) return null;

  const expand = (value) => value.split('').map((c) => c + c).join('');
  const normalized = trimmed.length === 3 ? expand(trimmed) : trimmed;

  const r = Number.parseInt(normalized.slice(0, 2), 16);
  const g = Number.parseInt(normalized.slice(2, 4), 16);
  const b = Number.parseInt(normalized.slice(4, 6), 16);

  if ([r, g, b].some((n) => Number.isNaN(n))) return null;

  const clampedAlpha = Math.min(Math.max(alpha, 0), 1);
  return `rgba(${r},${g},${b},${clampedAlpha})`;
};

const coalesce = (...values) => {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
};

const toFiniteNumber = (value) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const toIsoFromSeconds = (value) => {
  const numeric = toFiniteNumber(value);
  if (numeric == null) return null;
  try {
    const date = new Date(numeric * 1000);
    if (Number.isNaN(date.valueOf())) return null;
    return date.toISOString();
  } catch {
    return null;
  }
};

const formatPriceDisplay = (value, precision = 2) => {
  const numeric = toFiniteNumber(value);
  if (numeric == null) return 'n/a';
  const digits = Math.min(Math.max(Number(precision) || 2, 2), 8);
  return numeric.toFixed(digits);
};

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

const buildVaBoxSummaryText = ({
  startSec,
  endSec,
  requestedEndSec,
  val,
  vah,
  poc,
  sessions,
  valueAreaId,
  precision,
}) => {
  const parts = [
    `start=${toIsoFromSeconds(startSec) ?? 'n/a'}`,
    `end=${toIsoFromSeconds(endSec) ?? 'n/a'}`,
    `VAL=${formatPriceDisplay(val, precision)}`,
    `VAH=${formatPriceDisplay(vah, precision)}`,
  ];

  if (poc != null) {
    parts.push(`POC=${formatPriceDisplay(poc, precision)}`);
  }
  if (sessions != null) {
    parts.push(`sessions=${sessions}`);
  }
  if (valueAreaId != null) {
    parts.push(`id=${valueAreaId}`);
  }
  if (requestedEndSec != null && requestedEndSec !== endSec) {
    parts.push('extended_to_last_bar=true');
  }

  return parts.join(' | ');
};

export const ChartComponent = ({ chartId }) => {
  // Logger for this file.
  const logger = useMemo(() => createLogger(LOG_NS, { chartId }), [chartId]);
  const { debug, info, warn, error } = logger;

  // Context wiring.
  const { registerChart, updateChart, bumpRefresh } = useChartState();
  const chartState = useChartValue(chartId);

  // Local UI state.
  const [symbol, setSymbol] = useState('CL');
  const [interval, setInterval] = useState('15m');
  const [datasource, setDatasource] = useState(DEFAULT_DATASOURCE);
  const [exchange, setExchange] = useState(DEFAULT_MARKET_PROVIDER);
  const [marketProvider, setMarketProvider] = useState(DEFAULT_MARKET_PROVIDER);
  const [palOpen, setPalOpen] = useState(false);
  const [dateRange, setDateRange] = useState([
    new Date(Date.now() - 90 * 24 * 60 * 60 * 1000),
    new Date()
  ]);
  const [dataLoading, setDataLoading] = useState(false);
  const [rangeWarning, setRangeWarning] = useState(null);
  const [connectionNotice, setConnectionNotice] = useState(null);
  const [lastRefreshAt, setLastRefreshAt] = useState(null);

  const modeRef = useRef('historical');
  const dataLoadingRef = useRef(false);

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
        text: 'text-emerald-200',
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
  const chartContainerRef = useRef(null);
  const chartRef = useRef(null);
  const seriesRef = useRef(null);
  const seededRef = useRef(false); // ensure we seed only once
  const pvMgrRef = useRef(null);
  const lastBarRef = useRef(null);
  const barSpacingRef = useRef(null);
  const timeframeWarningRef = useRef(null);
  const symbolRef = useRef(symbol);
  const intervalRef = useRef(interval);
  const dateRangeRef = useRef(dateRange);
  const datasourceRef = useRef(datasource);
  const exchangeRef = useRef(exchange);
  const marketProviderRef = useRef(marketProvider);
  const lastCryptoExchangeRef = useRef(DEFAULT_CRYPTO_EXCHANGE);
  const lastMarketProviderRef = useRef(DEFAULT_MARKET_PROVIDER);
  const lastIbExchangeRef = useRef(DEFAULT_IB_EXCHANGE);
  const lastMarketDatasourceRef = useRef(DEFAULT_DATASOURCE);

  const handleDatasourceChange = useCallback((nextId) => {
    if (nextId === DATASOURCE_IDS.CCXT) {
      if (datasourceRef.current !== DATASOURCE_IDS.CCXT) {
        const previousMarketDatasource =
          datasourceRef.current === DATASOURCE_IDS.CCXT
            ? lastMarketDatasourceRef.current || DEFAULT_DATASOURCE
            : datasourceRef.current || DEFAULT_DATASOURCE;

        lastMarketDatasourceRef.current = previousMarketDatasource;
        lastMarketProviderRef.current =
          marketProviderRef.current || lastMarketProviderRef.current || DEFAULT_MARKET_PROVIDER;
        lastIbExchangeRef.current = exchangeRef.current || lastIbExchangeRef.current || DEFAULT_IB_EXCHANGE;
      }

      setDatasource(DATASOURCE_IDS.CCXT);
      setExchange(lastCryptoExchangeRef.current || DEFAULT_CRYPTO_EXCHANGE);
      return;
    }

    const storedDatasource =
      datasourceRef.current === DATASOURCE_IDS.CCXT
        ? lastMarketDatasourceRef.current || DEFAULT_DATASOURCE
        : datasourceRef.current || DEFAULT_DATASOURCE;

    const resolvedDatasource =
      storedDatasource === DATASOURCE_IDS.CCXT ? DEFAULT_DATASOURCE : storedDatasource;

    lastMarketDatasourceRef.current = resolvedDatasource;

    let nextProvider = lastMarketProviderRef.current || DEFAULT_MARKET_PROVIDER;
    if (resolvedDatasource === DATASOURCE_IDS.IBKR) {
      nextProvider = 'ibkr';
    } else if (resolvedDatasource === DATASOURCE_IDS.YFINANCE) {
      nextProvider = 'yfinance';
    }

    lastMarketProviderRef.current = nextProvider;

    setDatasource(resolvedDatasource);
    setMarketProvider(nextProvider);

    if (resolvedDatasource === DATASOURCE_IDS.IBKR) {
      const venue = lastIbExchangeRef.current || DEFAULT_IB_EXCHANGE;
      setExchange(venue);
      lastIbExchangeRef.current = venue;
    } else if (resolvedDatasource === DATASOURCE_IDS.YFINANCE) {
      setExchange('yfinance');
    } else {
      const fallback = lastMarketProviderRef.current || DEFAULT_MARKET_PROVIDER;
      setExchange(fallback);
    }
  }, []);

  const handleExchangeChange = useCallback((nextId) => {
    if (!nextId) return;

    if (datasourceRef.current === DATASOURCE_IDS.CCXT) {
      setExchange(nextId);
      lastCryptoExchangeRef.current = nextId;
      return;
    }

    setMarketProvider(nextId);
    lastMarketProviderRef.current = nextId;

    if (nextId === 'ibkr') {
      const venue = lastIbExchangeRef.current || DEFAULT_IB_EXCHANGE;
      setDatasource(DATASOURCE_IDS.IBKR);
      setExchange(venue);
      lastMarketDatasourceRef.current = DATASOURCE_IDS.IBKR;
      lastIbExchangeRef.current = venue;
      return;
    }

    if (nextId === 'yfinance') {
      setDatasource(DATASOURCE_IDS.YFINANCE);
      setExchange('yfinance');
      lastMarketDatasourceRef.current = DATASOURCE_IDS.YFINANCE;
      return;
    }

    const resolved = nextId || DEFAULT_MARKET_PROVIDER;
    setDatasource(DATASOURCE_IDS.ALPACA);
    setExchange(resolved);
    lastMarketDatasourceRef.current = DATASOURCE_IDS.ALPACA;
  }, []);

  const exchangeSelectOptions = useMemo(() => {
    if (datasource === DATASOURCE_IDS.CCXT) {
      const centralized = CRYPTO_EXCHANGES.filter((ex) => ex.category === 'CEX').map((ex) => ({
        value: ex.value,
        label: ex.label,
        badge: 'CEX',
      }));

      const decentralized = CRYPTO_EXCHANGES.filter((ex) => ex.category === 'DEX').map((ex) => ({
        value: ex.value,
        label: ex.label,
        badge: 'DEX',
      }));

      return [
        ...(centralized.length
          ? [{ label: 'Centralized Exchanges', options: centralized }]
          : []),
        ...(decentralized.length
          ? [{ label: 'Decentralized Exchanges', options: decentralized }]
          : []),
      ];
    }

    return MARKET_PROVIDERS.map((provider) => ({
      value: provider.value,
      label: provider.label,
    }));
  }, [datasource]);

  const selectedExchangeValue = useMemo(() => {
    if (datasource === DATASOURCE_IDS.CCXT) {
      return exchange || DEFAULT_CRYPTO_EXCHANGE;
    }
    return marketProvider || DEFAULT_MARKET_PROVIDER;
  }, [datasource, exchange, marketProvider]);

  const exchangePlaceholder = datasource === DATASOURCE_IDS.CCXT ? 'Select exchange' : 'Select provider';

  const handleIbVenueChange = useCallback((venue) => {
    if (!venue) return;
    setExchange(venue);
    lastIbExchangeRef.current = venue;
  }, []);

  const supportsLive = useMemo(() => datasource === DATASOURCE_IDS.IBKR, [datasource]);
  const liveDisabledReason = useMemo(() => {
    if (supportsLive) return null;
    if (datasource === DATASOURCE_IDS.CCXT) {
      return 'Switch to the Markets datasource and choose Interactive Brokers to stream live data.';
    }
    return 'Live updates require the Interactive Brokers datasource.';
  }, [supportsLive, datasource]);

    // Overlay resource handles.
  const overlayHandlesRef = useRef({ priceLines: [] });

  useEffect(() => {
    symbolRef.current = symbol;
  }, [symbol]);

  useEffect(() => {
    intervalRef.current = interval;
  }, [interval]);

  useEffect(() => {
    dateRangeRef.current = dateRange;
  }, [dateRange]);

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
    marketProviderRef.current = marketProvider;
  }, [marketProvider]);

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
    targetExchange,
  } = {}) => {
    const effectiveSymbol = targetSymbol ?? symbolRef.current;
    const effectiveInterval = targetInterval ?? intervalRef.current;
    const effectiveRange = targetRange ?? dateRangeRef.current;
    const effectiveDatasource = (targetDatasource ?? datasourceRef.current) || DEFAULT_DATASOURCE;
    const effectiveExchangeRaw = targetExchange ?? exchangeRef.current;
    const effectiveExchange = effectiveExchangeRaw ? effectiveExchangeRaw : null;

    const [rangeStartRaw, rangeEndRaw] = effectiveRange || [];
    const startDate = rangeStartRaw instanceof Date ? rangeStartRaw : rangeStartRaw ? new Date(rangeStartRaw) : null;
    let endDate = rangeEndRaw instanceof Date ? rangeEndRaw : rangeEndRaw ? new Date(rangeEndRaw) : null;
    if (modeRef.current === 'live' && !(Array.isArray(targetRange) && targetRange.length >= 2)) {
      endDate = new Date();
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
      return false;
    }

    if (effectiveDatasource === DATASOURCE_IDS.CCXT && !effectiveExchange) {
      warn('chart_load_missing_exchange', { symbol: effectiveSymbol, interval: effectiveInterval });
      showWarning('Select a crypto exchange before loading data.');
      return false;
    }

    const startISO = startDate.toISOString();
    const endISO = endDate.toISOString();

    setDataLoading(true);
    try {
      markAttempt();
      info('candles_fetch_start', {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        startISO,
        endISO,
        datasource: effectiveDatasource,
        exchange: effectiveExchange,
      });
      const resp = await fetchCandleData({
        symbol: effectiveSymbol,
        timeframe: effectiveInterval,
        start: startISO,
        end: endISO,
        datasource: effectiveDatasource,
        exchange: effectiveExchange ?? undefined,
      });

      if (!Array.isArray(resp) || resp.length === 0) {
        warn('no data', { symbol: effectiveSymbol, interval: effectiveInterval });
        markSuccess();
        showWarning('No candles found for the selected window. Try a different symbol, range, or datasource.');
        return false;
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
        return false;
      }

      if (seriesRef.current) {
        if (effectiveDatasource === DATASOURCE_IDS.CCXT) {
          const format = deriveCcxtPriceFormat(resp);
          if (format) {
            seriesRef.current.applyOptions({ priceFormat: format });
          }
        }
        seriesRef.current.setData(data);
      }

      lastBarRef.current = data.at(-1);

      if (data.length > 1) {
        let minStep = Infinity;
        for (let i = 1; i < data.length; i += 1) {
          const step = data[i].time - data[i - 1].time;
          if (Number.isFinite(step) && step > 0 && step < minStep) {
            minStep = step;
          }
        }
        barSpacingRef.current = Number.isFinite(minStep) && minStep > 0 ? minStep : null;
      } else {
        barSpacingRef.current = null;
      }

      pvMgrRef.current?.updateVABlockContext({
        lastSeriesTime: lastBarRef.current?.time,
        barSpacing: barSpacingRef.current,
      });
      const first = data[0]?.time;
      const last = data.at(-1)?.time;
      if (chartRef.current && Number.isFinite(first) && Number.isFinite(last)) {
        const span = Math.max(1, last - first);
        const pad = Math.max(1, Math.floor(span * 0.05));
        const scaleApi = chartRef.current.timeScale();
        scaleApi.setVisibleRange({ from: first - pad, to: last + pad });
        scaleApi.scrollToPosition(0, false);
      } else {
        chartRef.current?.timeScale().scrollToRealTime();
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

      const refreshAt = new Date();
      info('candles_fetch_success', {
        points: data.length,
        first: data[0]?.time,
        last: data.at(-1)?.time,
      });

      markSuccess();
      setLastRefreshAt(refreshAt);
      updateChart?.(chartId, {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        dateRange: [startDate, endDate],
        datasource: effectiveDatasource,
        exchange: effectiveExchange,
        lastUpdatedAt: refreshAt.toISOString(),
      });
      return true;
    } catch (e) {
      markError(e);
      error('candles_fetch_failed', e);
    } finally {
      setDataLoading(false);
    }

    return false;
  }, [info, warn, error, markAttempt, markSuccess, markError, updateChart, chartId, showWarning, debug, setLastRefreshAt]);

  const refreshLive = useCallback(async () => {
    if (!supportsLive) {
      return false;
    }

    if (dataLoadingRef.current) {
      debug('live_refresh_skipped_busy');
      return false;
    }

    const [rangeStartRaw] = dateRangeRef.current || [];
    const startDate = rangeStartRaw instanceof Date
      ? rangeStartRaw
      : rangeStartRaw
        ? new Date(rangeStartRaw)
        : new Date(Date.now() - LIVE_LOOKBACK_MS);
    const now = new Date();

    setDateRange((prev) => {
      if (!Array.isArray(prev) || prev.length !== 2) {
        return [startDate, now];
      }
      const prevStart = prev[0] instanceof Date ? prev[0] : startDate;
      return [prevStart, now];
    });

    return loadChartData({
      targetSymbol: symbolRef.current,
      targetInterval: intervalRef.current,
      targetRange: [startDate, now],
      targetDatasource: datasourceRef.current,
      targetExchange: exchangeRef.current,
    });
  }, [supportsLive, loadChartData, debug, setDateRange]);

  const { mode, setMode } = useLiveDataMode({ supportsLive, onRefresh: refreshLive, logger });

  useEffect(() => {
    modeRef.current = mode;
  }, [mode]);

  const lastModeRef = useRef('historical');
  useEffect(() => {
    if (lastModeRef.current === 'live' && mode !== 'live' && !supportsLive) {
      showWarning('Live mode is only available with Interactive Brokers. Reverting to historical mode.');
    }
    lastModeRef.current = mode;
  }, [mode, supportsLive, showWarning]);

  useEffect(() => {
    if (mode === 'live') {
      setRangeWarning(null);
    }
  }, [mode]);

  // Create chart once.
  useEffect(() => {
    const el = chartContainerRef.current;
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

    loadChartData();

    if (!seededRef.current) {
      updateChart?.(chartId, {
        symbol: initialSymbol,
        interval: initialInterval,
        dateRange: initialRange,
        datasource: datasourceRef.current,
        exchange: exchangeRef.current || null,
      });
      bumpRefresh?.(chartId); // trigger initial indicator load
      seededRef.current = true;
    }

    info('chart_created');

    const overlayHandles = overlayHandlesRef.current;

    return () => {
      try {
        overlayHandles?.priceLines?.forEach(h => {
          try {
            seriesRef.current?.removePriceLine(h);
          } catch {
            // ignore failures when price line already removed
          }
        });
        overlayHandles?.markersApi?.setMarkers?.([]);
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
  }, [chartId, registerChart, updateChart, bumpRefresh, info, error, loadChartData]);

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
    const el = chartContainerRef.current;
    if (!el || !chartRef.current) return;

    const ro = new ResizeObserver(([entry]) => {
      const r = entry?.contentRect; if (!r) return;
      chartRef.current.applyOptions({ width: r.width, height: r.height });
      debug('chart_resize', { width: r.width, height: r.height });
    });

    ro.observe(el);
    return () => ro.disconnect();
  }, [debug]);

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

  const applySymbol = (sym) => {
    setSymbol(sym);
    setPalOpen(false);
  };

  // Overlay refs and syncer.
  const syncOverlays = useCallback((overlays = []) => {
    setDataLoading(true);
    // Guard on required refs.
    if (!seriesRef.current || !chartRef.current) return;

    // Helper: normalize time to seconds.
    const toSec = (t) => {
      if (t == null) return t;
      if (typeof t !== 'number') return t;
      return t > 2e10 ? Math.floor(t / 1000) : t; 
    };

    // 1) Clear existing price lines.
    overlayHandlesRef.current.priceLines.forEach(h => {
      try {
        seriesRef.current.removePriceLine(h);
      } catch {
        // ignore if price line already cleared
      }
    });
    overlayHandlesRef.current.priceLines = [];

    // Ensure markers plugin exists; clear existing markers.
    if (!overlayHandlesRef.current.markersApi) {
      overlayHandlesRef.current.markersApi = createSeriesMarkers(seriesRef.current, []);
    } else {
      overlayHandlesRef.current.markersApi.setMarkers([]);
    }

    pvMgrRef.current?.clearFrame();

    // 2) Build fresh markers and touch points.
    const markers = [];
    const touchPoints = [];
    const boxes = [];
    const signalBubbles = [];
    const allSegments = [];
    const allPolylines = [];

    // 3) Walk overlays and apply.
    for (const ov of overlays) {
      const { type, payload, color, ind_id: indicatorId } = ov || {};
      if (!payload) continue;

      const overlayLogger = logger.child({ indicatorId, indicatorType: type });
      overlayLogger.debug('overlay_payload_received', {
        priceLines: Array.isArray(payload.price_lines) ? payload.price_lines.length : 0,
        markers: Array.isArray(payload.markers) ? payload.markers.length : 0,
        boxes: Array.isArray(payload.boxes) ? payload.boxes.length : 0,
        segments: Array.isArray(payload.segments) ? payload.segments.length : 0,
        polylines: Array.isArray(payload.polylines) ? payload.polylines.length : 0,
      });

      const paneViews = getPaneViewsFor(type);
      const norm = adaptPayload(type, payload, color);
      overlayLogger.debug('overlay_adapted', {
        priceLines: Array.isArray(norm.priceLines) ? norm.priceLines.length : 0,
        markers: Array.isArray(norm.markers) ? norm.markers.length : 0,
        touchPoints: Array.isArray(norm.touchPoints) ? norm.touchPoints.length : 0,
        boxes: Array.isArray(norm.boxes) ? norm.boxes.length : 0,
        segments: Array.isArray(norm.segments) ? norm.segments.length : 0,
        polylines: Array.isArray(norm.polylines) ? norm.polylines.length : 0,
        bubbles: Array.isArray(norm.bubbles) ? norm.bubbles.length : 0,
      });

      // 3a) Price lines.
      if (Array.isArray(payload.price_lines)) {
        for (const pl of payload.price_lines) {
          const handle = seriesRef.current.createPriceLine({
            price: pl.price,
            color: pl.color ?? undefined,
            lineWidth: pl.lineWidth ?? 1,
            lineStyle: pl.lineStyle ?? 0,
            axisLabelVisible: pl.axisLabelVisible ?? false,
            title: pl.title ?? type ?? '',
          });
          overlayHandlesRef.current.priceLines.push(handle);
        }
      }

      // 3b) Markers.
      markers.push(...norm.markers);

      if (Array.isArray(norm.bubbles) && norm.bubbles.length) {
        if (color) {
          signalBubbles.push(...norm.bubbles.map(b => {
            const accentColor = color;
            const backgroundColor = toRgba(accentColor, 0.16) ?? undefined;
            return {
              ...b,
              accentColor,
              backgroundColor,
            };
          }));
        } else {
          signalBubbles.push(...norm.bubbles);
        }
      }

      // 3c) Touch points.
      if (paneViews.includes('touch') && norm.touchPoints?.length) {
        touchPoints.push(...norm.touchPoints.map(p => ({
          ...p,
          time: toSec(p.time),
        })));
      }

      // 3d) VA Boxes.
      if (paneViews.includes('va_box') && norm.boxes?.length) {
        const lastCandleSec = toSec(lastBarRef.current?.time);
        const baseIndex = boxes.length;
        const summaryEntries = [];
        const normalizedBoxes = norm.boxes.map((box, idxInGroup) => {
          const x1 = toSec(box.x1);
          const requestedX2 = toSec(box.x2);
          const extendBox = box.extend !== undefined ? Boolean(box.extend) : false;
          let x2 = requestedX2;

          if (!Number.isFinite(x2)) {
            if (extendBox && Number.isFinite(lastCandleSec)) {
              x2 = lastCandleSec;
            } else {
              x2 = x1;
            }
          } else if (extendBox && Number.isFinite(lastCandleSec) && lastCandleSec > x2) {
            overlayLogger.debug('va_box_span_extended', {
              boxIndex: baseIndex + idxInGroup,
              x1,
              originalX2: requestedX2,
              forcedX2: lastCandleSec,
            });
            x2 = lastCandleSec;
          }

          const pocValue = toFiniteNumber(
            coalesce(
              box.poc,
              box.POC,
              box?.meta?.poc,
              box?.metadata?.poc,
            ),
          );
          const sessions = coalesce(
            box.session_count,
            box.sessions,
            box.sessionCount,
            box?.meta?.session_count,
            box?.metadata?.session_count,
          );
          const valueAreaId = coalesce(
            box.value_area_id,
            box.valueAreaId,
            box.value_areaId,
            box.id,
            box?.meta?.value_area_id,
            box?.metadata?.value_area_id,
          );
          const label = coalesce(
            box.label,
            box.session_label,
            box.session,
            box.profile_label,
          );
          const sourceStart = coalesce(box.start, box.start_date, box.startDate);
          const sourceEnd = coalesce(box.end, box.end_date, box.endDate);

          const y1 = Number(box.y1);
          const y2 = Number(box.y2);
          const precision = Number.isFinite(Number(box.precision))
            ? Math.min(Math.max(Number(box.precision), 2), 8)
            : undefined;

          summaryEntries.push({
            index: baseIndex + idxInGroup + 1,
            startSec: x1,
            endSec: x2,
            requestedEndSec: requestedX2,
            val: Number.isFinite(y1) ? y1 : null,
            vah: Number.isFinite(y2) ? y2 : null,
            poc: pocValue,
            sessions,
            valueAreaId,
            label,
            sourceStart,
            sourceEnd,
            precision,
          });

          return {
            x1,
            x2,
            y1,
            y2,
            color: box.color,
            border: box.border,
            precision: box.precision,
          };
        }).filter(Boolean);
        boxes.push(...normalizedBoxes);
        normalizedBoxes.forEach((b, idx) => {
          const width = Number.isFinite(b.x2) && Number.isFinite(b.x1)
            ? Number(b.x2) - Number(b.x1)
            : null;
          overlayLogger.debug('va_box_applied', {
            boxIndex: baseIndex + idx,
            x1: b.x1,
            x2: b.x2,
            y1: b.y1,
            y2: b.y2,
            width,
          });
        });

        if (summaryEntries.length) {
          overlayLogger.info('va_box_summary', {
            appended: summaryEntries.length,
            total: boxes.length,
          });
          summaryEntries.forEach((entry) => {
            overlayLogger.info('va_box_summary_entry', {
              index: entry.index,
              detail: buildVaBoxSummaryText(entry),
              valueAreaId: entry.valueAreaId ?? null,
              label: entry.label ?? null,
              sourceStart: entry.sourceStart ?? null,
              sourceEnd: entry.sourceEnd ?? null,
            });
          });
        }
      }

      if (paneViews.includes('segment') && norm.segments?.length) {
        allSegments.push(...norm.segments);
      }
      if (paneViews.includes('polyline') && norm.polylines?.length) {
        allPolylines.push(...norm.polylines);
      }
    }

    // Group touch points by time, strictly 1 item per time.
    const grouped = new Map();
    for (const p of touchPoints) {
      if (p.time == null || Number.isNaN(p.time)) continue;
      if (!grouped.has(p.time)) grouped.set(p.time, []);
      grouped.get(p.time).push({
        price:  p.originalData?.price ?? p.price,
        color:  p.originalData?.color ?? p.color,
        size:   (p.originalData?.size ?? p.size ?? 3),
      });
    }
    
    // 4) Sort markers for deterministic rendering.
    markers.sort((a, b) => a.time - b.time);

    // 5) Apply markers to the main series.
    try {
      // seriesRef.current.setMarkers(markers);
      overlayHandlesRef.current.markersApi.setMarkers(markers);
      

      pvMgrRef.current?.setTouchPoints(touchPoints);
      pvMgrRef.current?.setVABlocks(boxes, {
        lastSeriesTime: lastBarRef.current?.time,
        barSpacing: barSpacingRef.current,
      });
      pvMgrRef.current?.setSegments(allSegments);
      pvMgrRef.current?.setPolylines(allPolylines);
      pvMgrRef.current?.setSignalBubbles(signalBubbles);

      // --- C: VWAP vs Candles coverage + coordinate check ---
      // seriesRef.current.setData(touch)
    } catch (e) {
      error('overlays_apply_failed', e);
    }

    // 6) Log summary for quick tracing.
    info('overlays_applied', {
      priceLines: overlayHandlesRef.current.priceLines.length,
      markers: markers.length,
      touchPoints: touchPoints.length,
      boxes: boxes.length,
      bubbles: signalBubbles.length,
      segments: allSegments.length,
      polylines: allPolylines.length,
    });

    setDataLoading(false);
  }, [info, error, logger]);

  // React to overlay changes.
  useEffect(() => {
    if (!chartState) return;
    syncOverlays(chartState.overlays || []);
  }, [chartState, syncOverlays]);

  // Apply handler.
  const handleApply = useCallback((overrides = {}) => {
    const nextSymbol = overrides.symbol ?? symbol;
    const nextInterval = overrides.interval ?? interval;
    const nextRange = overrides.dateRange ?? dateRange;
    const nextDatasource = overrides.datasource ?? datasource;
    const nextExchange = overrides.exchange ?? exchange;
    const [start, end] = nextRange || [];
    const maxWindowMs = 90 * 24 * 60 * 60 * 1000;
    const windowMs = start && end ? Math.abs(end.getTime() - start.getTime()) : 0;
    if (windowMs > maxWindowMs) {
      warn('apply_blocked_range', { chartId, symbol: nextSymbol, interval: nextInterval, windowMs });
      showWarning('Please choose a window of 90 days or less before applying.');
      return;
    }

    if (nextDatasource === 'CCXT' && !nextExchange) {
      warn('apply_missing_exchange', { chartId, symbol: nextSymbol });
      showWarning('Select a crypto exchange before loading data.');
      return;
    }

    setRangeWarning(null);
    info('apply', {
      chartId,
      symbol: nextSymbol,
      interval: nextInterval,
      dateRange: nextRange,
      datasource: nextDatasource,
      exchange: nextExchange,
    });
    syncOverlays([]); // clear overlays on apply
    updateChart?.(chartId, {
      symbol: nextSymbol,
      interval: nextInterval,
      dateRange: nextRange,
      datasource: nextDatasource,
      exchange: nextExchange || null,
    });
    loadChartData({
      targetSymbol: nextSymbol,
      targetInterval: nextInterval,
      targetRange: nextRange,
      targetDatasource: nextDatasource,
      targetExchange: nextExchange,
    });
    bumpRefresh?.(chartId);
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, dateRange, datasource, exchange, warn, syncOverlays, showWarning]);

  function useBusyDelay(busy, ms=250){
    const [show,setShow]=useState(false);
    useEffect(()=>{
      if(busy){ const t=setTimeout(()=>setShow(true), ms); return ()=>clearTimeout(t); }
      setShow(false);
    },[busy,ms]);
    return show;
  }

  const loaderActive = useBusyDelay(chartState?.overlayLoading || chartState?.signalsLoading || dataLoading);
  const loaderMessage = chartState?.signalsLoading ? 'Generating signals…'
    : chartState?.overlayLoading ? 'Loading overlays…'
      : mode === 'live' ? 'Streaming latest data…'
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

  return (
    <div className="space-y-5">
      {connectionNotice && (
        <div className="flex items-start gap-3 rounded-2xl border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-100 shadow-lg shadow-rose-900/40">
          <span className="mt-0.5 text-lg">⚠️</span>
          <div>
            <p className="font-semibold tracking-tight">Connection issue</p>
            <p className="text-xs text-rose-100/80">{connectionNotice}</p>
          </div>
        </div>
      )}

      {rangeWarning && (
        <div className="flex items-center gap-2 rounded-2xl border border-amber-400/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100 shadow-lg shadow-amber-900/30">
          <span className="text-lg">⚠️</span>
          <span className="font-medium">{rangeWarning}</span>
        </div>
      )}

      <div className="rounded-3xl border border-white/8 bg-[#1b1d26]/85 p-6 shadow-[0_50px_140px_-80px_rgba(0,0,0,0.85)]">
        <div className="flex flex-col gap-5 md:flex-row md:flex-wrap md:items-end md:justify-between">
          <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-end">
            <div className="flex min-w-[13rem] flex-col gap-2">
              <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Datasource</span>
              <div className="inline-flex rounded-lg border border-slate-600/60 bg-slate-900/60 p-1">
                {DATASOURCE_OPTIONS.map((option) => {
                  const isCryptoOption = option.value === DATASOURCE_IDS.CCXT;
                  const isActive = isCryptoOption
                    ? datasource === DATASOURCE_IDS.CCXT
                    : datasource !== DATASOURCE_IDS.CCXT;
                  return (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => handleDatasourceChange(option.value)}
                      className={`min-w-[5.5rem] rounded-md px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.25em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${isActive ? 'bg-[color:var(--accent-alpha-30)] text-[color:var(--accent-text-strong)] shadow-inner' : 'text-slate-300 hover:bg-[color:var(--accent-alpha-15)] hover:text-[color:var(--accent-text-soft)]'}`}
                    >
                      {option.label}
                    </button>
                  );
                })}
              </div>
            </div>

            <DropdownSelect
              className="min-w-[15rem]"
              label="Exchange"
              value={selectedExchangeValue}
              onChange={handleExchangeChange}
              options={exchangeSelectOptions}
              placeholder={exchangePlaceholder}
            />

            {marketProvider === 'ibkr' ? (
              <DropdownSelect
                className="min-w-[14rem]"
                label="IB Venue"
                value={exchange || DEFAULT_IB_EXCHANGE}
                onChange={handleIbVenueChange}
                options={IB_EXCHANGES.map((entry) => ({
                  value: entry.value,
                  label: entry.label,
                  description: entry.description,
                }))}
                placeholder="Select venue"
              />
            ) : null}

            <DataModeToggle
              mode={mode}
              onChange={setMode}
              supportsLive={supportsLive}
              disabledReason={liveDisabledReason}
            />

            <TimeframeSelect selected={interval} onChange={setInterval} />
            <SymbolInput value={symbol} onChange={setSymbol} />
            <div className="flex items-end gap-2">
              <DateRangePickerComponent
                dateRange={dateRange}
                setDateRange={setDateRange}
                disabled={mode === 'live'}
              />
              <button
                type="button"
                onClick={() => handleApply()}
                className="mb-[6px] inline-flex h-9 w-9 items-center justify-center rounded-full border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-10)] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-20)] hover:text-[color:var(--accent-text-bright)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
                aria-label="Reload chart data"
                title="Reload chart data"
              >
                <RotateCcw className="size-4" />
              </button>
            </div>
          </div>
        </div>

        <div className="mt-4 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-xs text-slate-400">{lastRefreshCopy}</p>
          {connectionStatus === 'error' && connectionMessage ? (
            <p className={`text-xs ${statusTextClass} sm:text-right`}>{connectionMessage}</p>
          ) : null}
        </div>

        <div className="relative mt-6 h-[700px] overflow-hidden rounded-2xl border border-white/10 bg-gradient-to-b from-[#222430] to-[#151720]">
          <div className="pointer-events-none absolute right-5 top-5 inline-flex items-center gap-2 rounded-full border border-white/10 bg-black/40 px-3 py-1 text-[11px] uppercase tracking-[0.32em] text-slate-200 shadow-lg shadow-black/30">
            Press <kbd className="rounded border border-white/20 bg-black/70 px-1 text-[10px] text-slate-100">/</kbd> presets
          </div>
          <div ref={chartContainerRef} className="h-full w-full" />

          <SymbolPalette open={palOpen} onClose={() => setPalOpen(false)} onPick={applySymbol} />
          <HotkeyHint />
          <LoadingOverlay show={loaderActive} message={loaderMessage} />
        </div>
      </div>
    </div>
  )
};
