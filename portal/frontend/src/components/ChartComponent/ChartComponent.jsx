import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { createChart, CandlestickSeries, createSeriesMarkers } from 'lightweight-charts';
import { RotateCcw, Maximize2, Minimize2 } from 'lucide-react';
import { TimeframeSelect, SymbolInput } from './TimeframeSelectComponent';
import { DateRangePickerComponent } from './DateTimePickerComponent.jsx';
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
import { HistoricalLookbackControl } from './LookbackControls.jsx';
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
const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_LOOKBACK_DAYS = 365;
const DEFAULT_LOOKBACK_DAYS = 90;
const LIVE_CRYPTO_EXCHANGES = new Set(['binanceus']);
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
  const [symbolDraft, setSymbolDraft] = useState('CL');
  const [interval, setInterval] = useState('15m');
  const [datasource, setDatasource] = useState(DEFAULT_DATASOURCE);
  const [exchange, setExchange] = useState(DEFAULT_MARKET_PROVIDER);
  const [marketProvider, setMarketProvider] = useState(DEFAULT_MARKET_PROVIDER);
  const [palOpen, setPalOpen] = useState(false);
  const [dateRange, setDateRange] = useState([
    new Date(Date.now() - DEFAULT_LOOKBACK_DAYS * DAY_MS),
    new Date(),
  ]);
  const [historicalWindowMode, setHistoricalWindowMode] = useState(
    HISTORICAL_WINDOW_MODES.LOOKBACK,
  );
  const [historicalLookbackDays, setHistoricalLookbackDays] = useState(DEFAULT_LOOKBACK_DAYS);
  const [historicalLookbackInput, setHistoricalLookbackInput] = useState(
    String(DEFAULT_LOOKBACK_DAYS),
  );
  const [liveLookbackDays, setLiveLookbackDays] = useState(DEFAULT_LOOKBACK_DAYS);
  const [liveLookbackInput, setLiveLookbackInput] = useState(String(DEFAULT_LOOKBACK_DAYS));
  const [dataLoading, setDataLoading] = useState(false);
  const [dataLoaderContext, setDataLoaderContext] = useState(null);
  const [rangeWarning, setRangeWarning] = useState(null);
  const [connectionNotice, setConnectionNotice] = useState(null);
  const [lastRefreshAt, setLastRefreshAt] = useState(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [fullscreenHost, setFullscreenHost] = useState(null);

  const chartShellClasses = useMemo(() => {
    const base =
      'relative overflow-hidden border border-white/12 bg-gradient-to-b from-[#1d2336] via-[#111827] to-[#070b14] shadow-[0_50px_160px_-90px_rgba(0,0,0,0.85)]';
    const size = isFullscreen
      ? 'h-full w-full rounded-none'
      : 'h-[700px] rounded-[28px]';
    return `${base} ${size}`;
  }, [isFullscreen]);

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

  useEffect(() => {
    const normalizedHistorical = String(clampLookbackDays(historicalLookbackDays));
    setHistoricalLookbackInput((prev) => (prev === normalizedHistorical ? prev : normalizedHistorical));

    const normalized = String(clampLookbackDays(liveLookbackDays));
    setLiveLookbackInput((prev) => (prev === normalized ? prev : normalized));
  }, [historicalLookbackDays, liveLookbackDays]);

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
    exchange: null,
  });
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

  const toggleFullscreen = useCallback(() => {
    setIsFullscreen((prev) => !prev);
  }, [setIsFullscreen]);

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

  const normalizedExchange = useMemo(() => normalizeExchangeId(exchange), [exchange]);
  const supportsLive = useMemo(() => {
    if (datasource === DATASOURCE_IDS.IBKR) {
      return true;
    }
    if (datasource === DATASOURCE_IDS.CCXT) {
      const fallback = normalizeExchangeId(lastCryptoExchangeRef.current || DEFAULT_CRYPTO_EXCHANGE);
      const candidate = normalizedExchange || fallback;
      return LIVE_CRYPTO_EXCHANGES.has(candidate);
    }
    return false;
  }, [datasource, normalizedExchange]);

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

    // Overlay resource handles.
  const overlayHandlesRef = useRef({ priceLines: [] });

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
    behavior = 'auto',
    loaderReason,
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

    const previousLastBar = lastBarRef.current;
    const canStreamAppend =
      behavior === 'append'
        || (behavior === 'auto' && modeRef.current === 'live' && Boolean(previousLastBar));
    const hasStreamingBaseline = canStreamAppend && previousLastBar?.time != null;

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
        warn('no data', { symbol: effectiveSymbol, interval: effectiveInterval, behavior });
        markSuccess();
        if (!canStreamAppend) {
          showWarning('No candles found for the selected window. Try a different symbol, range, or datasource.');
        } else {
          debug('live_refresh_empty_batch', { startISO, endISO });
        }
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
        exchange: effectiveExchange,
        lastUpdatedAt: refreshAt.toISOString(),
      });

      activeSeriesKeyRef.current = {
        symbol: effectiveSymbol,
        interval: effectiveInterval,
        datasource: effectiveDatasource,
        exchange: effectiveExchange,
      };

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
      return { ok: false, reason: 'error' };
    } finally {
      dataLoadingRef.current = false;
      if (loaderContext) {
        setDataLoading(false);
        setDataLoaderContext(null);
      }
    }
  }, [info, warn, error, markAttempt, markSuccess, markError, updateChart, chartId, showWarning, debug, setLastRefreshAt]);

  const refreshLive = useCallback(async () => {
    if (!supportsLive) {
      return false;
    }

    if (dataLoadingRef.current) {
      debug('live_refresh_skipped_busy');
      return false;
    }

    const now = new Date();
    const lookbackMs = clampLookbackDays(liveLookbackDays) * DAY_MS;
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
  }, [supportsLive, loadChartData, debug, bumpRefresh, chartId, liveLookbackDays]);

  const { mode, setMode } = useLiveDataMode({ supportsLive, onRefresh: refreshLive, logger });

  useEffect(() => {
    modeRef.current = mode;
  }, [mode]);

  useEffect(() => {
    if (mode === 'live') {
      return;
    }
    if (historicalWindowMode !== HISTORICAL_WINDOW_MODES.LOOKBACK) {
      return;
    }
    const now = new Date();
    const normalized = clampLookbackDays(historicalLookbackDays);
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    dateRangeRef.current = nextRange;
    setDateRange(nextRange);
  }, [mode, historicalLookbackDays, historicalWindowMode]);

  useEffect(() => {
    if (mode !== 'live') {
      return;
    }
    const now = new Date();
    const normalized = clampLookbackDays(liveLookbackDays);
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    dateRangeRef.current = nextRange;
    setDateRange(nextRange);
  }, [mode, liveLookbackDays]);

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

  const handleHistoricalLookbackChange = useCallback((days) => {
    const normalized = clampLookbackDays(days);
    setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.LOOKBACK);
    setHistoricalLookbackDays(normalized);
    setHistoricalLookbackInput(String(normalized));
  }, []);

  const handleHistoricalLookbackInputChange = useCallback((event) => {
    const raw = event?.target?.value ?? '';
    const sanitized = raw.replace(/[^0-9]/g, '');
    setHistoricalLookbackInput(sanitized);
  }, []);

  const handleHistoricalLookbackCommit = useCallback(() => {
    const parsed = Number.parseInt(historicalLookbackInput, 10);
    const normalized = clampLookbackDays(
      Number.isNaN(parsed) ? historicalLookbackDays : parsed,
    );
    if (normalized !== historicalLookbackDays) {
      handleHistoricalLookbackChange(normalized);
    } else {
      setHistoricalLookbackInput(String(normalized));
    }
  }, [handleHistoricalLookbackChange, historicalLookbackInput, historicalLookbackDays]);

  const handleLiveLookbackInputChange = useCallback((event) => {
    const raw = event?.target?.value ?? '';
    const sanitized = raw.replace(/[^0-9]/g, '');
    setLiveLookbackInput(sanitized);
  }, []);

  const handleDateRangeSelection = useCallback((nextRange) => {
    if (!Array.isArray(nextRange)) return;
    const normalized = nextRange.map((value) => {
      if (value instanceof Date) return value;
      if (!value) return value;
      const converted = new Date(value);
      return Number.isNaN(converted.getTime()) ? null : converted;
    });
    setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.RANGE);
    dateRangeRef.current = normalized;
    setDateRange(normalized);
  }, []);

  const handleHistoricalModeToggle = useCallback((nextMode) => {
    if (nextMode === HISTORICAL_WINDOW_MODES.RANGE) {
      setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.RANGE);
      return;
    }
    setHistoricalWindowMode(HISTORICAL_WINDOW_MODES.LOOKBACK);
  }, []);

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
  const handleApply = useCallback(async (overrides = {}, options = {}) => {
    const nextSymbol = overrides.symbol ?? symbol;
    const nextInterval = overrides.interval ?? interval;
    const fallbackRange = modeRef.current === 'live' ? dateRangeRef.current : dateRange;
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
      exchange: nextExchange,
    });
    const prevKey = activeSeriesKeyRef.current;
    const symbolChanged = prevKey.symbol !== nextSymbol;
    const isSeriesChange =
      symbolChanged
      || prevKey.interval !== nextInterval
      || prevKey.datasource !== nextDatasource
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
    syncOverlays([]); // clear overlays on apply
    updateChart?.(chartId, {
      symbol: nextSymbol,
      interval: nextInterval,
      dateRange: effectiveRange,
      datasource: nextDatasource,
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
      targetExchange: nextExchange,
      behavior,
      loaderReason: symbolChanged ? 'symbol-change' : undefined,
    });

    if (result?.ok && (result.replaced || result.appended)) {
      bumpRefresh?.(chartId);
    }

    return result;
  }, [info, loadChartData, updateChart, bumpRefresh, chartId, symbol, interval, dateRange, datasource, exchange, warn, syncOverlays, showWarning]);

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
    const parsed = Number.parseInt(liveLookbackInput, 10);
    const normalized = clampLookbackDays(Number.isNaN(parsed) ? liveLookbackDays : parsed);
    const changed = normalized !== liveLookbackDays;

    setLiveLookbackDays(normalized);
    setLiveLookbackInput(String(normalized));

    const now = new Date();
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    dateRangeRef.current = nextRange;
    setDateRange(nextRange);

    if (modeRef.current === 'live' && supportsLive && changed) {
      void handleApply({ dateRange: nextRange }, { behavior: 'replace' });
    }
  }, [liveLookbackInput, liveLookbackDays, supportsLive, handleApply]);

  const handleLiveLookbackPresetSelect = useCallback((days) => {
    const normalized = clampLookbackDays(days);
    setLiveLookbackDays(normalized);
    setLiveLookbackInput(String(normalized));

    const now = new Date();
    const start = new Date(now.getTime() - normalized * DAY_MS);
    const nextRange = [start, now];
    dateRangeRef.current = nextRange;
    setDateRange(nextRange);

    if (modeRef.current === 'live' && supportsLive) {
      void handleApply({ dateRange: nextRange }, { behavior: 'replace' });
    }
  }, [supportsLive, handleApply]);

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
      : dateRange;

    void handleApply({
      symbol,
      interval,
      datasource,
      exchange,
      dateRange: liveRange,
    }, { behavior: 'replace' });
  }, [mode, symbol, interval, datasource, exchange, handleApply, dateRange, supportsLive]);

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

    setDateRange((prev) => {
      const prevStart = prev?.[0] instanceof Date ? prev[0].getTime() : null;
      const prevEnd = prev?.[1] instanceof Date ? prev[1].getTime() : null;
      if (prevStart === start.getTime() && prevEnd === end.getTime()) {
        return prev;
      }
      return [start, end];
    });
  }, [mode, setDateRange]);

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

  const isLookbackMode = historicalWindowMode === HISTORICAL_WINDOW_MODES.LOOKBACK;
  const isRangeMode = historicalWindowMode === HISTORICAL_WINDOW_MODES.RANGE;
  const liveMode = mode === 'live';
  const symbolDisplay = (symbol || '—').toString().toUpperCase();
  const intervalDisplay = (interval ? interval.toString() : '—').toUpperCase();
  const datasourceDisplay = useMemo(() => {
    const map = {
      [DATASOURCE_IDS.ALPACA]: 'Markets data',
      [DATASOURCE_IDS.YFINANCE]: 'Yahoo Finance',
      [DATASOURCE_IDS.IBKR]: 'Interactive Brokers',
      [DATASOURCE_IDS.CCXT]: 'Crypto data',
    };
    return map[datasource] || 'Markets data';
  }, [datasource]);

  const venueDisplay = useMemo(() => {
    if (datasource === DATASOURCE_IDS.CCXT) {
      const entry = CRYPTO_EXCHANGES.find((ex) => ex.value === exchange);
      if (entry?.label) {
        return entry.category ? `${entry.label} (${entry.category})` : entry.label;
      }
      return 'Crypto venue';
    }

    if (datasource === DATASOURCE_IDS.IBKR) {
      const entry = IB_EXCHANGES.find((ex) => ex.value === exchange);
      return entry?.label || exchange || 'IBKR routing';
    }

    if (datasource === DATASOURCE_IDS.YFINANCE) {
      return 'Yahoo Finance';
    }

    const providerEntry = MARKET_PROVIDERS.find((provider) => provider.value === marketProvider);
    if (providerEntry?.label) {
      return providerEntry.label;
    }

    if (typeof exchange === 'string' && exchange.trim()) {
      return exchange.trim().toUpperCase();
    }

    return null;
  }, [datasource, exchange, marketProvider]);

  const instrumentMeta = useMemo(() => {
    const parts = [datasourceDisplay, venueDisplay].filter(Boolean);
    return parts.join(' • ');
  }, [datasourceDisplay, venueDisplay]);

  const chartSurface = (
    <div className={chartShellClasses}>
      <div className="pointer-events-none absolute left-6 top-6 z-20 flex max-w-[70%] flex-col gap-1.5 text-slate-200 drop-shadow-[0_10px_30px_rgba(0,0,0,0.65)]">
        <div className="flex flex-wrap items-baseline gap-2">
          <span className="text-2xl font-semibold tracking-tight text-white">{symbolDisplay}</span>
          <span className="rounded-full border border-white/20 bg-black/60 px-3 py-0.5 text-[11px] font-semibold uppercase tracking-[0.35em] text-slate-100">
            {intervalDisplay}
          </span>
        </div>
        {instrumentMeta ? (
          <div className="text-[11px] font-semibold uppercase tracking-[0.32em] text-slate-300/90">
            {instrumentMeta}
          </div>
        ) : null}
      </div>
      <button
        type="button"
        aria-pressed={isFullscreen}
        onClick={toggleFullscreen}
        className="pointer-events-auto absolute right-6 top-6 z-30 inline-flex items-center gap-2 rounded-full border border-white/20 bg-black/60 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.25em] text-slate-100 shadow-lg shadow-black/30 transition hover:border-white/40 hover:bg-black/80 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/70"
      >
        {isFullscreen ? (
          <>
            <Minimize2 className="h-3.5 w-3.5" aria-hidden="true" />
            Exit Fullscreen
          </>
        ) : (
          <>
            <Maximize2 className="h-3.5 w-3.5" aria-hidden="true" />
            Fullscreen
          </>
        )}
      </button>
      <div ref={attachChartContainerRef} className="h-full w-full" />

      <SymbolPalette open={palOpen} onClose={() => setPalOpen(false)} onPick={applySymbol} />
      <HotkeyHint />
      <LoadingOverlay show={loaderActive} message={loaderMessage} />
    </div>
  );

  const renderedChartSurface = isFullscreen && fullscreenHost
    ? createPortal(chartSurface, fullscreenHost)
    : chartSurface;

  return (
    <>
      <div className="space-y-6">
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

        <section className="rounded-[28px] border border-white/8 bg-gradient-to-br from-[#080b14]/95 via-[#070a13]/95 to-[#04060c]/95 p-6 shadow-[0_50px_150px_-90px_rgba(0,0,0,0.85)]">
          <div className="flex flex-col gap-6">
            <header className="flex flex-col gap-2.5 md:flex-row md:items-center md:justify-between">
              <div>
                <h2 className="text-base font-semibold tracking-tight text-slate-100">Workspace controls</h2>
                <p className="text-sm text-slate-400">Set up your instrument, venue, and data horizon.</p>
              </div>
              <button
                type="button"
                onClick={() => { void handleApply(); }}
                className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-[color:var(--accent-alpha-40)] bg-[color:var(--accent-alpha-15)] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-60)] hover:bg-[color:var(--accent-alpha-25)] hover:text-[color:var(--accent-text-bright)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)]"
                aria-label="Reload chart data"
                title="Reload chart data"
              >
                <RotateCcw className="size-4" />
              </button>
            </header>

            <div className="grid gap-5 xl:grid-cols-[minmax(0,1.75fr)_minmax(0,1.1fr)]">
              <div className="space-y-5">
                <div className="rounded-2xl border border-white/12 bg-[#0b1324]/60 p-4 shadow-lg shadow-black/25">
                  <div className="flex items-start justify-between gap-2.5">
                    <div>
                      <span className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400/80">Instrument</span>
                      <p className="text-sm text-slate-400">Choose the asset, timeframe, and streaming mode.</p>
                    </div>
                    <span className="hidden text-[11px] uppercase tracking-[0.3em] text-slate-500/80 sm:block">
                      {symbolDisplay} · {intervalDisplay || '—'}
                    </span>
                  </div>
                  <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                    <SymbolInput
                      value={symbolDraft}
                      onChange={handleSymbolInputChange}
                      onCommit={handleSymbolInputCommit}
                      onRequestPick={() => setPalOpen(true)}
                      className="md:col-span-2 xl:col-span-1"
                    />
                    <TimeframeSelect selected={interval} onChange={setInterval} className="xl:col-span-1" />
                    <DataModeToggle
                      mode={mode}
                      onChange={setMode}
                      supportsLive={supportsLive}
                      disabledReason={liveDisabledReason}
                      liveDescription={liveDescription}
                      className="md:col-span-2 xl:col-span-1"
                    />
                  </div>
                </div>
              </div>

              <div className="space-y-5">
                <div className="rounded-2xl border border-white/12 bg-[#0b1324]/60 p-4 shadow-lg shadow-black/25">
                  <span className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400/80">Market access</span>
                  <p className="mt-1 text-sm text-slate-400">
                    Toggle between exchanges or providers to route historical and live data.
                  </p>
                  <div className="mt-3 flex flex-col gap-3.5">
                    <div className="inline-flex flex-wrap gap-1.5 rounded-xl border border-white/10 bg-[#050912]/80 p-1">
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
                            className={`min-w-[5.5rem] rounded-lg px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.28em] transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[color:var(--accent-outline)] ${
                              isActive
                                ? 'bg-[color:var(--accent-alpha-28)] text-[color:var(--accent-text-strong)] shadow-inner'
                                : 'text-slate-300 hover:bg-[#111d34] hover:text-[color:var(--accent-text-soft)]'
                            }`}
                          >
                            {option.label}
                          </button>
                        );
                      })}
                    </div>

                    <DropdownSelect
                      className="w-full rounded-2xl border border-white/12 bg-[#050912]/80 p-3.5 shadow-inner shadow-black/10"
                      label="Exchange"
                      value={selectedExchangeValue}
                      onChange={handleExchangeChange}
                      options={exchangeSelectOptions}
                      placeholder={exchangePlaceholder}
                    />

                    {marketProvider === 'ibkr' ? (
                      <DropdownSelect
                        className="w-full rounded-2xl border border-white/12 bg-[#050912]/80 p-3.5 shadow-inner shadow-black/10"
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
                  </div>
                </div>
              </div>
            </div>

            <div className="rounded-2xl border border-white/12 bg-[#0b1324]/60 p-5 shadow-lg shadow-black/25">
              <div className="flex flex-wrap items-center gap-2.5">
                <div>
                  <span className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400/80">Data window</span>
                  <p className="text-sm text-slate-400">Control how much history to load for studies.</p>
                </div>

                {!liveMode ? (
                  <div className="ml-auto inline-flex items-center gap-1.5 rounded-full border border-white/12 bg-[#050912]/80 p-1">
                    <button
                      type="button"
                      onClick={() => handleHistoricalModeToggle(HISTORICAL_WINDOW_MODES.RANGE)}
                      className={`rounded-full px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.32em] transition ${
                        isRangeMode
                          ? 'bg-[color:var(--accent-alpha-28)] text-[color:var(--accent-text-strong)] shadow-inner'
                          : 'text-slate-300 hover:bg-[#111d34] hover:text-[color:var(--accent-text-soft)]'
                      }`}
                    >
                      Calendar range
                    </button>
                    <button
                      type="button"
                      onClick={() => handleHistoricalModeToggle(HISTORICAL_WINDOW_MODES.LOOKBACK)}
                      className={`rounded-full px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.32em] transition ${
                        isLookbackMode
                          ? 'bg-[color:var(--accent-alpha-28)] text-[color:var(--accent-text-strong)] shadow-inner'
                          : 'text-slate-300 hover:bg-[#111d34] hover:text-[color:var(--accent-text-soft)]'
                      }`}
                    >
                      Days back
                    </button>
                  </div>
                ) : (
                  <span className="ml-auto text-[11px] uppercase tracking-[0.32em] text-slate-400">Live streaming</span>
                )}
              </div>

              <div className="mt-5 grid gap-4 lg:grid-cols-2">
                <div
                  className={`rounded-2xl border border-white/12 bg-[#050912]/80 p-4 transition ${
                    isRangeMode && !liveMode
                      ? 'ring-1 ring-[color:var(--accent-ring-strong)]'
                      : 'opacity-65'
                  } ${
                    liveMode
                      ? 'cursor-not-allowed'
                      : 'cursor-pointer hover:border-[color:var(--accent-alpha-30)] hover:opacity-95'
                  }`}
                  onClick={() => {
                    if (!isRangeMode && !liveMode) {
                      handleHistoricalModeToggle(HISTORICAL_WINDOW_MODES.RANGE);
                    }
                  }}
                >
                  <DateRangePickerComponent
                    dateRange={dateRange}
                    setDateRange={handleDateRangeSelection}
                    disabled={liveMode || !isRangeMode}
                  />
                </div>

                <HistoricalLookbackControl
                  value={liveMode ? liveLookbackDays : historicalLookbackDays}
                  onSelect={liveMode ? handleLiveLookbackPresetSelect : handleHistoricalLookbackChange}
                  maxDays={MAX_LOOKBACK_DAYS}
                  active={liveMode ? true : isLookbackMode}
                  onActivate={liveMode ? undefined : handleHistoricalModeToggle}
                  inputValue={liveMode ? liveLookbackInput : historicalLookbackInput}
                  onInputChange={liveMode ? handleLiveLookbackInputChange : handleHistoricalLookbackInputChange}
                  onInputCommit={liveMode ? handleLiveLookbackCommit : handleHistoricalLookbackCommit}
                  title={liveMode ? 'Live window' : 'Days back'}
                  subtitle={
                    liveMode
                      ? 'Stream real-time candles with a trailing history buffer'
                      : 'Rolling lookback presets'
                  }
                  footnote={
                    liveMode
                      ? `Streaming last ${clampLookbackDays(liveLookbackDays)} days`
                      : undefined
                  }
                />
              </div>
            </div>
          </div>
        </section>

        <div className="flex flex-col gap-2 text-xs text-slate-400/80 sm:flex-row sm:items-center sm:justify-between">
          <p>{lastRefreshCopy}</p>
          {connectionStatus === 'error' && connectionMessage ? (
            <p className={`${statusTextClass} sm:text-right`}>{connectionMessage}</p>
          ) : null}
        </div>

        {renderedChartSurface}
      </div>
    </>
  )
};
