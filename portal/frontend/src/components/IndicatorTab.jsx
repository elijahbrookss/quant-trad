import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import { Plus, X, RefreshCw } from 'lucide-react'
import {
  fetchIndicators,
  createIndicator,
  updateIndicator,
  deleteIndicator,
  setIndicatorEnabled,
  bulkToggleIndicators,
  bulkDeleteIndicators,
  duplicateIndicator,
  fetchIndicatorOverlays,
  generateIndicatorSignals,
} from '../adapters/indicator.adapter'
import { applyIndicatorColors, runSignalGeneration } from './indicatorSignals.js'
// import IndicatorModal from './IndicatorModal'
import IndicatorModalV2 from './IndicatorModal.v2.jsx'
const IndicatorModal = IndicatorModalV2; // for now, swap in new version under old name
import { useChartState } from '../contexts/ChartStateContext'
import IndicatorCard from './IndicatorCard.jsx';
import DeleteIndicatorModal from './DeleteIndicatorModal.jsx';
import DropdownSelect from './ChartComponent/DropdownSelect.jsx';
import { createLogger } from '../utils/logger.js';
import LoadingOverlay from './LoadingOverlay.jsx';


// Gold, Maroon, Orange, Purple, Lime, Gray
const COLOR_SWATCHES = [
  '#facc15', '#b91c1c', '#f97316', '#a855f7', '#84cc16', '#6b7280',
  '#3b82f6', '#10b981', '#ec4899', '#14b8a6', '#eab308', '#f43f5e'
];

const DEFAULT_INDICATOR_COLOR = '#60a5fa';
const DEFAULT_PAGE_SIZE = 6;
const PAGE_SIZE_OPTIONS = [6, 12, 24, 48];
const BUSY_MESSAGE = 'Chart is computing indicators—please wait.';

const buildColorMap = (list = []) => {
  if (!Array.isArray(list)) return {};
  return list.reduce((acc, indicator) => {
    if (!indicator?.id) return acc;
    const raw = typeof indicator?.color === 'string' ? indicator.color.trim() : '';
    acc[indicator.id] = raw || DEFAULT_INDICATOR_COLOR;
    return acc;
  }, {});
};

const shallowEqualMap = (a = {}, b = {}) => {
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (const key of keysA) {
    if (a[key] !== b[key]) return false;
  }
  return true;
};

const formatIndicatorType = (type) => {
  if (!type) return 'Custom';
  return type
    .split(/[_-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');
};

const toInt = (v) => {
  if (typeof v === 'number') return Math.trunc(v);
  if (typeof v === 'string') {
    const n = Number(v.trim());
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }
  return null;
};

const toIntList = (v) => {
  if (Array.isArray(v)) return v.map(toInt).filter((n) => n !== null);
  if (typeof v === 'string') {
    const tokens = v.split(/[\s,;]+/).filter(Boolean);
    return tokens.map(toInt).filter((n) => n !== null);
  }
  if (v == null) return [];
  const n = toInt(v);
  return n !== null ? [n] : [];
};

// normalize known params (add more keys here if needed)
const normalizeParams = (params) => {
  const p = { ...params };
  if (p.lookbacks !== undefined) p.lookbacks = toIntList(p.lookbacks);
  return p;
};

const stripRuntimeParams = (params) => {
  const cleaned = { ...params };
  const runtimeKeys = [
    'symbol',
    'interval',
    'timeframe',
    'start',
    'end',
    'datasource',
    'exchange',
    'provider_id',
    'venue_id',
    'instrument_id',
    'bot_id',
    'strategy_id',
    'bot_mode',
    'run_id',
  ];
  for (const key of runtimeKeys) {
    if (Object.prototype.hasOwnProperty.call(cleaned, key)) {
      delete cleaned[key];
    }
  }
  return cleaned;
};

const parseTimestamp = (value) => {
  if (!value) return 0;
  const ts = Date.parse(value);
  return Number.isNaN(ts) ? 0 : ts;
};

const sortIndicators = (list = []) => {
  return [...list].sort((a, b) => {
    const enabledDelta = Number(Boolean(b?.enabled)) - Number(Boolean(a?.enabled));
    if (enabledDelta !== 0) return enabledDelta;
    const createdDelta = parseTimestamp(b?.created_at) - parseTimestamp(a?.created_at);
    if (createdDelta !== 0) return createdDelta;
    return parseTimestamp(b?.updated_at) - parseTimestamp(a?.updated_at);
  });
};

// Manages the list of indicators and syncs enabled ones to the chart context
export const IndicatorSection = ({ chartId }) => {
  const [indicators, setIndicators] = useState([])
  const indicatorsRef = useRef(indicators)
  useEffect(() => {
    indicatorsRef.current = indicators
  }, [indicators])
  const [modalOpen, setModalOpen] = useState(false)
  const [editing, setEditing] = useState(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState(null)
  const [indColors, setIndColors] = useState({});
  const [showEnabledOnly, setShowEnabledOnly] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [typeFilter, setTypeFilter] = useState('all');
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [selectedIds, setSelectedIds] = useState(() => new Set());
  const [bulkActionLoading, setBulkActionLoading] = useState(false);
  const [duplicateBusyId, setDuplicateBusyId] = useState(null);
  const [refreshingList, setRefreshingList] = useState(false);
  const [jobState, setJobState] = useState({ busy: false, indicatorId: null, type: null, label: '' });
  const [notice, setNotice] = useState('');
  const noticeTimerRef = useRef(null);
  const [deleteModal, setDeleteModal] = useState({ open: false, indicatorId: null, indicatorName: '' });


  const { updateChart, getChart } = useChartState()

  const logger = useMemo(() => createLogger('IndicatorSection', { chartId }), [chartId])
  const { debug, info, warn, error: logError } = logger

  // Read current chart slice
  const chartState = getChart(chartId)

  const mergeIndicatorLists = useCallback((incoming = [], previous = undefined) => {
    const prevList = Array.isArray(previous) ? previous : (Array.isArray(indicatorsRef.current) ? indicatorsRef.current : []);
    const serverList = Array.isArray(incoming) ? incoming : [];
    const serverIds = new Set(serverList.map((item) => item?.id).filter(Boolean));

    const merged = serverList.map((item) => {
      if (!item?.id) return item;
      const existing = prevList.find((prev) => prev?.id === item.id);
      if (!existing) return item;
      const hydrated = { ...item };
      if (existing._status) hydrated._status = existing._status;
      if (existing._error) hydrated._error = existing._error;
      if (existing._draft) hydrated._draft = existing._draft;
      if (existing.color && !hydrated.color) hydrated.color = existing.color;
      return hydrated;
    });

    const locals = prevList.filter((item) => item?._local && item?.id && !serverIds.has(item.id));
    if (locals.length) {
      merged.push(...locals);
    }

    const sorted = sortIndicators(merged);
    updateChart(chartId, { indicators: sorted });
    return sorted;
  }, [chartId, updateChart]);

  const showNotice = useCallback((message) => {
    if (!message) return;
    setNotice(message);
    if (noticeTimerRef.current) {
      clearTimeout(noticeTimerRef.current);
    }
    noticeTimerRef.current = setTimeout(() => setNotice(''), 3600);
  }, []);

  const startJob = useCallback((label, meta = {}) => {
    setJobState({
      busy: true,
      label: label || 'Processing indicator changes…',
      indicatorId: meta.indicatorId ?? null,
      type: meta.type ?? null,
    });
  }, []);

  const finishJob = useCallback(() => {
    setJobState({ busy: false, indicatorId: null, type: null, label: '' });
  }, []);

  const guardBusy = useCallback((reason) => {
    const blocked = jobState.busy || chartState?.overlayLoading;
    if (blocked) {
      showNotice('Already processing indicator changes.');
      warn('indicator_action_blocked_busy', {
        reason,
        activeJob: jobState,
        overlayLoading: chartState?.overlayLoading,
      });
    }
    return blocked;
  }, [chartState?.overlayLoading, jobState, showNotice, warn]);

  const fetchAndSyncIndicators = useCallback(async ({ silent = false } = {}) => {
    if (!silent) {
      setIsLoading(true);
    }
    try {
      const payload = await fetchIndicators();
      const list = Array.isArray(payload) ? payload : [];
      const merged = mergeIndicatorLists(list);
      setIndicators(merged);
      return merged;
    } catch (err) {
      const message = err?.message || 'Unable to load indicators';
      setError(message);
      logError('indicator_list_fetch_failed', err);
      return [];
    } finally {
      if (!silent) {
        setIsLoading(false);
      }
    }
  }, [chartId, logError, mergeIndicatorLists]);

  useEffect(() => {
    fetchAndSyncIndicators();
  }, [fetchAndSyncIndicators]);
  useEffect(() => {
    if (!Array.isArray(indicators)) {
      setIndColors((prev) => (Object.keys(prev).length ? {} : prev));
      return;
    }
    const next = buildColorMap(indicators);
    setIndColors((prev) => (shallowEqualMap(prev, next) ? prev : next));
  }, [indicators]);

  useEffect(() => {
    debug('indicator_chart_state_snapshot', {
      hasState: Boolean(chartState),
      version: chartState?._version ?? 0,
      overlayCount: chartState?.overlays?.length ?? 0,
    });
  }, [chartState, debug]);

  useEffect(() => {
    return () => {
      if (noticeTimerRef.current) {
        clearTimeout(noticeTimerRef.current);
      }
    };
  }, []);

  // Defensive: prevent stuck loading overlays if nothing is actually busy
  useEffect(() => {
    if (!isLoading) return;
    if (jobState.busy || chartState?.overlayLoading || refreshingList || bulkActionLoading) return;
    setIsLoading(false);
  }, [bulkActionLoading, chartState?.overlayLoading, isLoading, jobState.busy, refreshingList]);

  // Safety valve: clear overlayLoading if it lingers with no active jobs
  useEffect(() => {
    if (!chartState?.overlayLoading) return undefined;
    const timer = setTimeout(() => {
      const snapshot = getChart(chartId);
      const active =
        jobState.busy ||
        refreshingList ||
        bulkActionLoading ||
        isLoading;
      if (!active && snapshot?.overlayLoading) {
        updateChart(chartId, { overlayLoading: false });
      }
    }, 1800);
    return () => clearTimeout(timer);
  }, [bulkActionLoading, chartId, chartState?.overlayLoading, getChart, isLoading, jobState.busy, refreshingList, updateChart]);

  // Derive ISO start/end from dateRange
  const [startISO, endISO] = useMemo(() => {
    const [s, e] = chartState?.dateRange || []
    const sISO = typeof s === 'string' ? s : s?.toISOString()
    const eISO = typeof e === 'string' ? e : e?.toISOString()
    return [sISO, eISO]
  }, [chartState?.dateRange?.[0], chartState?.dateRange?.[1]])

  const contextPayload = useMemo(() => {
    const interval = chartState?.interval || chartState?.timeframe || null;
    const toISO = (value) => {
      if (!value) return null;
      if (typeof value === 'string') return value;
      if (value?.toISOString) return value.toISOString();
      return null;
    };
    const startVal = toISO(startISO || chartState?.start || chartState?.range?.[0]);
    const endVal = toISO(endISO || chartState?.end || chartState?.range?.[1]);

    return {
      symbol: chartState?.symbol || null,
      start: startVal,
      end: endVal,
      interval,
      datasource: chartState?.datasource || chartState?.provider || null,
      exchange: chartState?.exchange || null,
      instrument_id: chartState?.instrument_id || null,
      provider_id: chartState?.provider_id || null,
      venue_id: chartState?.venue_id || null,
    };
  }, [
    chartState?.symbol,
    chartState?.interval,
    chartState?.timeframe,
    chartState?.datasource,
    chartState?.provider,
    chartState?.exchange,
    chartState?.instrument_id,
    chartState?.provider_id,
    chartState?.venue_id,
    chartState?.start,
    chartState?.end,
    chartState?.range,
    startISO,
    endISO,
  ]);

  const missingContext = useMemo(() => {
    const required = ['symbol', 'interval', 'start', 'end'];
    return required.filter((key) => !contextPayload[key]);
  }, [contextPayload]);

  const requireContextPayload = useCallback(
    (reason = 'unknown') => {
      if (missingContext.length) {
        const msg = `Missing required context param${missingContext.length > 1 ? 's' : ''}: ${missingContext.join(', ')}`;
        setError(msg);
        warn('indicator_context_missing', {
          reason,
          chartId,
          missing: missingContext,
          context: contextPayload,
        });
        return null;
      }
      return contextPayload;
    },
    [missingContext, warn, chartId, contextPayload],
  );

  useEffect(() => {
    if (!chartState || !chartState._version) {
      warn('indicator_refresh_skipped_version', { reason: 'no_version' });
      setIsLoading(false);
      return;
    }

    // Wait for all required context fields before triggering overlay refresh
    // This prevents race conditions where _version bumps before datasource is set
    if (!chartState.datasource || !chartState.symbol || !chartState.interval) {
      warn('indicator_refresh_skipped_context', {
        datasource: chartState.datasource,
        exchange: chartState.exchange,
        symbol: chartState.symbol,
        interval: chartState.interval,
        reason: !chartState.datasource ? 'no_datasource' : !chartState.symbol ? 'no_symbol' : 'no_interval',
      });
      setIsLoading(false);
      return;
    }

    // clear overlays immediately
    updateChart(chartId, { overlays: [] });

    let isMounted = true;
    setIsLoading(true);

    (async () => {
      try {
        await refreshEnabledOverlays(); // uses current indicators list; patches params before overlays
      } catch (e) {
        if (isMounted) {
          setError(e.message);
          logError('indicator_refresh_failed', e);
        }
      } finally {
        if (isMounted) setIsLoading(false);
      }
    })();

    return () => { isMounted = false; };
  }, [chartId, chartState?._version, chartState?.datasource, chartState?.exchange]);

  // When indicator colors change, recolor overlays in chart context (post-render).
  useEffect(() => {
    const overlays = (getChart(chartId)?.overlays) || [];
    if (!overlays.length) return;
    const recolored = applyIndicatorColors(overlays, indColors);
    updateChart(chartId, { overlays: recolored });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [indColors, chartId]);

  // Refresh overlays for enabled indicators
  // ensure enabled indicators carry current chart symbol/interval before fetching overlays
  // re-fetch indicators and ensure enabled indicators' params match current chart before overlays
  // patch enabled indicators to current chart symbol/interval, then compute overlays
  const refreshEnabledOverlays = async (list = indicators) => {
    updateChart(chartId, { overlayLoading: true }); // show loading state

    const ctx = requireContextPayload('overlay_refresh');
    if (!chartState || !ctx) {
      updateChart(chartId, { overlays: [], overlayLoading: false });
      return;
    }

    // if list is empty/undefined, try one fetch to seed; otherwise use provided/current list
    let working = Array.isArray(list) && list.length ? list : indicators;
    if (!Array.isArray(working) || working.length === 0) {
      try {
        working = (await fetchIndicators()) || [];
        const sortedWorking = sortIndicators(working);
        working = sortedWorking;
        setIndicators(sortedWorking);
        updateChart(chartId, { indicators: sortedWorking });
      } catch (e) {
        logError('indicator_seed_failed', e);
        updateChart(chartId, { overlays: [], overlayLoading: false });
        return;
      }
    }

    // patch params for enabled indicators if symbol/interval mismatch
    const enabled = working.filter(i => i?.enabled && !i?._local);
    info('overlay_refresh_start', {
      enabled: enabled.length,
      symbol: ctx.symbol,
      interval: ctx.interval,
    });
    const active = enabled;

    // compute overlays for enabled indicators using current chart window
    const body = { ...ctx };

    const results = await Promise.all(
      active.map(async (ind) => {
        try {
          const payload = await fetchIndicatorOverlays(ind.id, body);
          info('overlay_fetch_success', {
            indicatorId: ind.id,
            indicatorType: ind.type,
            instrument_id: chartState?.instrument_id,
            hasPayload: Boolean(payload),
          });
          if (!payload) return null;
          if (payload?.type && payload?.payload) {
            return {
              ...payload,
              ind_id: ind.id,
              type: payload.type || ind.type,
              payload: payload.payload,
              color: payload.color ?? ind.color,
              source: payload.source ?? 'indicator',
            };
          }
          return { ind_id: ind.id, type: ind.type, payload };
        } catch (e) {
          const msg = String(e?.message ?? e);
          if (
            msg.includes('Indicator not found') ||
            msg.includes('No candles available') ||
            msg.includes('No overlays computed')
          ) {
            warn('overlay_fetch_skipped', { indicatorId: ind.id, message: msg });
            if (msg.includes('No candles available')) {
              const label = ind?.name || formatIndicatorType(ind?.type);
              setError(`No candles were available for ${label}. Adjust the chart range, timeframe, or datasource and try again.`);
            }
            return null;
          }
          logError('overlay_fetch_failed', { indicatorId: ind.id, instrument_id: chartState?.instrument_id }, e);
          return null;
        }
      })
    );

    const overlaysPayload = results.filter(Boolean);
    const nextColorMap = buildColorMap(working);
    setIndColors((prev) => (shallowEqualMap(prev, nextColorMap) ? prev : nextColorMap));

    const colored = applyIndicatorColors(overlaysPayload, nextColorMap);
    updateChart(chartId, { overlays: colored, overlayLoading: false });
    info('overlay_refresh_complete', {
      overlays: colored.length,
      indicatorsProcessed: active.length,
      enabledCount: enabled.length,
    });
  };

  const handleRefreshList = useCallback(async () => {
    if (guardBusy('manual_refresh')) return;
    setRefreshingList(true);
    try {
      const latest = await fetchAndSyncIndicators({ silent: true });
      await refreshEnabledOverlays(latest);
    } catch (e) {
      setError(e.message);
      logError('indicator_manual_refresh_failed', e);
    } finally {
      setRefreshingList(false);
    }
  }, [fetchAndSyncIndicators, refreshEnabledOverlays, guardBusy, logError]);

  const applySignalRules = useCallback((indicatorId, ruleSelection) => {
    const selection = Array.isArray(ruleSelection) ? ruleSelection : null;
    if (!indicatorId || !selection) return;
    const currentConfig = getChart(chartId)?.signalsConfig || {};
    const currentEnabled = currentConfig.enabledRules || {};
    const nextEnabled = { ...currentEnabled };

    nextEnabled[indicatorId] = selection;
    const nextSignalsConfig = {
      ...currentConfig,
      enabledRules: nextEnabled,
    };
    updateChart(chartId, { signalsConfig: nextSignalsConfig });
  }, [chartId, getChart, updateChart]);

  const createIndicatorOptimistic = useCallback(async (meta, params, reuseId = null) => {
    const tempId = reuseId || `temp-${Date.now()}`;
    const optimisticIndicator = {
      id: tempId,
      name: meta.name,
      type: meta.type,
      params,
      enabled: true,
      color: meta.color || DEFAULT_INDICATOR_COLOR,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      _local: true,
      _status: 'creating',
      _draft: { type: meta.type, params, name: meta.name, color: meta.color || null },
    };

    setIndicators((prev) => {
      const withoutTemp = prev.filter((ind) => ind.id !== tempId);
      const merged = sortIndicators([...withoutTemp, optimisticIndicator]);
      updateChart(chartId, { indicators: merged });
      return merged;
    });

    startJob('Creating indicator…', { indicatorId: tempId, type: 'create' });
    try {
      const created = await createIndicator({ type: meta.type, params, name: meta.name });
      const createdIndicator = {
        ...created,
        _status: 'computing',
        _local: false,
        color: created?.color || optimisticIndicator.color,
      };

      setIndicators((prev) => {
        const filtered = prev.filter((ind) => ind.id !== tempId);
        const merged = sortIndicators([...filtered, createdIndicator]);
        updateChart(chartId, { indicators: merged });
        return merged;
      });

      const latest = await fetchAndSyncIndicators({ silent: true });
      await refreshEnabledOverlays(latest);
      setIndicators((prev) => prev.map((ind) => (
        ind.id === createdIndicator.id ? { ...ind, _status: null } : ind
      )));

      return createdIndicator;
    } catch (e) {
      setError(e.message);
      logError('indicator_create_failed', e);
      setIndicators((prev) => prev.map((ind) => (
        ind.id === tempId
          ? { ...ind, _status: 'failed', _error: e.message }
          : ind
      )));
      throw e;
    } finally {
      finishJob();
    }
  }, [chartId, finishJob, fetchAndSyncIndicators, logError, refreshEnabledOverlays, startJob, updateChart]);

  // Handlers for modal save/delete
  const handleSave = async (meta) => {
    const core = stripRuntimeParams(normalizeParams(meta.params));
    const ctx = requireContextPayload('save_indicator');
    if (!ctx) return;
    if (guardBusy('save_indicator')) return;

    if ('lookbacks' in core) {
      if (!Array.isArray(core.lookbacks) || core.lookbacks.length === 0) {
        setError('Lookbacks must be a comma/space-separated list of integers, e.g., "5, 10, 20".');
        return;
      }
    }

    const params = { ...core, ...ctx };

    setError(null);
    setModalOpen(false);
    setEditing(null);

    try {
      let indicatorId = meta.id ?? null;
      let needsIndicatorUpdate = true;

      if (meta.id) {
        const existing = indicators.find((i) => i.id === meta.id);
        if (existing) {
          const existingCore = stripRuntimeParams(existing.params || {});
          const coreParamsChanged = JSON.stringify(existingCore) !== JSON.stringify(core);
          const nameChanged = meta.name !== existing.name;
          needsIndicatorUpdate = coreParamsChanged || nameChanged;
        }
      }

      if (!needsIndicatorUpdate) {
        if (meta.id) {
          applySignalRules(meta.id, meta.signalRules);
        }
        return;
      }

      if (!meta.id) {
        const created = await createIndicatorOptimistic(meta, params);
        if (created?.id && meta.signalRules?.length) {
          applySignalRules(created.id, meta.signalRules);
        }
        return;
      }

      if (guardBusy('indicator_update')) return;

      startJob('Updating indicator…', { indicatorId: meta.id, type: 'update' });
      setIndicators((prev) => prev.map((ind) => (
        ind.id === meta.id ? { ...ind, _status: 'updating' } : ind
      )));
      updateChart(chartId, { overlays: [], overlayLoading: true });

      const existing = indicators.find((i) => i.id === meta.id) || null;
      const payload = await updateIndicator(meta.id, {
        type: meta.type,
        params,
        name: meta.name,
        color: existing?.color ?? null,
      });
      indicatorId = payload?.id ?? meta.id;

      if (indicatorId) {
        applySignalRules(indicatorId, meta.signalRules);
      }

      const latest = await fetchAndSyncIndicators({ silent: false });
      await refreshEnabledOverlays(latest);
      setIndicators((prev) => prev.map((ind) => (
        ind.id === indicatorId ? { ...ind, _status: null } : ind
      )));
    } catch (e) {
      setError(e.message);
      logError('indicator_save_failed', e);
      setIndicators((prev) => prev.map((ind) => (
        ind.id === meta.id ? { ...ind, _status: 'failed', _error: e.message } : ind
      )));
      updateChart(chartId, { overlayLoading: false });
    } finally {
      setIsLoading(false);
      finishJob();
    }
  };

  // Opens the delete confirmation modal instead of deleting directly
  const openDeleteModal = (id) => {
    if (!id) return;
    const indicator = indicators.find((ind) => ind.id === id);
    setDeleteModal({
      open: true,
      indicatorId: id,
      indicatorName: indicator?.name || indicator?.type || 'this indicator',
    });
  };

  // Actual delete logic - called from modal confirmation
  const confirmDelete = async (id) => {
    if (!id) return;
    if (guardBusy('delete_indicator')) return;
    setIsLoading(true);
    startJob('Removing indicator…', { indicatorId: id, type: 'delete' });
    try {
      await deleteIndicator(id);
      setSelectedIds((prev) => {
        if (!prev || prev.size === 0 || !prev.has(id)) return prev;
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      const currentConfig = getChart(chartId)?.signalsConfig;
      if (currentConfig && typeof currentConfig === 'object') {
        const nextConfig = { ...currentConfig };
        let changed = false;

        const enabledRules = currentConfig.enabledRules;
        if (enabledRules && Object.prototype.hasOwnProperty.call(enabledRules, id)) {
          const nextEnabled = { ...enabledRules };
          delete nextEnabled[id];
          if (Object.keys(nextEnabled).length > 0) {
            nextConfig.enabledRules = nextEnabled;
          } else {
            delete nextConfig.enabledRules;
          }
          changed = true;
        }

        if (changed) {
          const remainingKeys = Object.keys(nextConfig);
          updateChart(chartId, {
            signalsConfig: remainingKeys.length > 0 ? nextConfig : null,
          });
        }
      }
      const latest = await fetchAndSyncIndicators({ silent: true });
      await refreshEnabledOverlays(latest);
      setDeleteModal({ open: false, indicatorId: null, indicatorName: '' });
    } catch (e) {
      setError(e.message);
      logError('indicator_delete_failed', e);
      throw e; // Re-throw so modal can handle error state
    } finally {
      setIsLoading(false);
      finishJob();
    }
  };

  // Legacy handler - now opens modal
  const handleDelete = (id) => {
    openDeleteModal(id);
  };

  const toggleIndicatorSelection = (id) => {
    if (!id) return
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  const handleBulkDelete = async () => {
    const ids = Array.from(selectedIds || [])
    if (!ids.length) return
    if (guardBusy('bulk_delete')) return;
    try {
      setBulkActionLoading(true)
      setIsLoading(true)
      startJob('Deleting indicators…', { type: 'bulk-delete' });
      await bulkDeleteIndicators(ids)
      setSelectedIds(new Set())
      const latest = await fetchAndSyncIndicators({ silent: true })
      await refreshEnabledOverlays(latest)
    } catch (e) {
      setError(e.message)
      logError('indicator_bulk_delete_failed', e)
    } finally {
      setBulkActionLoading(false)
      setIsLoading(false)
      finishJob();
    }
  }

  const handleBulkToggle = async (enabled) => {
    const ids = Array.from(selectedIds || [])
    if (!ids.length) return
    if (guardBusy('bulk_toggle')) return;
    try {
      setBulkActionLoading(true)
      startJob(enabled ? 'Showing overlays…' : 'Hiding overlays…', { type: 'bulk-toggle' });
      await bulkToggleIndicators(ids, enabled)
      const latest = await fetchAndSyncIndicators({ silent: true })
      await refreshEnabledOverlays(latest)
    } catch (e) {
      setError(e.message)
      logError('indicator_bulk_toggle_failed', e)
    } finally {
      setBulkActionLoading(false)
      finishJob();
    }
  }

  const toggleEnable = async (id) => {
    if (guardBusy('toggle_enable')) return;
    const target = indicators.find((indicator) => indicator.id === id)
    if (!target) return
    const previousEnabled = !!target.enabled
    const nextEnabled = !previousEnabled

    startJob(nextEnabled ? 'Enabling overlay…' : 'Disabling overlay…', { indicatorId: id, type: 'toggle' });

    setIndicators((prev) => {
      const next = sortIndicators(
        prev.map((indicator) =>
          indicator.id === id ? { ...indicator, enabled: nextEnabled, _status: 'updating' } : indicator,
        ),
      )
      updateChart(chartId, { indicators: next })
      return next
    })

    try {
      await setIndicatorEnabled(id, nextEnabled);
      const latest = await fetchAndSyncIndicators({ silent: true })
      await refreshEnabledOverlays(latest)
      setIndicators((prev) => prev.map((indicator) => (
        indicator.id === id ? { ...indicator, _status: null } : indicator
      )));
    } catch (err) {
      setError(err.message)
      logError('indicator_toggle_failed', err)
      setIndicators((prev) => {
        const next = sortIndicators(
          prev.map((indicator) =>
            indicator.id === id ? { ...indicator, enabled: previousEnabled, _status: null } : indicator,
          ),
        )
        updateChart(chartId, { indicators: next })
        return next
      })
    } finally {
      finishJob();
    }
  }

  // Regenerate signals
  const generateSignals = async (id) => {
    if (guardBusy('generate_signals')) return;
    startJob('Generating signals…', { indicatorId: id, type: 'signals' });
    info('signal_generation_start', {
      indicatorId: id,
      chartState: {
        datasource: chartState?.datasource,
        exchange: chartState?.exchange,
        symbol: chartState?.symbol,
        interval: chartState?.interval,
      },
    });

    // Validate chart state has all required fields before generating signals
    if (!chartState?.datasource || !chartState?.symbol || !chartState?.interval) {
      const missing = !chartState?.datasource ? 'datasource' : !chartState?.symbol ? 'symbol' : 'interval';
      const errorMsg = `Cannot generate signals: ${missing} is not set. Please ensure chart is fully loaded.`;
      setError(errorMsg);
      warn('signal_generation_blocked', {
        indicatorId: id,
        reason: `missing_${missing}`,
        chartState: {
          datasource: chartState?.datasource,
          exchange: chartState?.exchange,
          symbol: chartState?.symbol,
          interval: chartState?.interval,
        },
      });
      return;
    }

    const indicator = indicators.find((ind) => ind.id === id);

    // Get fresh chart state right before calling runSignalGeneration
    // to avoid stale closure issues
    const freshChartState = getChart(chartId);
    info('signal_generation_fresh_state', {
      indicatorId: id,
      freshChartState: {
        datasource: freshChartState?.datasource,
        exchange: freshChartState?.exchange,
        symbol: freshChartState?.symbol,
        interval: freshChartState?.interval,
      },
    });

    try {
      await runSignalGeneration({
        indicator,
        chartId,
        chartState: freshChartState,
        startISO,
        endISO,
        indColors,
        getChart,
        updateChart,
        setError,
        signalsAdapter: generateIndicatorSignals,
      });
    } catch (e) {
      setError(e.message);
      logError('signal_generation_failed', e);
    } finally {
      finishJob();
    }
  };


  const openEditModal = (indicator = null) => {
    if (indicator) {
      const enabledRules = chartState?.signalsConfig?.enabledRules?.[indicator.id] || []
      setEditing({ ...indicator, signalRules: [...enabledRules] })
    } else {
      setEditing(null)
    }
    setModalOpen(true)
    setError(null)
  }

  const handleSelectColor = async (indicatorId, color) => {
    if (guardBusy('select_color')) return;
    const indicator = indicators.find((ind) => ind.id === indicatorId);
    if (!indicator) return;

    const normalizedColor = typeof color === 'string' && color.trim()
      ? color.trim()
      : DEFAULT_INDICATOR_COLOR;

    setIndColors((prev) => ({ ...prev, [indicatorId]: normalizedColor }));

    const optimisticIndicators = indicators.map((ind) =>
      ind.id === indicatorId ? { ...ind, color: normalizedColor ?? null } : ind,
    );
    setIndicators(optimisticIndicators);
    updateChart(chartId, { indicators: optimisticIndicators });

    try {
      // Only send color, don't modify params to avoid triggering expensive recomputation
      const updated = await updateIndicator(indicatorId, {
        type: indicator.type,
        name: indicator.name,
        params: indicator.params,
        color: normalizedColor,
      });
      if (updated) {
        setIndicators((prev) => {
          const next = prev.map((ind) => (ind.id === indicatorId ? updated : ind));
          updateChart(chartId, { indicators: next });
          return next;
        });
        setIndColors((prev) => ({
          ...prev,
          [indicatorId]: updated.color?.trim() ? updated.color : DEFAULT_INDICATOR_COLOR,
        }));
      }
    } catch (e) {
      setError(e.message);
      logError('indicator_color_update_failed', e);
      setIndColors((prev) => ({
        ...prev,
        [indicatorId]: indicator.color?.trim() ? indicator.color : DEFAULT_INDICATOR_COLOR,
      }));
      setIndicators((prev) => {
        const next = prev.map((ind) => (
          ind.id === indicatorId
            ? {
                ...ind,
                color: indicator.color ?? null,
                params: indicator.params ?? ind.params,
              }
            : ind
        ));
        updateChart(chartId, { indicators: next });
        return next;
      });
    }
  };

  const handleDuplicate = async (id) => {
    if (!id) return
    if (guardBusy('duplicate_indicator')) return;
    try {
      setDuplicateBusyId(id)
      startJob('Duplicating indicator…', { indicatorId: id, type: 'duplicate' });
      await duplicateIndicator(id)
      const latest = await fetchAndSyncIndicators({ silent: true })
      await refreshEnabledOverlays(latest)
    } catch (e) {
      setError(e.message)
      logError('indicator_duplicate_failed', e)
    } finally {
      setDuplicateBusyId(null)
      finishJob();
    }
  }

  // Recompute overlays for a single indicator
  const handleRecomputeOverlays = async (id) => {
    if (!id) return;
    if (guardBusy('recompute_overlays')) return;
    const indicator = indicators.find((ind) => ind.id === id);
    if (!indicator) return;

    const ctx = requireContextPayload('recompute_overlays');
    if (!ctx) return;

    startJob('Recomputing overlays…', { indicatorId: id, type: 'recompute' });
    setIndicators((prev) => prev.map((ind) => (
      ind.id === id ? { ...ind, _status: 'computing' } : ind
    )));

    try {
      const payload = await fetchIndicatorOverlays(id, ctx);
      if (payload) {
        const currentOverlays = getChart(chartId)?.overlays || [];
        // Remove old overlays for this indicator and add new ones
        const filtered = currentOverlays.filter((o) => o.ind_id !== id);
        const newOverlay = { ind_id: id, type: indicator.type, payload };
        const merged = [...filtered, newOverlay];
        const colored = applyIndicatorColors(merged, indColors);
        updateChart(chartId, { overlays: colored });
      }
      setIndicators((prev) => prev.map((ind) => (
        ind.id === id ? { ...ind, _status: null } : ind
      )));
      info('overlay_recompute_success', { indicatorId: id });
    } catch (e) {
      setError(e.message);
      logError('overlay_recompute_failed', { indicatorId: id }, e);
      setIndicators((prev) => prev.map((ind) => (
        ind.id === id ? { ...ind, _status: null } : ind
      )));
    } finally {
      finishJob();
    }
  };

  const retryCreate = async (indicator) => {
    if (!indicator?._local || !indicator?._draft) return;
    const ctx = requireContextPayload('retry_indicator');
    if (!ctx || guardBusy('retry_indicator')) return;
    const draftParams = stripRuntimeParams(indicator._draft.params || {});
    const params = { ...draftParams, ...ctx };
    try {
      await createIndicatorOptimistic(
        { type: indicator._draft.type, name: indicator._draft.name, signalRules: indicator.signalRules || [], color: indicator._draft.color },
        params,
        indicator.id
      );
    } catch {
      // errors are surfaced via setError; no-op here
    }
  };

  const removeLocalIndicator = (id) => {
    if (!id) return;
    setIndicators((prev) => {
      const next = prev.filter((ind) => ind.id !== id || !ind._local);
      updateChart(chartId, { indicators: next });
      return next;
    });
  };

  const isSignalsLoading = !!chartState?.signalsLoading
  const signalsLoadingFor = chartState?.signalsLoadingFor

  const typeOptions = useMemo(() => {
    const unique = new Set();
    indicators.forEach((indicator) => {
      if (indicator?.type) unique.add(indicator.type);
    });
    return Array.from(unique).sort();
  }, [indicators]);

  const trimmedSearchQuery = searchQuery.trim();

  const filteredIndicators = useMemo(() => {
    const base = showEnabledOnly ? indicators.filter((ind) => ind?.enabled) : indicators;

    const byType = typeFilter === 'all'
      ? base
      : base.filter((indicator) => (indicator?.type ?? '') === typeFilter);

    const query = trimmedSearchQuery.toLowerCase();
    if (!query) return byType;

    return byType.filter((indicator) => {
      const name = (indicator?.name ?? '').toLowerCase();
      const type = (indicator?.type ?? '').toLowerCase();
      const paramsString = JSON.stringify(indicator?.params ?? {}).toLowerCase();
      const id = (indicator?.id ?? '').toLowerCase();
      return (
        name.includes(query) ||
        type.includes(query) ||
        id.includes(query) ||
        paramsString.includes(query)
      );
    });
  }, [indicators, showEnabledOnly, trimmedSearchQuery, typeFilter]);

  const enabledCount = useMemo(
    () => indicators.filter((indicator) => indicator?.enabled).length,
    [indicators]
  );

  const totalCount = indicators.length;
  const filteredCount = filteredIndicators.length;
  const totalPages = filteredCount ? Math.ceil(filteredCount / pageSize) : 1;
  const pageStartIndex = (currentPage - 1) * pageSize;
  const paginatedIndicators = filteredIndicators.slice(
    pageStartIndex,
    pageStartIndex + pageSize,
  );

  useEffect(() => {
    setCurrentPage(1);
  }, [trimmedSearchQuery, typeFilter, showEnabledOnly, pageSize]);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages || 1);
    }
  }, [currentPage, totalPages]);

  useEffect(() => {
    setSelectedIds((prev) => {
      if (!prev || prev.size === 0) return prev;
      const allowed = new Set(indicators.map((ind) => ind.id));
      let changed = false;
      const next = new Set();
      prev.forEach((id) => {
        if (allowed.has(id)) {
          next.add(id);
        } else {
          changed = true;
        }
      });
      if (changed || next.size !== prev.size) {
        return next;
      }
      return prev;
    });
  }, [indicators]);

  const indicatorSummary = useMemo(() => {
    if (!totalCount) return 'No indicators created yet.';
    if (!filteredCount) {
      if (showEnabledOnly && enabledCount === 0) return 'No enabled indicators found.';
      return 'No indicators match your filters yet.';
    }

    const pageStart = pageStartIndex + 1;
    const pageEnd = Math.min(pageStartIndex + pageSize, filteredCount);
    const pageSummary = `${pageStart}-${pageEnd} of ${filteredCount} matching ${filteredCount === 1 ? 'indicator' : 'indicators'}`;

    if (!showEnabledOnly && !trimmedSearchQuery && (typeFilter === 'all' || typeFilter === '')) {
      if (filteredCount === totalCount) return `${pageSummary} (out of ${totalCount} total)`;
      return `${pageSummary} (from ${totalCount} total)`;
    }
    return pageSummary;
  }, [
    totalCount,
    filteredCount,
    showEnabledOnly,
    enabledCount,
    pageStartIndex,
    pageSize,
    trimmedSearchQuery,
    typeFilter,
  ]);

  const noIndicatorsMessage = useMemo(() => {
    if (!totalCount) return 'No indicators yet. Create one to get started.';
    if (showEnabledOnly && !trimmedSearchQuery && (typeFilter === 'all' || typeFilter === '')) {
      return 'No enabled indicators yet. Toggle the filter to view all indicators.';
    }
    return 'No indicators match your filters yet. Try adjusting the filters or search terms.';
  }, [totalCount, showEnabledOnly, trimmedSearchQuery, typeFilter]);

  const sectionBusy = Boolean(
    jobState.busy ||
    chartState?.overlayLoading ||
    refreshingList ||
    bulkActionLoading
  );

  const overlayMessage = jobState.busy
    ? (jobState.label || BUSY_MESSAGE)
    : chartState?.overlayLoading
      ? BUSY_MESSAGE
      : refreshingList
        ? 'Refreshing indicators…'
        : bulkActionLoading
          ? 'Applying bulk changes…'
          : '';

  const actionLocked = sectionBusy || isLoading;

  if (!chartState || !chartId) return <div className="text-red-500">Error: No chart state found</div>

  return (
    <div className="space-y-4">
      {error && (
        <div className="relative rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
          <div className="pr-6">
            <p className="font-semibold text-red-200">Error</p>
            <p className="mt-1 text-red-100/90">{error}</p>
          </div>
          <button
            type="button"
            onClick={() => setError(null)}
            className="absolute right-3 top-3 text-red-200/70 hover:text-red-100"
            aria-label="Dismiss error"
          >
            <X className="size-4" />
          </button>
        </div>
      )}

      {notice && !error && (
        <div className="flex items-center gap-2 rounded-lg border border-amber-400/30 bg-amber-400/10 px-3 py-2 text-xs text-amber-100">
          <span className="h-2 w-2 rounded-full bg-amber-300" aria-hidden="true" />
          <span>{notice}</span>
        </div>
      )}

      <section className="relative overflow-visible rounded-2xl border border-white/10 bg-[#0d1422]/90 shadow-[0_22px_80px_-60px_rgba(0,0,0,0.85)]">
        <LoadingOverlay show={Boolean(overlayMessage)} message={overlayMessage || 'Working…'} />
        <div className={`relative space-y-4 p-5 ${overlayMessage ? 'opacity-80' : ''}`}>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="space-y-1">
              <p className="text-[11px] uppercase tracking-[0.32em] text-slate-400">Indicators / overlays & signals</p>
              <div className="flex flex-wrap items-center gap-3 text-xs text-slate-400">
                <span className="font-semibold text-slate-100">{indicatorSummary}</span>
                <span className="h-4 w-px bg-white/10" aria-hidden="true" />
                <span className="text-slate-500">Enabled</span>
                <span className="font-semibold text-[color:var(--accent-text-soft)]">{enabledCount}</span>
                <span className="h-4 w-px bg-white/10" aria-hidden="true" />
                <span className="text-slate-500">Total</span>
                <span className="font-semibold text-slate-100">{totalCount}</span>
                {sectionBusy && (
                  <span className="inline-flex items-center gap-2 rounded-full bg-amber-400/10 px-3 py-1 text-[11px] font-medium text-amber-100">
                    <span className="h-2 w-2 rounded-full bg-amber-300 animate-pulse" />
                    {BUSY_MESSAGE}
                  </span>
                )}
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={handleRefreshList}
                disabled={actionLocked}
                className={`inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-xs font-semibold transition ${
                  actionLocked
                    ? 'cursor-not-allowed border-white/10 text-slate-500'
                    : 'border-white/15 text-slate-200 hover:border-[color:var(--accent-alpha-40)] hover:text-white'
                }`}
              >
                <RefreshCw className={`size-4 ${refreshingList ? 'animate-spin' : ''}`} />
                Sync list
              </button>
              <button
                type="button"
                disabled={actionLocked}
                onClick={() => openEditModal()}
                className={`inline-flex items-center gap-2 rounded-lg px-3 py-2 text-xs font-semibold uppercase tracking-[0.2em] shadow-[0_12px_35px_-18px_var(--accent-shadow-strong)] transition ${
                  actionLocked
                    ? 'cursor-not-allowed bg-[color:var(--accent-alpha-10)] text-slate-500'
                    : 'bg-[color:var(--accent-alpha-25)] text-[color:var(--accent-text-bright)] hover:bg-[color:var(--accent-alpha-35)]'
                }`}
              >
                <Plus className="size-3.5" />
                Add indicator
              </button>
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-[minmax(0,1.2fr)_0.8fr_0.7fr_0.6fr]">
            <label className="relative block">
              <span className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-slate-500">⌕</span>
              <input
                type="text"
                placeholder="Search by name, type, or parameter"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full rounded-lg border border-white/10 bg-[#0b111d] px-8 py-2 text-sm text-slate-100 placeholder-slate-500 outline-none transition focus:border-[color:var(--accent-alpha-40)] focus:ring-2 focus:ring-[color:var(--accent-ring-strong)]"
              />
            </label>

            <select
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
              className="w-full rounded-lg border border-white/10 bg-[#0b111d] px-3 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-200 outline-none transition hover:border-[color:var(--accent-alpha-40)] focus:border-[color:var(--accent-alpha-60)]"
            >
              <option value="all">All types</option>
              {typeOptions.map((type) => (
                <option key={type} value={type}>{formatIndicatorType(type)}</option>
              ))}
            </select>

            <label className="flex items-center gap-3 rounded-lg border border-white/10 bg-[#0b111d] px-3 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-200">
              <input
                type="checkbox"
                className="size-4 accent-[color:var(--accent-base)]"
                checked={showEnabledOnly}
                onChange={(event) => setShowEnabledOnly(event.target.checked)}
              />
              <span>Enabled only</span>
            </label>

            <div className="flex items-center justify-end gap-2 rounded-lg border border-white/5 bg-[#0b111d] px-3 py-2 text-xs text-slate-300">
              <span className="text-slate-500">Page size</span>
              <DropdownSelect
                value={pageSize}
                onChange={setPageSize}
                options={PAGE_SIZE_OPTIONS.map((size) => ({ value: size, label: String(size) }))}
              />
            </div>
          </div>

          {selectedIds.size > 0 && (
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-[color:var(--accent-alpha-30)] bg-[color:var(--accent-alpha-08)] px-4 py-3">
              <div className="flex items-center gap-3 text-xs font-semibold text-[color:var(--accent-text-soft)]">
                <span>{selectedIds.size} selected</span>
                <span className="text-[11px] text-slate-500">Single-job guard prevents concurrent runs.</span>
              </div>
              <div className="flex flex-wrap items-center gap-2 text-xs font-semibold">
                <button
                  type="button"
                  disabled={actionLocked}
                  onClick={() => handleBulkToggle(true)}
                  className={`rounded border px-3 py-1.5 transition ${
                    actionLocked ? 'cursor-not-allowed border-white/10 text-slate-500' : 'border-white/10 text-slate-100 hover:border-[color:var(--accent-alpha-40)] hover:text-white'
                  }`}
                >
                  Show overlays
                </button>
                <button
                  type="button"
                  disabled={actionLocked}
                  onClick={() => handleBulkToggle(false)}
                  className={`rounded border px-3 py-1.5 transition ${
                    actionLocked ? 'cursor-not-allowed border-white/10 text-slate-500' : 'border-white/10 text-slate-100 hover:border-[color:var(--accent-alpha-40)] hover:text-white'
                  }`}
                >
                  Hide overlays
                </button>
                <button
                  type="button"
                  disabled
                  className="rounded border border-dashed border-white/10 px-3 py-1.5 text-slate-500"
                  title="Disabled: indicator jobs run one at a time."
                >
                  Bulk signals (queued)
                </button>
                <button
                  type="button"
                  disabled={actionLocked}
                  onClick={handleBulkDelete}
                  className={`rounded border px-3 py-1.5 transition ${
                    actionLocked ? 'cursor-not-allowed border-white/10 text-slate-500' : 'border-rose-400/40 text-rose-100 hover:border-rose-300/60 hover:text-rose-50'
                  }`}
                >
                  Delete
                </button>
              </div>
            </div>
          )}

          <div className="space-y-2">
            {paginatedIndicators.map(indicator => {
              const isGenerating = (isSignalsLoading && signalsLoadingFor === indicator.id) || (jobState.busy && jobState.indicatorId === indicator.id && jobState.type === 'signals')
              const disableSignals = actionLocked || (isSignalsLoading && signalsLoadingFor !== indicator.id)
              const isSelected = selectedIds.has(indicator.id)
              return (
                <IndicatorCard
                  key={indicator.id}
                  indicator={indicator}
                  color={indColors[indicator.id] ?? DEFAULT_INDICATOR_COLOR}
                  onToggle={toggleEnable}
                  onEdit={openEditModal}
                  onDelete={handleDelete}
                  onDuplicate={handleDuplicate}
                  onGenerateSignals={generateSignals}
                  onSelectColor={handleSelectColor}
                  onRecompute={handleRecomputeOverlays}
                  colorSwatches={COLOR_SWATCHES}
                  isGeneratingSignals={isGenerating}
                  disableSignalAction={disableSignals}
                  selected={isSelected}
                  onSelectionToggle={() => toggleIndicatorSelection(indicator.id)}
                  duplicatePending={duplicateBusyId === indicator.id}
                  busy={actionLocked}
                  activeJobId={jobState.indicatorId}
                  onRetryCreate={() => retryCreate(indicator)}
                  onRemoveLocal={() => removeLocalIndicator(indicator.id)}
                />
              )
            })}

            {!paginatedIndicators.length && !isLoading && (
              <div className="rounded-lg border border-dashed border-slate-800 bg-slate-900/30 px-4 py-6 text-center text-sm text-slate-500">
                {noIndicatorsMessage}
              </div>
            )}
          </div>

          {filteredCount > pageSize && (
            <nav className="mt-2 flex items-center justify-between gap-3 text-xs text-slate-300" aria-label="Pagination">
              <span className="font-medium">Page <span className="text-[color:var(--accent-text-soft)]">{currentPage}</span> of {totalPages}</span>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
                  disabled={currentPage === 1}
                  className={`rounded px-3 py-1 transition ${
                    currentPage === 1
                      ? 'cursor-not-allowed text-slate-600'
                      : 'text-slate-200 hover:bg-slate-800'
                  }`}
                >
                  Prev
                </button>
                {Array.from({ length: Math.min(totalPages, 5) }).map((_, index) => {
                  let pageNumber = index + 1;
                  if (totalPages > 5 && currentPage > 3) {
                    pageNumber = currentPage - 2 + index;
                  }
                  if (pageNumber > totalPages) return null;
                  const isActive = pageNumber === currentPage;
                  return (
                    <button
                      key={pageNumber}
                      type="button"
                      onClick={() => setCurrentPage(pageNumber)}
                      className={`rounded px-2.5 py-1 transition ${
                        isActive
                          ? 'bg-[color:var(--accent-alpha-18)] text-[color:var(--accent-text-soft)]'
                          : 'text-slate-400 hover:bg-slate-800 hover:text-white'
                      }`}
                    >
                      {pageNumber}
                    </button>
                  );
                })}
                <button
                  type="button"
                  onClick={() => setCurrentPage((prev) => Math.min(prev + 1, totalPages))}
                  disabled={currentPage === totalPages}
                  className={`rounded px-3 py-1 transition ${
                    currentPage === totalPages
                      ? 'cursor-not-allowed text-slate-600'
                      : 'text-slate-200 hover:bg-slate-800'
                  }`}
                >
                  Next
                </button>
              </div>
            </nav>
          )}
        </div>
      </section>

      <IndicatorModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        initial={editing}
        onSave={handleSave}
        error={error}
      />

      <DeleteIndicatorModal
        open={deleteModal.open}
        indicatorId={deleteModal.indicatorId}
        indicatorName={deleteModal.indicatorName}
        onClose={() => setDeleteModal({ open: false, indicatorId: null, indicatorName: '' })}
        onConfirm={confirmDelete}
      />
    </div>
  )
}
