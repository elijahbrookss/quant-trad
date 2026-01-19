import { useState, useEffect, useMemo, useCallback } from 'react'
import { Switch, Popover, Transition, PopoverButton, PopoverPanel } from '@headlessui/react'
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


  const { updateChart, getChart } = useChartState()

  const logger = useMemo(() => createLogger('IndicatorSection', { chartId }), [chartId])
  const { debug, info, warn, error: logError } = logger

  // Read current chart slice
  const chartState = getChart(chartId)

  const fetchAndSyncIndicators = useCallback(async ({ silent = false } = {}) => {
    if (!silent) {
      setIsLoading(true);
    }
    try {
      const payload = await fetchIndicators();
      const list = Array.isArray(payload) ? payload : [];
      const sorted = sortIndicators(list);
      setIndicators(sorted);
      updateChart(chartId, { indicators: sorted });
      return sorted;
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
  }, [chartId, updateChart, logError]);

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

  // Derive ISO start/end from dateRange
  const [startISO, endISO] = useMemo(() => {
    const [s, e] = chartState?.dateRange || []
    const sISO = typeof s === 'string' ? s : s?.toISOString()
    const eISO = typeof e === 'string' ? e : e?.toISOString()
    return [sISO, eISO]
  }, [chartState?.dateRange?.[0], chartState?.dateRange?.[1]])

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

    if (!chartState) {
      updateChart(chartId, { overlays: [], overlayLoading: false });
      return;
    }

    // Wait for datasource and all required fields before loading overlays
    // This prevents race conditions where _version bumps before datasource is set
    if (!chartState.datasource || !chartState.symbol || !chartState.interval || !chartState.instrument_id) {
      warn('overlay_refresh_waiting_for_context', {
        chartId,
        hasChartState: Boolean(chartState),
        datasource: chartState.datasource,
        exchange: chartState.exchange,
        symbol: chartState.symbol,
        interval: chartState.interval,
        instrument_id: chartState.instrument_id,
        reason: !chartState.datasource
          ? 'no_datasource'
          : !chartState.symbol
            ? 'no_symbol'
            : !chartState.interval
              ? 'no_interval'
              : 'no_instrument_id',
      });
      updateChart(chartId, { overlays: [], overlayLoading: false });
      return;
    }

    // if list is empty/undefined, try one fetch to seed; otherwise use provided/current list
    let working = Array.isArray(list) && list.length ? list : indicators;
    if (!Array.isArray(working) || working.length === 0) {
      try {
        working = (await fetchIndicators({ symbol: chartState.symbol, interval: chartState.interval })) || [];
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
    const enabled = working.filter(i => i?.enabled);
    info('overlay_refresh_start', {
      enabled: enabled.length,
      symbol: chartState.symbol,
      interval: chartState.interval,
    });
    const active = enabled;

    // compute overlays for enabled indicators using current chart window
    const body = {
      start: startISO,
      end: endISO,
      interval: chartState.interval,
      symbol: chartState.symbol,
      datasource: chartState?.datasource,
      exchange: chartState?.exchange,
      instrument_id: chartState?.instrument_id,
    };

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
          return payload ? { ind_id: ind.id, type: ind.type, payload } : null;
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
  }, [fetchAndSyncIndicators, refreshEnabledOverlays, logError]);

  // Handlers for modal save/delete
  const handleSave = async (meta) => {
    const core = stripRuntimeParams(normalizeParams(meta.params));

    if ('lookbacks' in core) {
      if (!Array.isArray(core.lookbacks) || core.lookbacks.length === 0) {
        setError('Lookbacks must be a comma/space-separated list of integers, e.g., "5, 10, 20".');
        return;
      }
    }

    const params = { ...core };

    setError(null);
    setModalOpen(false);
    setEditing(null);

    try {
      let indicatorId = meta.id ?? null;
      let needsIndicatorUpdate = true;

      // Check if only signal rules changed (not indicator params)
      if (meta.id) {
        const existing = indicators.find((i) => i.id === meta.id);
        if (existing) {
          // Compare core params (without runtime params like symbol/interval/start/end)
          const existingCore = stripRuntimeParams(existing.params || {});
          const coreParamsChanged = JSON.stringify(existingCore) !== JSON.stringify(core);
          const nameChanged = meta.name !== existing.name;

          // If only signalRules changed, don't update indicator
          needsIndicatorUpdate = coreParamsChanged || nameChanged;
        }
      }

      if (needsIndicatorUpdate) {
        setIsLoading(true);
        updateChart(chartId, { overlays: [], overlayLoading: true });

        if (meta.id) {
          const existing = indicators.find((i) => i.id === meta.id) || null;
          const payload = await updateIndicator(meta.id, {
            type: meta.type,
            params,
            name: meta.name,
            color: existing?.color ?? null,
          });
          indicatorId = payload?.id ?? meta.id;
        } else {
          const created = await createIndicator({ type: meta.type, params, name: meta.name });
          indicatorId = created?.id ?? null;
        }
      } else {
        indicatorId = meta.id;
      }

      if (indicatorId) {
        const ruleSelection = Array.isArray(meta.signalRules) ? meta.signalRules : null;
        const currentConfig = getChart(chartId)?.signalsConfig || {};
        const currentEnabled = currentConfig.enabledRules || {};
        const nextEnabled = { ...currentEnabled };
        if (ruleSelection && ruleSelection.length) {
          nextEnabled[indicatorId] = ruleSelection;
        }
        const nextSignalsConfig = {
          ...currentConfig,
          enabledRules: nextEnabled,
        };
        updateChart(chartId, { signalsConfig: nextSignalsConfig });
      }

      // Only refetch and refresh overlays if indicator params actually changed
      // Signal rule changes are client-side only (stored in chart state)
      if (needsIndicatorUpdate) {
        const latest = await fetchAndSyncIndicators({ silent: false });
        await refreshEnabledOverlays(latest);
      }
    } catch (e) {
      setError(e.message);
      logError('indicator_save_failed', e);
      updateChart(chartId, { overlayLoading: false });
    } finally {
      setIsLoading(false);
    }
  };

  const handleDelete = async (id) => {
    if (!id) return;
    setIsLoading(true);
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
    } catch (e) {
      setError(e.message);
      logError('indicator_delete_failed', e);
    } finally {
      setIsLoading(false);
    }
  }

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
    try {
      setBulkActionLoading(true)
      setIsLoading(true)
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
    }
  }

  const handleBulkToggle = async (enabled) => {
    const ids = Array.from(selectedIds || [])
    if (!ids.length) return
    try {
      setBulkActionLoading(true)
      await bulkToggleIndicators(ids, enabled)
      const latest = await fetchAndSyncIndicators({ silent: true })
      await refreshEnabledOverlays(latest)
    } catch (e) {
      setError(e.message)
      logError('indicator_bulk_toggle_failed', e)
    } finally {
      setBulkActionLoading(false)
    }
  }

  const toggleEnable = (id) => {
    const target = indicators.find((indicator) => indicator.id === id)
    if (!target) return
    const previousEnabled = !!target.enabled
    const nextEnabled = !previousEnabled

    setIndicators((prev) => {
      const next = sortIndicators(
        prev.map((indicator) =>
          indicator.id === id ? { ...indicator, enabled: nextEnabled } : indicator,
        ),
      )
      updateChart(chartId, { indicators: next })
      queueMicrotask(() => { void refreshEnabledOverlays(next) })
      return next
    })

    setIndicatorEnabled(id, nextEnabled)
      .then(async () => {
        const latest = await fetchAndSyncIndicators({ silent: true })
        await refreshEnabledOverlays(latest)
      })
      .catch((err) => {
        setError(err.message)
        logError('indicator_toggle_failed', err)
        setIndicators((prev) => {
          const next = sortIndicators(
            prev.map((indicator) =>
              indicator.id === id ? { ...indicator, enabled: previousEnabled } : indicator,
            ),
          )
          updateChart(chartId, { indicators: next })
          return next
        })
      })
  }

  // Regenerate signals
  const generateSignals = async (id) => {
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
    try {
      setDuplicateBusyId(id)
      await duplicateIndicator(id)
      const latest = await fetchAndSyncIndicators({ silent: true })
      await refreshEnabledOverlays(latest)
    } catch (e) {
      setError(e.message)
      logError('indicator_duplicate_failed', e)
    } finally {
      setDuplicateBusyId(null)
    }
  }

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

      {/* Indicators section */}
      <section className="border-b border-slate-800">
        <div className="flex flex-col gap-4 px-5 py-4">
          {/* Header with title and actions */}
          <div className="flex items-center justify-between gap-4">
            <div className="flex-1">
              <h3 className="text-sm font-semibold text-slate-100">Indicators</h3>
              <p className="text-xs text-slate-500 mt-1">Manage overlay configurations</p>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              <button
                type="button"
                onClick={handleRefreshList}
                disabled={refreshingList}
                className={`inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold transition ${
                  refreshingList
                    ? 'text-slate-500 cursor-wait'
                    : 'text-slate-300 hover:text-slate-100'
                }`}
              >
                <RefreshCw className={`size-3.5 ${refreshingList ? 'animate-spin' : ''}`} aria-hidden="true" />
                Refresh
              </button>
              <button
                type="button"
                onClick={() => openEditModal()}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold text-emerald-400 hover:text-emerald-300 transition"
              >
                <Plus className="size-3.5" aria-hidden="true" />
                Add
              </button>
            </div>
          </div>

          {/* Filters - improved visibility */}
          <div className="flex items-center gap-4 flex-wrap">
            <div className="flex items-center gap-3 text-[11px] font-mono">
              <span className="text-slate-300 font-semibold">{enabledCount}</span>
              <span className="text-slate-600">enabled</span>
              <span className="text-slate-700">•</span>
              <span className="text-slate-300 font-semibold">{totalCount}</span>
              <span className="text-slate-600">total</span>
            </div>
            
            <label className="flex items-center gap-2 text-[11px] text-slate-300 hover:text-slate-100 transition cursor-pointer">
              <input
                type="checkbox"
                className="size-3.5 accent-emerald-500 cursor-pointer"
                checked={showEnabledOnly}
                onChange={(event) => setShowEnabledOnly(event.target.checked)}
              />
              <span className="font-medium">enabled only</span>
            </label>
            
            <select
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
              className="bg-transparent border-b border-slate-700 hover:border-slate-600 focus:border-emerald-500 focus:outline-none text-slate-200 text-[11px] font-medium cursor-pointer px-0 py-1 transition"
            >
              <option value="all">all types</option>
              {typeOptions.map((type) => (
                <option key={type} value={type}>{formatIndicatorType(type)}</option>
              ))}
            </select>
            
            {trimmedSearchQuery && (
              <span className="text-[11px] text-slate-500 font-mono">search: <span className="text-slate-300 font-medium">{trimmedSearchQuery}</span></span>
            )}
          </div>
        </div>

        {/* Search box - more visible */}
        <div className="border-t border-slate-800 px-5 py-3">
          <input
            type="text"
            placeholder="Search indicators..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full bg-transparent border-0 border-b-2 border-slate-700 hover:border-slate-600 focus:border-emerald-500 focus:outline-none text-sm font-medium text-slate-100 placeholder-slate-500 pb-2 transition"
          />
        </div>
      </section>

      {/* Indicators list */}
      <LoadingOverlay show={isLoading} message="Loading indicators…" />
      <div className={`transition ${isLoading ? 'pointer-events-none select-none blur-sm opacity-40' : 'opacity-100'}`}>
        {/* Bulk actions - more visible */}
        {selectedIds.size > 0 && (
          <div className="mb-4 flex items-center justify-between border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 rounded">
            <span className="text-sm font-mono text-emerald-300 font-semibold">{selectedIds.size} selected</span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                disabled={bulkActionLoading}
                onClick={handleBulkToggle}
                className={`px-3 py-1 text-xs font-semibold rounded transition ${
                  bulkActionLoading
                    ? 'text-slate-500 cursor-not-allowed'
                    : 'text-slate-200 hover:text-slate-100 hover:bg-slate-700'
                }`}
              >
                Toggle
              </button>
              <button
                type="button"
                disabled={bulkActionLoading}
                onClick={handleBulkDelete}
                className={`px-3 py-1 text-xs font-semibold rounded transition ${
                  bulkActionLoading
                    ? 'text-slate-500 cursor-not-allowed'
                    : 'text-rose-300 hover:text-rose-200 hover:bg-rose-500/20'
                }`}
              >
                Delete
              </button>
            </div>
          </div>
        )}

        {/* Indicators list */}
        <div className="space-y-2">
          {paginatedIndicators.map(indicator => {
            const isGenerating = isSignalsLoading && signalsLoadingFor === indicator.id
            const disableSignals = isSignalsLoading && signalsLoadingFor !== indicator.id
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
                colorSwatches={COLOR_SWATCHES}
                isGeneratingSignals={isGenerating}
                disableSignalAction={disableSignals}
                selected={isSelected}
                onSelectionToggle={() => toggleIndicatorSelection(indicator.id)}
                duplicatePending={duplicateBusyId === indicator.id}
              />
            )
          })}

          {!isLoading && paginatedIndicators.length === 0 && (
            <div className="rounded-lg border border-dashed border-slate-800 bg-slate-900/20 px-4 py-6 text-center text-sm text-slate-500">
              {noIndicatorsMessage}
            </div>
          )}
        </div>

        {/* Pagination - more visible */}
        {filteredCount > pageSize && (
          <nav className="mt-4 flex items-center justify-between gap-3 text-xs text-slate-300 font-mono" aria-label="Pagination">
            <span className="font-medium">Page <span className="text-emerald-400">{currentPage}</span> of {totalPages}</span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setCurrentPage((prev) => Math.max(prev - 1, 1))}
                disabled={currentPage === 1}
                className={`px-3 py-1 text-xs font-semibold rounded transition ${
                  currentPage === 1
                    ? 'text-slate-600 cursor-not-allowed'
                    : 'text-slate-300 hover:text-slate-100 hover:bg-slate-800'
                }`}
              >
                ← Prev
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
                    className={`px-2.5 py-1 text-xs font-semibold rounded transition ${
                      isActive
                        ? 'text-emerald-400 bg-emerald-500/20 border border-emerald-500/50'
                        : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'
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
                className={`px-3 py-1 text-xs font-semibold rounded transition ${
                  currentPage === totalPages
                    ? 'text-slate-600 cursor-not-allowed'
                    : 'text-slate-300 hover:text-slate-100 hover:bg-slate-800'
                }`}
              >
                Next →
              </button>
            </div>
          </nav>
        )}
      </div>

      <IndicatorModal
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        initial={editing}
        onSave={handleSave}
        error={error}
      />
    </div>
  )
}
