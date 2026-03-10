// src/contexts/ChartStateContext.jsx
import { createContext, useContext, useReducer, useMemo, useCallback, useEffect, useRef } from 'react';
import { createLogger } from '../utils/logger.js';

const LOG_NS = 'ChartStateContext'; // file namespace

// State shape: { [chartId]: { ...chartData, handles?, _version? } }
function reducer(state, action) {
  const indicatorsEqual = (a = [], b = []) => {
    if (a === b) return true;
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i += 1) {
      const x = a[i]; const y = b[i];
      if (x === y) continue;
      if (!x || !y || typeof x !== 'object' || typeof y !== 'object') return false;
      if (x.id !== y.id) return false;
      if (x.enabled !== y.enabled) return false;
      if (x.type !== y.type) return false;
      if (x._status !== y._status) return false;
    }
    return true;
  };
  switch (action.type) {
    case 'REGISTER': {
      const { id, handles } = action;
      const curr = state[id] || {};
      if (curr.handles === handles) return state; // no-op
      return { ...state, [id]: { ...curr, handles } };
    }
    case 'UPDATE': {
      const { id, patch } = action;
      const curr = state[id] || {};
      const nextPatch = { ...patch };
      if (Object.prototype.hasOwnProperty.call(patch, 'indicators')) {
        if (indicatorsEqual(curr.indicators, patch.indicators)) {
          delete nextPatch.indicators; // avoid churn on identical indicator lists
        }
      }
      // only update if at least one patched field changes
      let changed = false;
      for (const k of Object.keys(nextPatch)) {
        if (curr[k] !== nextPatch[k]) { changed = true; break; }
      }
      if (!changed) return state; // no-op
      return { ...state, [id]: { ...curr, ...nextPatch } };
    }
    case 'BUMP': {
      const { id } = action;
      const curr = state[id] || {};
      const v = (curr._version || 0) + 1;
      return { ...state, [id]: { ...curr, _version: v } };
    }
    default:
      return state;
  }
}

const ChartCtx = createContext(null);

export function ChartStateProvider({ children }) {
  const { debug, info } = useMemo(() => createLogger(LOG_NS), []);
  const [charts, dispatch] = useReducer(reducer, {});
  const chartsRef = useRef(charts);
  const registerSeqRef = useRef(0);
  const handleIdsRef = useRef(new WeakMap());

  useEffect(() => {
    chartsRef.current = charts;
  }, [charts]);

  // actions are stable; no effects that set state here
  const registerChart = useCallback((id, handles, meta = {}) => {
    const existingHandles = chartsRef.current?.[id]?.handles;
    if (existingHandles === handles) return; // avoid duplicate logs/dispatches

    registerSeqRef.current += 1;
    const nextSeq = registerSeqRef.current;
    const getHandleId = (handle) => {
      if (!handle) return 'null';
      const map = handleIdsRef.current;
      if (map.has(handle)) return map.get(handle);
      const nextId = `h${map.size + 1}`;
      map.set(handle, nextId);
      return nextId;
    };

    info('chart_register', {
      chartId: id,
      changed: Boolean(existingHandles),
      registerSeq: nextSeq,
      previousHandleId: getHandleId(existingHandles),
      nextHandleId: getHandleId(handles),
      caller: meta?.caller || 'unknown',
      lifecycleSeq: meta?.lifecycleSeq ?? null,
      mountId: meta?.mountId ?? null,
    });
    dispatch({ type: 'REGISTER', id, handles });
  }, [info]);

  const updateChart = useCallback((id, patch) => {
    debug('chart_update', { chartId: id, keys: Object.keys(patch) });
    dispatch({ type: 'UPDATE', id, patch });
  }, [debug]);

  const bumpRefresh = useCallback((id) => {
    debug('chart_bump', { chartId: id });
    dispatch({ type: 'BUMP', id });
  }, [debug]);

  const getChart = useCallback((id) => charts[id], [charts]);

  // context value is memoized to avoid churn
  const value = useMemo(() => ({
    charts,
    getChart,
    registerChart,
    updateChart,
    bumpRefresh,
  }), [charts, getChart, registerChart, updateChart, bumpRefresh]);

  return (
    <ChartCtx.Provider value={value}>
      {children}
    </ChartCtx.Provider>
  );
}

// returns actions + whole state if needed
export function useChartState() {
  const ctx = useContext(ChartCtx);
  if (!ctx) throw new Error('useChartState must be used within ChartStateProvider');
  return ctx;
}

// returns a single chart slice; simple selector
export function useChartValue(id) {
  const ctx = useChartState();
  return ctx.charts[id];
}
