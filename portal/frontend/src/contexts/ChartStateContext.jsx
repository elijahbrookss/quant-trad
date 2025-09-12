// src/contexts/ChartStateContext.jsx
import { createContext, useContext, useReducer, useMemo, useCallback } from 'react';
import { createLogger } from '../utils/logger.js';

const LOG_NS = 'ChartStateContext'; // file namespace

// State shape: { [chartId]: { ...chartData, handles?, _version? } }
function reducer(state, action) {
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
      // only update if at least one patched field changes
      let changed = false;
      for (const k of Object.keys(patch)) {
        if (curr[k] !== patch[k]) { changed = true; break; }
      }
      if (!changed) return state; // no-op
      return { ...state, [id]: { ...curr, ...patch } };
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

  // actions are stable; no effects that set state here
  const registerChart = useCallback((id, handles) => {
    info('register', { id });
    dispatch({ type: 'REGISTER', id, handles });
  }, [info]);

  const updateChart = useCallback((id, patch) => {
    debug('update', { id, keys: Object.keys(patch) });
    dispatch({ type: 'UPDATE', id, patch });
  }, [debug]);

  const bumpRefresh = useCallback((id) => {
    debug('bump', { id });
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
