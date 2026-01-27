const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 50 };

const envLogLevel = (() => {
  const explicit = import.meta?.env?.VITE_LOG_LEVEL;
  if (explicit && LEVELS[explicit]) return explicit;
  if (import.meta?.env?.VITE_DEBUG_LOGS) return 'debug';
  return null;
})();

let globalLevel = envLogLevel ?? ((import.meta?.env?.MODE === 'production') ? 'warn' : 'debug');
try {
  const stored = typeof localStorage !== 'undefined' ? localStorage.getItem('LOG_LEVEL') : null;
  if (stored && LEVELS[stored]) globalLevel = stored;
} catch {
  /* ignore storage access issues */
}

let sink = null; // Optional external sink for piping logs elsewhere.

export function setLogLevel(level) {
  if (LEVELS[level]) globalLevel = level;
}

export function setLogSink(fn) {
  sink = (typeof fn === 'function') ? fn : null;
}

const formatValue = (value) => {
  if (value === undefined) return 'undefined';
  if (value === null) return 'null';
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (typeof value === 'string') {
    return /\s/.test(value) ? JSON.stringify(value) : value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const formatContext = (context) => {
  if (!context) return '';
  const entries = Object.entries(context)
    .filter(([, v]) => v !== undefined)
    .map(([k, v]) => `${k}=${formatValue(v)}`);
  return entries.length ? ` ${entries.join(' ')}` : '';
};

const normalizeArgs = (baseCtx, args) => {
  const [eventOrMessage, maybeCtx, ...rest] = args;
  const extras = [];
  let event = typeof eventOrMessage === 'string' ? eventOrMessage : 'log';
  let ctx = { ...baseCtx };

  const mergeCtx = (candidate) => {
    if (candidate && typeof candidate === 'object' && !Array.isArray(candidate) && !(candidate instanceof Error)) {
      ctx = { ...ctx, ...candidate };
      return true;
    }
    return false;
  };

  if (!mergeCtx(maybeCtx) && maybeCtx !== undefined) {
    extras.push(maybeCtx);
  }

  for (const item of rest) {
    if (!mergeCtx(item)) extras.push(item);
  }

  return { event, ctx, extras };
};

const makeFormatter = (namespace) => (level) => (event, ctx) => {
  const ts = new Date().toISOString();
  const contextStr = formatContext(ctx);
  const eventStr = typeof event === 'string' ? `event=${event}` : `event=${formatValue(event)}`;
  return `[${ts}] ${level.toUpperCase()} ${namespace} ${eventStr}${contextStr}`;
};

export function createLogger(namespace = 'app', baseContext = {}) {
  const allowed = (lvl) => LEVELS[lvl] >= LEVELS[globalLevel];
  const formatter = makeFormatter(namespace);

  const logWith = (lvl, method = 'log') => (...args) => {
    if (!allowed(lvl)) return;
    const { event, ctx, extras } = normalizeArgs(baseContext, args);
    const line = formatter(lvl)(event, ctx);
    const sinkPayload = { level: lvl, namespace, event, context: ctx, extras };
    try {
      (console[method] || console.log)(line, ...extras);
    } catch {
      // ignore console issues
    }
    if (sink) {
      try { sink(sinkPayload); } catch { /* ignore sink errors */ }
    }
  };

  return {
    debug: logWith('debug', 'debug'),
    info: logWith('info', 'info'),
    warn: logWith('warn', 'warn'),
    error: logWith('error', 'error'),
    child(extraCtx = {}) {
      return createLogger(namespace, { ...baseContext, ...extraCtx });
    },
  };
}
