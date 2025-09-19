// Minimal namespaced logger with levels.
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 50 };

let globalLevel = (import.meta?.env?.MODE === 'production') ? 'warn' : 'debug';
try { globalLevel = localStorage.getItem('LOG_LEVEL') || globalLevel; } catch {}

let sink = null; // Optional external sink.

export function setLogLevel(level) { if (LEVELS[level]) globalLevel = level; }
export function setLogSink(fn) { sink = (typeof fn === 'function') ? fn : null; }

export function createLogger(namespace = 'app') {
  const ts = () => new Date().toISOString();
  const allowed = (lvl) => LEVELS[lvl] >= LEVELS[globalLevel];
  const make = (lvl, method = 'log') => (...args) => {
    if (!allowed(lvl)) return;
    const prefix = `[${ts()}] [${namespace}]`;
    (console[method] || console.log)(prefix, ...args);
    if (sink) { try { sink(lvl, namespace, ...args); } catch {} }
  };
  return {
    debug: make('debug', 'log'),
    info:  make('info',  'info'),
    warn:  make('warn',  'warn'),
    error: make('error', 'error'),
  };
}
