import { useCallback, useEffect, useMemo, useState } from 'react';
import { SYMBOL_GROUPS } from '../data/symbol-presets';

const FAV_KEY = 'qt.symbolFavorites';
const DEFAULT_FAVORITES = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'LINK/USDT'];

const normalizeSymbol = (value) => (value ?? '').toString().trim().toUpperCase();

const buildPickPayload = (item) => {
  const symbol = normalizeSymbol(item?.symbol ?? item?.s);
  if (!symbol) return null;
  const timeframe = (item?.timeframe ?? item?.interval ?? '').toString().trim();
  const datasource = (item?.datasource ?? '').toString().trim().toUpperCase();
  const exchange = (item?.exchange ?? '').toString().trim().toUpperCase();
  return {
    symbol,
    timeframe: timeframe || undefined,
    datasource: datasource || undefined,
    exchange: exchange || undefined,
  };
};

const hasLocalStorage = () => {
  try {
    return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
  } catch {
    return false;
  }
};

const loadFavs = () => {
  if (!hasLocalStorage()) {
    return [...DEFAULT_FAVORITES];
  }

  try {
    const raw = JSON.parse(window.localStorage.getItem(FAV_KEY) || '[]');
    if (Array.isArray(raw) && raw.length) {
      return [...new Set(raw.map(normalizeSymbol))];
    }
  } catch {
    // ignore parsing/storage errors and fall back to defaults
  }

  return [...DEFAULT_FAVORITES];
};

const saveFavs = (arr) => {
  if (!hasLocalStorage()) {
    return;
  }

  try {
    window.localStorage.setItem(FAV_KEY, JSON.stringify(arr));
  } catch {
    // ignore persistence issues (private browsing, etc.)
  }
};

export default function SymbolPalette({ open, onClose, onPick }) {
  const [q, setQ] = useState('');
  const [favs, setFavs] = useState(() => loadFavs());

  useEffect(() => { if (!open) setQ(''); }, [open]);

  const flat = useMemo(
    () => SYMBOL_GROUPS.flatMap((group) =>
      group.items.map((item) => ({
        ...item,
        group: group.title,
        symbol: normalizeSymbol(item.symbol ?? item.s),
      })),
    ),
    [],
  );
  const results = useMemo(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return flat;
    return flat.filter(x =>
      x.s.toLowerCase().includes(needle) ||
      x.name.toLowerCase().includes(needle) ||
      x.group.toLowerCase().includes(needle) ||
      x.note.toLowerCase().includes(needle)
    );
  }, [q, flat]);

  const isFav = (s) => favs.includes(normalizeSymbol(s));
  const toggleFav = (s) => {
    const symbol = normalizeSymbol(s);
    setFavs((prev) => {
      const next = isFav(symbol)
        ? prev.filter(x => x !== symbol)
        : [...new Set([symbol, ...prev])].slice(0, 12);
      saveFavs(next);
      return next;
    });
  };

  const emitPick = useCallback((item) => {
    const payload = buildPickPayload(item);
    if (!payload) return;
    onPick?.(payload);
  }, [onPick]);

  const handleDirectPick = () => {
    const trimmed = normalizeSymbol(q);
    if (!trimmed) return;
    const matched = results.find((x) => normalizeSymbol(x.s) === trimmed);
    if (matched) {
      emitPick(matched);
    } else {
      emitPick({ s: trimmed });
    }
    onClose?.();
  };

  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-40 grid place-items-center bg-[#04060d]/80 px-4 backdrop-blur-xl"
      onClick={onClose}
    >
      <div
        className="relative w-full max-w-[1120px] overflow-hidden rounded-[26px] border border-white/12 bg-gradient-to-br from-[#080b14]/95 via-[#060a12]/95 to-[#04060c]/95 shadow-[0_60px_160px_-90px_rgba(0,0,0,0.9)]"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex flex-col gap-1.5 border-b border-white/10 bg-[#0b1324]/70 px-6 py-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-base font-semibold tracking-tight text-slate-100">Symbol library</h2>
            <p className="text-sm text-slate-400">
              Search curated presets or enter any market ticker manually.
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-4 py-1.5 text-xs font-semibold uppercase tracking-[0.3em] text-slate-200 transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-18)] hover:text-[color:var(--accent-text-strong)]"
          >
            Close
          </button>
        </header>

        <div className="flex flex-col gap-4 px-6 py-4">
          <div className="flex flex-col gap-2.5 rounded-2xl border border-white/12 bg-[#0b1324]/70 px-4 py-4 shadow-inner shadow-black/20">
            <label className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">
              Find a symbol
            </label>
            <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
              <input
                autoFocus
                value={q}
                onChange={(e) => setQ(e.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    event.preventDefault();
                    handleDirectPick();
                  }
                }}
                placeholder="Search symbols, sectors, or type LINK/USDT"
                className="flex-1 rounded-xl border border-white/12 bg-[#050912]/80 px-3 py-2 text-sm text-slate-100 placeholder:text-slate-500 outline-none focus:border-[color:var(--accent-alpha-40)] focus:ring-1 focus:ring-[color:var(--accent-ring)]"
              />
              <button
                type="button"
                onClick={handleDirectPick}
                className="inline-flex items-center justify-center rounded-xl border border-white/12 bg-[color:var(--accent-alpha-15)] px-4 py-2 text-[11px] font-semibold uppercase tracking-[0.32em] text-[color:var(--accent-text-strong)] transition hover:border-[color:var(--accent-alpha-40)] hover:bg-[color:var(--accent-alpha-25)]"
              >
                Use symbol
              </button>
            </div>
            <p className="text-[11px] text-slate-500">
              Favorites sync locally so your go-to markets stay within reach.
            </p>
          </div>

          {!!favs.length && (
            <div className="rounded-2xl border border-white/8 bg-[#090f1c]/60 p-4">
              <div className="mb-3 flex items-center justify-between">
                <span className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400/80">Favorites</span>
                <span className="text-[11px] text-slate-500">Pinned locally</span>
              </div>
              <div className="flex max-h-[32vh] flex-col gap-2 overflow-auto pr-1">
                {favs.map((s) => {
                  const normalized = normalizeSymbol(s);
                  const preset = flat.find((i) => normalizeSymbol(i.s) === normalized);
                  const entry =
                    preset || {
                      s: normalized,
                      symbol: normalized,
                      name: 'Custom symbol',
                      note: 'Manual entry',
                      group: 'Favorites',
                      edge: '',
                    };
                  return (
                    <Row key={`fav-${normalized}`} x={entry} onPick={emitPick} onFav={toggleFav} fav />
                  );
                })}
              </div>
            </div>
          )}

          <div className="rounded-2xl border border-white/8 bg-[#090f1c]/60 p-4">
            <div className="mb-3 flex items-center justify-between">
              <span className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-400/80">All presets</span>
              <span className="text-[11px] text-slate-500">{results.length} matches</span>
            </div>
            <div className="flex max-h-[45vh] flex-col gap-2 overflow-auto pr-1">
              {results.map((x) => (
                <Row key={`${x.group}-${x.s}`} x={x} onPick={emitPick} onFav={toggleFav} fav={isFav(x.s)} />
              ))}
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2 rounded-2xl border border-white/8 bg-[#080d19]/60 px-4 py-3 text-[11px] text-slate-500">
            <span>
              Tip: press{' '}
              <kbd className="rounded border border-white/15 bg-white/5 px-1 py-0.5 text-[10px] text-slate-200">/</kbd> to open the palette anywhere
            </span>
            <span className="text-slate-500/80">Favorites are stored only on this device.</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function Row({ x, onPick, onFav, fav }) {
  return (
    <div className="group flex w-full items-center justify-between gap-3 rounded-2xl border border-white/8 bg-[#0b1324]/70 px-4 py-3 transition hover:border-[color:var(--accent-alpha-35)] hover:bg-[#101d34]">
      <button
        onClick={() => onPick?.(x)}
        className="flex-1 text-left"
      >
        <div className="font-semibold tracking-tight text-slate-100">
          {x.s}{' '}
          <span className="text-sm font-normal text-slate-400">· {x.name}</span>
        </div>
        <div className="text-[11px] text-slate-400">
          <span className="uppercase tracking-[0.24em] text-slate-500">{x.group}</span> — {x.note}
          {x.edge ? <span className="text-slate-500"> · Edge:</span> : null} {x.edge}
        </div>
      </button>
      <button
        onClick={() => onFav(x.s)}
        className={`ml-2 mt-1 flex h-8 w-8 items-center justify-center rounded-full border text-sm transition ${
          fav
            ? 'border-amber-400 bg-amber-400/20 text-amber-300'
            : 'border-white/12 bg-white/5 text-slate-300 group-hover:border-[color:var(--accent-alpha-35)] group-hover:text-[color:var(--accent-text-soft)]'
        }`}
        title={fav ? 'Unfavorite' : 'Favorite'}
      >
        ★
      </button>
    </div>
  );
}
