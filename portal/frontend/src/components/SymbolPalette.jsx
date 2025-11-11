import { useEffect, useMemo, useState } from 'react';
import { SYMBOL_GROUPS } from '../data/symbol-presets';

const FAV_KEY = 'qt.symbolFavorites';
const DEFAULT_FAVORITES = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT', 'LINK/USDT'];

const normalizeSymbol = (value) => (value ?? '').toString().trim().toUpperCase();

const loadFavs = () => {
  try {
    const raw = JSON.parse(localStorage.getItem(FAV_KEY) || '[]');
    if (Array.isArray(raw) && raw.length) {
      return [...new Set(raw.map(normalizeSymbol))];
    }
  } catch {
    // ignore
  }
  return [...DEFAULT_FAVORITES];
};
const saveFavs = (arr) => {
  try { localStorage.setItem(FAV_KEY, JSON.stringify(arr)); } catch {
    // ignore persistence issues (private browsing, etc.)
  }
};

export default function SymbolPalette({ open, onClose, onPick }) {
  const [q, setQ] = useState('');
  const [favs, setFavs] = useState(loadFavs());

  useEffect(() => { if (!open) setQ(''); }, [open]);

  const flat = useMemo(() =>
    SYMBOL_GROUPS.flatMap(g => g.items.map(it => ({ ...it, group: g.title }))), []);
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

  const handleDirectPick = () => {
    const trimmed = normalizeSymbol(q);
    if (!trimmed) return;
    const matched = results.find((x) => normalizeSymbol(x.s) === trimmed);
    const symbol = matched ? matched.s : trimmed;
    onPick?.(symbol);
    onClose?.();
  };

  if (!open) return null;
  return (
    <div className="fixed inset-0 z-40 bg-black/50 backdrop-blur-sm" onClick={onClose}>
      <div
        className="mx-auto mt-24 w-[780px] max-w-[94vw] rounded-2xl bg-neutral-900/95 border border-neutral-700 shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-3 border-b border-neutral-700 flex items-center gap-2">
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
            placeholder="Search symbols or groups… try: metals, SPY, oil"
            className="flex-1 bg-neutral-800/70 text-neutral-100 placeholder-neutral-400 px-3 py-2 rounded-lg outline-none"
          />
          <button
            type="button"
            onClick={handleDirectPick}
            className="rounded-lg border border-neutral-700 bg-neutral-800 px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.25em] text-neutral-300 transition hover:border-neutral-500 hover:text-neutral-100"
          >
            Use symbol
          </button>
        </div>

        {!!favs.length && (
          <div className="max-h-[50vh] overflow-auto p-3 pt-2">
            <div className="text-[11px] uppercase tracking-wide text-neutral-400 mb-1">Favorites</div>
            {favs.map((s) => {
              const normalized = normalizeSymbol(s);
              const preset = flat.find(i => normalizeSymbol(i.s) === normalized);
              const entry = preset || {
                s: normalized,
                name: 'Custom symbol',
                note: 'Manual entry',
                group: 'Favorites',
                edge: '',
              };
              return (
                <Row key={`fav-${normalized}`} x={entry} onPick={onPick} onFav={toggleFav} fav />
              );
            })}
          </div>
        )}

        <div className="max-h-[60vh] overflow-auto p-3 pt-2">
          {results.map((x) => (
            <Row key={`${x.group}-${x.s}`} x={x} onPick={onPick} onFav={toggleFav} fav={isFav(x.s)} />
          ))}
        </div>

        <div className="p-2 text-xs text-neutral-500 flex justify-between border-t border-neutral-800">
          <span>Tip: press <kbd className="px-1 py-0.5 rounded bg-neutral-800 border border-neutral-700">/</kbd> to open anywhere</span>
          <button onClick={onClose}
                  className="px-2 py-1 rounded bg-neutral-800 border border-neutral-700 text-neutral-200">
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

function Row({ x, onPick, onFav, fav }) {
  return (
    <div className="group w-full px-3 py-2 rounded-lg hover:bg-neutral-800/60 transition flex justify-between items-start">
      <button onClick={() => onPick(x.s)} className="text-left">
        <div className="text-neutral-100 font-medium">
          {x.s} <span className="text-neutral-400">· {x.name}</span>
        </div>
        <div className="text-xs text-neutral-400">
          <span className="text-neutral-500">{x.group}</span> — {x.note}
          {x.edge ? <span className="text-neutral-500"> · Edge: </span> : null}{x.edge}
        </div>
      </button>
      <button
        onClick={() => onFav(x.s)}
        className={[
          'ml-3 mt-1 h-6 w-6 rounded-full border flex items-center justify-center',
          fav ? 'border-amber-400 bg-amber-500/20 text-amber-300'
              : 'border-neutral-600 bg-neutral-800/60 text-neutral-300 group-hover:text-neutral-100',
        ].join(' ')}
        title={fav ? 'Unfavorite' : 'Favorite'}
      >
        ★
      </button>
    </div>
  );
}
