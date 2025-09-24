import { useEffect, useMemo, useState } from 'react';
import { SYMBOL_GROUPS } from '../data/symbol-presets';

const FAV_KEY = 'qt.symbolFavorites';

const loadFavs = () => {
  try { return JSON.parse(localStorage.getItem(FAV_KEY) || '[]'); } catch (_err) { return []; }
};
const saveFavs = (arr) => {
  try {
    localStorage.setItem(FAV_KEY, JSON.stringify(arr));
  } catch (_err) {
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

  const isFav = (s) => favs.includes(s);
  const toggleFav = (s) => {
    setFavs((prev) => {
      const next = isFav(s) ? prev.filter(x => x !== s) : [...new Set([s, ...prev])].slice(0, 12);
      saveFavs(next);
      return next;
    });
  };

  const handleEnter = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      const first = results[0];
      if (first) {
        onPick?.(first.s);
      }
    }
  };

  if (!open) return null;
  return (
    <div className="fixed inset-0 z-40 bg-black/60 backdrop-blur" onClick={onClose}>
      <div
        className="mx-auto mt-24 w-[780px] max-w-[94vw] rounded-2xl border border-neutral-800 bg-neutral-950/95 text-neutral-100 shadow-2xl backdrop-blur"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-neutral-800 p-3">
          <input
            autoFocus
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search symbols or groups… try: metals, SPY, oil"
            onKeyDown={handleEnter}
            className="flex-1 rounded-lg border border-transparent bg-neutral-900 px-3 py-2 text-sm text-neutral-100 placeholder-neutral-500 outline-none transition focus:border-neutral-700 focus:bg-neutral-900/80"
          />
          <span className="text-xs text-neutral-500">Press Enter to select</span>
        </div>

        {!!favs.length && (
          <div className="max-h-[50vh] overflow-auto p-3 pt-2">
            <div className="mb-1 text-[11px] uppercase tracking-wide text-neutral-500">Favorites</div>
            {favs.map(s => {
              const x = flat.find(i => i.s === s);
              if (!x) return null;
              return (
                <Row key={`fav-${s}`} x={x} onPick={onPick} onFav={toggleFav} fav />
              );
            })}
          </div>
        )}

        <div className="max-h-[60vh] overflow-auto p-3 pt-2">
          {results.map((x) => (
            <Row key={`${x.group}-${x.s}`} x={x} onPick={onPick} onFav={toggleFav} fav={isFav(x.s)} />
          ))}
        </div>

        <div className="flex justify-between border-t border-neutral-800 p-2 text-xs text-neutral-500">
          <span>Tip: press <kbd className="rounded border border-neutral-700 bg-neutral-900 px-1 py-0.5 text-neutral-300">/</kbd> to open anywhere</span>
          <button
            onClick={onClose}
            className="rounded border border-neutral-700 bg-neutral-900 px-2 py-1 text-neutral-300 transition hover:border-neutral-500 hover:text-neutral-100"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

function Row({ x, onPick, onFav, fav }) {
  return (
    <div className="group flex w-full items-start justify-between rounded-lg px-3 py-2 transition hover:bg-neutral-900">
      <button onClick={() => onPick(x.s)} className="text-left">
        <div className="font-medium text-neutral-100">
          {x.s} <span className="text-neutral-500">· {x.name}</span>
        </div>
        <div className="text-xs text-neutral-400">
          <span className="text-neutral-500">{x.group}</span> — {x.note}
          {x.edge ? <span className="text-neutral-500"> · Edge: </span> : null}{x.edge}
        </div>
      </button>
      <button
        onClick={() => onFav(x.s)}
        className={[
          'ml-3 mt-1 flex h-6 w-6 items-center justify-center rounded-full border text-sm transition',
          fav ? 'border-amber-500 bg-amber-500/20 text-amber-300'
              : 'border-neutral-700 bg-neutral-900 text-neutral-500 group-hover:border-neutral-500 group-hover:text-neutral-200',
        ].join(' ')}
        title={fav ? 'Unfavorite' : 'Favorite'}
      >
        ★
      </button>
    </div>
  );
}
