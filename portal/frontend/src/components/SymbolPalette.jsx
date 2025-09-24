import { useEffect, useMemo, useState } from 'react';
import { SYMBOL_GROUPS } from '../data/symbol-presets';

const FAV_KEY = 'qt.symbolFavorites';

const loadFavs = () => {
  try { return JSON.parse(localStorage.getItem(FAV_KEY) || '[]'); } catch { return []; }
};
const saveFavs = (arr) => { try { localStorage.setItem(FAV_KEY, JSON.stringify(arr)); } catch {} };

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
    <div className="fixed inset-0 z-40 bg-black/30 backdrop-blur-sm" onClick={onClose}>
      <div
        className="mx-auto mt-24 w-[780px] max-w-[94vw] rounded-2xl border border-zinc-200 bg-white shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-zinc-200 p-3">
          <input
            autoFocus
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search symbols or groups… try: metals, SPY, oil"
            onKeyDown={handleEnter}
            className="flex-1 rounded-lg border border-transparent bg-zinc-50 px-3 py-2 text-sm text-zinc-700 placeholder-zinc-400 outline-none transition focus:border-zinc-300 focus:bg-white"
          />
          <span className="text-xs text-zinc-400">Press Enter to select</span>
        </div>

        {!!favs.length && (
          <div className="max-h-[50vh] overflow-auto p-3 pt-2">
            <div className="mb-1 text-[11px] uppercase tracking-wide text-zinc-400">Favorites</div>
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

        <div className="flex justify-between border-t border-zinc-200 p-2 text-xs text-zinc-500">
          <span>Tip: press <kbd className="rounded border border-zinc-200 bg-zinc-50 px-1 py-0.5">/</kbd> to open anywhere</span>
          <button
            onClick={onClose}
            className="rounded border border-zinc-200 bg-zinc-50 px-2 py-1 text-zinc-600 transition hover:border-zinc-300 hover:bg-white hover:text-zinc-800"
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
    <div className="group flex w-full items-start justify-between rounded-lg px-3 py-2 transition hover:bg-zinc-100">
      <button onClick={() => onPick(x.s)} className="text-left">
        <div className="font-medium text-zinc-800">
          {x.s} <span className="text-zinc-400">· {x.name}</span>
        </div>
        <div className="text-xs text-zinc-500">
          <span className="text-zinc-400">{x.group}</span> — {x.note}
          {x.edge ? <span className="text-zinc-400"> · Edge: </span> : null}{x.edge}
        </div>
      </button>
      <button
        onClick={() => onFav(x.s)}
        className={[
          'ml-3 mt-1 flex h-6 w-6 items-center justify-center rounded-full border text-sm transition',
          fav ? 'border-amber-300 bg-amber-100 text-amber-600'
              : 'border-zinc-200 bg-zinc-50 text-zinc-400 group-hover:border-zinc-300 group-hover:text-zinc-600',
        ].join(' ')}
        title={fav ? 'Unfavorite' : 'Favorite'}
      >
        ★
      </button>
    </div>
  );
}
