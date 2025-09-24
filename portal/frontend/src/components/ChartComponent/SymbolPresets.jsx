export default function SymbolPresets({ selected, onPick }) {
  const groups = [
    { title: 'Index ETFs', items: ['SPY', 'QQQ', 'IWM', 'DIA'] },
    { title: 'Metals',     items: ['GLD', 'SLV', 'PPLT', 'CPER'] },
    { title: 'Energy / Ag',items: ['USO', 'UNG', 'XLE', 'DBC', 'DBA'] },
  ];

  const Chip = ({ label, active, onClick }) => (
    <button
      onClick={() => onClick(label)}
      className={[
        'px-3 py-1 rounded-full text-xs transition-colors border',
        active
          ? 'border-purple-400/60 bg-purple-500/30 text-purple-100 shadow-[0_0_18px_rgba(168,85,247,0.25)]'
          : 'border-white/10 bg-white/5 text-slate-300 hover:bg-purple-500/10 hover:text-purple-100',
      ].join(' ')}
      title={`Load ${label}`}
    >
      <span className="align-middle font-medium tracking-wide">{label}</span>
    </button>
  );

  return (
    <div className="mt-1 flex flex-col gap-3">
      {groups.map(g => (
        <div key={g.title} className="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <span className="w-32 text-[11px] uppercase tracking-[0.3em] text-slate-500">{g.title}</span>
          <div className="flex flex-wrap gap-1.5">
            {g.items.map(sym => (
              <Chip key={sym} label={sym} active={selected === sym} onClick={onPick} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
