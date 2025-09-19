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
        'px-2.5 py-1 rounded-full text-xs transition-colors',
        'border',
        active
          ? 'bg-blue-600/80 text-white border-blue-400 shadow-sm'
          : 'bg-neutral-800/70 text-neutral-200 border-neutral-600 hover:bg-neutral-700',
      ].join(' ')}
      title={`Load ${label}`}
    >
      <span className="align-middle">{label}</span>
    </button>
  );

  return (
    <div className="flex flex-col gap-2 mt-1">
      {groups.map(g => (
        <div key={g.title} className="flex items-center gap-2 flex-wrap">
          <span className="text-[11px] uppercase tracking-wide text-neutral-400 w-28">{g.title}</span>
          <div className="flex gap-1.5 flex-wrap">
            {g.items.map(sym => (
              <Chip key={sym} label={sym} active={selected === sym} onClick={onPick} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
