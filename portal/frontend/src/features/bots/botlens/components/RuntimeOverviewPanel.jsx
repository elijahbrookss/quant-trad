import { BotLensPanel } from './BotLensPanel.jsx'

function DataRows({ rows }) {
  if (!rows.length) {
    return <div className="rounded-xl border border-dashed border-white/10 px-4 py-5 text-sm text-slate-400">No data.</div>
  }
  return (
    <div className="overflow-hidden rounded-xl border border-white/10">
      <table className="min-w-full text-left text-sm text-slate-200">
        <tbody className="divide-y divide-white/5">
          {rows.map((row) => (
            <tr key={row.key}>
              <td className="w-1/3 px-4 py-3 text-[11px] uppercase tracking-[0.24em] text-slate-500">{row.label}</td>
              <td className="px-4 py-3">{row.value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function RuntimeOverviewPanel({ model }) {
  return (
    <BotLensPanel
      eyebrow="Current State"
      title="Runtime overview"
      subtitle="These panels read only current run projection and selected-symbol base state. Retrieval history is rendered separately."
    >
      <div className="grid gap-4 xl:grid-cols-2">
        <div>
          <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Run projection</p>
          <DataRows rows={model.runRows} />
        </div>
        <div>
          <p className="mb-2 text-[10px] font-semibold uppercase tracking-[0.32em] text-slate-500">Selected symbol state</p>
          <DataRows rows={model.selectedRows} />
        </div>
      </div>
    </BotLensPanel>
  )
}
