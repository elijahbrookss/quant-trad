import { DateRangePickerComponent } from '../../ChartComponent/DateTimePickerComponent.jsx'

export function BacktestRangeField({ start, end, onChange }) {
  return (
    <div className="flex flex-col gap-2">
      <span className="text-[11px] uppercase tracking-[0.3em] text-slate-400">Backtest range</span>
      <DateRangePickerComponent
        className="rounded-xl border border-white/10 bg-[#0f1524]"
        dateRange={[start ? new Date(start) : undefined, end ? new Date(end) : undefined]}
        setDateRange={onChange}
      />
      <p className="text-[11px] text-slate-500">Provide start/end dates to walk through history.</p>
    </div>
  )
}
