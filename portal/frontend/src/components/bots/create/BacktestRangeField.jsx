import { DateRangePickerComponent } from '../../ChartComponent/DateTimePickerComponent.jsx'

export function BacktestRangeField({ start, end, onChange, compact = false }) {
  if (compact) {
    return (
      <DateRangePickerComponent
        className="rounded-md border border-slate-800 bg-slate-950/50"
        dateRange={[start ? new Date(start) : undefined, end ? new Date(end) : undefined]}
        setDateRange={onChange}
      />
    )
  }

  return (
    <div className="space-y-2">
      <label className="text-xs font-medium text-slate-400">Backtest Range</label>
      <DateRangePickerComponent
        className="rounded-lg border border-slate-800 bg-slate-950/50"
        dateRange={[start ? new Date(start) : undefined, end ? new Date(end) : undefined]}
        setDateRange={onChange}
      />
      <p className="text-xs text-slate-500">Historical date range for backtest execution</p>
    </div>
  )
}
