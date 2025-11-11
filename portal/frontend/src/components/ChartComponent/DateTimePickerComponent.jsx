import { useEffect, useRef } from "react";
import "flatpickr/dist/themes/dark.css";
import Flatpickr from "react-flatpickr";

export function DateRangePickerComponent({
  dateRange,
  setDateRange,
  defaultStart,
  defaultEnd,
  disabled = false,
}) {
  const today = new Date();
  const fortyFiveDaysAgo = new Date();
  fortyFiveDaysAgo.setDate(today.getDate() - 45);
  today.setMinutes(today.getMinutes() - 5);

  const datePickerRef = useRef(null);

  // Default values fallback if not passed
  const [startDate, endDate] = dateRange ?? [defaultStart ?? fortyFiveDaysAgo, defaultEnd ?? today];

  useEffect(() => {
    if (!startDate || !endDate) return;
    if (startDate > endDate) setDateRange([startDate, startDate]);
  }, [startDate, endDate, setDateRange]);

  return (
    <div className="flex min-w-[19rem] flex-col gap-3">
      <span className="text-[11px] font-medium uppercase tracking-[0.28em] text-slate-400/80">Date range</span>
      <div className="flex flex-wrap items-end gap-4">
        <div className="flex flex-col gap-1.5">
          <span className="text-xs uppercase tracking-[0.2em] text-slate-500">Start</span>
          <Flatpickr
            id="startDatePicker"
            ref={datePickerRef}
            value={startDate}
            onChange={([date]) => {
              if (disabled) return;
              setDateRange([date, endDate]);
            }}
            options={{
              dateFormat: "Y-m-d H:i",
              maxDate: "today",
              minDate: "2020-01-01",
              altInput: true,
              altFormat: "Y-m-d H:i",
              allowInput: true,
              enableTime: true,
            }}
            disabled={disabled}
            className={`w-48 rounded-lg border px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none focus:ring-1 focus:ring-[color:var(--accent-ring)] ${
              disabled
                ? 'cursor-not-allowed border-white/10 bg-[#090d16]/40 text-slate-500'
                : 'border-white/12 bg-[#0b1324]/90'
            }`}
          />
        </div>

        <div className="mb-1 flex h-8 items-center justify-center rounded-full border border-white/12 bg-[#0b1324]/70 px-3 text-xs uppercase tracking-[0.28em] text-slate-300">
          to
        </div>

        <div className="flex flex-col gap-1.5">
          <span className="text-xs uppercase tracking-[0.2em] text-slate-500">End</span>
          <Flatpickr
            id="endDatePicker"
            ref={datePickerRef}
            value={endDate}
            onChange={([date]) => {
              if (disabled) return;
              setDateRange([startDate, date]);
            }}
            options={{
              dateFormat: "Y-m-d H:i",
              minDate: "2020-01-01",
              maxDate: new Date(Date.now() + 5 * 60),
              altInput: true,
              altFormat: "Y-m-d H:i",
              allowInput: true,
              enableTime: true,
            }}
            disabled={disabled}
            className={`w-48 rounded-lg border px-3 py-2 text-sm text-slate-100 focus:border-[color:var(--accent-alpha-40)] focus:outline-none focus:ring-1 focus:ring-[color:var(--accent-ring)] ${
              disabled
                ? 'cursor-not-allowed border-white/10 bg-[#090d16]/40 text-slate-500'
                : 'border-white/12 bg-[#0b1324]/90'
            }`}
          />
        </div>
      </div>
    </div>
  );
}
