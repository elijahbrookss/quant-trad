import { useEffect, useRef } from "react";
import "flatpickr/dist/themes/dark.css";
import Flatpickr from "react-flatpickr";

export function DateRangePickerComponent({
  dateRange,
  setDateRange,
  defaultStart,
  defaultEnd,
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
  }, [startDate, endDate]);

  return (
    <div className="flex flex-col gap-2 min-w-[19rem]">
      <span className="text-[11px] uppercase tracking-[0.2em] text-neutral-400">Date range</span>
      <div className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1">
          <span className="text-xs text-neutral-500">Start</span>
          <Flatpickr
            id="startDatePicker"
            ref={datePickerRef}
            value={startDate}
            onChange={([date]) => setDateRange([date, endDate])}
            options={{
              dateFormat: "Y-m-d H:i",
              maxDate: "today",
              minDate: "2020-01-01",
              altInput: true,
              altFormat: "Y-m-d H:i",
              allowInput: true,
              enableTime: true,
            }}
            className="w-48 rounded-lg border border-neutral-700/70 bg-neutral-900/70 px-3 py-2 text-sm text-neutral-100 shadow-inner shadow-black/30 focus:border-sky-400 focus:outline-none focus:ring-2 focus:ring-sky-500/50"
          />
        </div>

        <div className="mb-1 flex h-8 items-center justify-center rounded-full border border-neutral-700/70 px-3 text-xs uppercase tracking-[0.2em] text-neutral-400">
          to
        </div>

        <div className="flex flex-col gap-1">
          <span className="text-xs text-neutral-500">End</span>
          <Flatpickr
            id="endDatePicker"
            ref={datePickerRef}
            value={endDate}
            onChange={([date]) => setDateRange([startDate, date])}
            options={{
              dateFormat: "Y-m-d H:i",
              minDate: "2020-01-01",
              maxDate: new Date(Date.now() + 5 * 60),
              altInput: true,
              altFormat: "Y-m-d H:i",
              allowInput: true,
              enableTime: true,
            }}
            className="w-48 rounded-lg border border-neutral-700/70 bg-neutral-900/70 px-3 py-2 text-sm text-neutral-100 shadow-inner shadow-black/30 focus:border-sky-400 focus:outline-none focus:ring-2 focus:ring-sky-500/50"
          />
        </div>
      </div>
    </div>
  );
}
