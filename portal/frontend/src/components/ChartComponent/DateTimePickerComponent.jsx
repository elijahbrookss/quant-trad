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

  // Auto-correct ranges
  useEffect(() => {
    if (startDate > endDate) {
      setDateRange([startDate, startDate]);
    }
  }, [startDate]);

  useEffect(() => {
    if (endDate < startDate) {
      setDateRange([endDate, endDate]);
    }
  }, [endDate]);

  return (
    <div className="flex items-center space-x-3">
      {/* Start Date Picker */}
      <div className="flex flex-col">
        <label htmlFor="startDatePicker" className="text-sm text-neutral-500">Start Date/Time</label>
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
          className="self-end w-fit bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </div>

      <div className="flex items-center">
        <span className="text-neutral-400 mt-4">to</span>
      </div>

      {/* End Date Picker */}
      <div className="flex flex-col">
        <label htmlFor="endDatePicker" className="text-sm text-neutral-500">End Date/Time</label>
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
          className="self-end w-fit bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </div>
    </div>
  );
}
