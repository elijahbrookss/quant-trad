import { useEffect, useRef, useState } from "react";
import "flatpickr/dist/themes/dark.css";
import Flatpickr from "react-flatpickr";

export function DateRangePickerComponent() {
  const today = new Date();
  const fortyFiveDaysAgo = new Date();
  fortyFiveDaysAgo.setDate(today.getDate() - 45);

  // Set end date to today but 5 minutes before the current time
  today.setMinutes(today.getMinutes() - 5);

  const [startDate, setStartDate] = useState(fortyFiveDaysAgo);
  const [endDate, setEndDate] = useState(today);

  const datePickerRef = useRef(null);

  // Automatically fix invalid date ranges
  useEffect(() => {
    if (startDate > endDate) {
      setEndDate(startDate);
    }
  }, [startDate]);

  useEffect(() => {
    if (endDate < startDate) {
      setStartDate(endDate);
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
          onChange={([date]) => setStartDate(date)}
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
          onChange={([date]) => setEndDate(date)}
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
