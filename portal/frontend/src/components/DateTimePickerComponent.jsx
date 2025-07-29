import { useEffect, useRef, useState } from "react";
// import DateRangePicker from 'flowbite-datepicker';
import "flatpickr/dist/themes/dark.css";
import Flatpickr from "react-flatpickr";

export function DateRangePickerComponent() {
  const [startDate, setStartDate] = useState([new Date(), new Date()]);
  const [endDate, setEndDate] = useState([new Date(), new Date()]);

  useEffect(() => {
    // Set default date range to last 45 days
    const defaultStart = new Date(
      Date.now() - 45 * 24 * 60 * 60 * 1000
    ).toISOString().split("T")[0];
    const defaultEnd = new Date();
    setStartDate(defaultStart);
    setEndDate(defaultEnd);
  }, []);

  const datePickerRef = useRef(null);

	return (
    <div className="flex items-center space-x-3">
      <div className="flex flex-col">
        <span className="text-sm text-neutral-500"> Start Date/Time </span>
        <Flatpickr
          ref={datePickerRef}
          data-enable-time
          value={startDate}
          onChange={setStartDate}
          options={{
            dateFormat: "Y-m-d H:i",
            time_24hr: true,
            enableTime: true,
            minuteIncrement: 1,
            maxDate: "today",
            minDate: "2020-01-01",
            altInput: true,
            altFormat: "Y-m-d H:i",
            allowInput: true,
          }}
          className ="w-fit bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </div> 
      <span className="text-neutral-400"> to </span>
      <div className="flex flex-col">
        <span className="text-sm text-neutral-500"> End Date/Time </span>
        <Flatpickr
          ref={datePickerRef}
          data-enable-time
          value={endDate}
          onChange={setEndDate}
          className ="w-fit bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </div> 
    </div>
	)
}