import { useEffect, useRef, useState } from "react";
// import DateRangePicker from 'flowbite-datepicker';
import "flatpickr/dist/themes/dark.css";
import Flatpickr from "react-flatpickr";

export function DateRangePickerComponent() {
  const [date, setDate] = useState([new Date(), new Date()]);
  const datePickerRef = useRef(null);


	return (
    <div className="flex items-center space-x-3">
      <div className="flex flex-col">
        <span className="text-sm text-neutral-500"> Start date/time </span>
        <Flatpickr
          ref={datePickerRef}
          data-enable-time
          value={date}
          onChange={setDate}
          className ="w-fit bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </div> 
      <span className="text-neutral-400"> to </span>
      <div className="flex flex-col">
        <span className="text-sm text-neutral-500"> End date/time </span>
        <Flatpickr
          ref={datePickerRef}
          data-enable-time
          value={date}
          onChange={setDate}
          className ="w-fit bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 focus:outline-none focus:ring-2 focus:ring-indigo-500"
        />
      </div> 
    </div>
	)
}