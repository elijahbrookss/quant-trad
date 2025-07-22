import { useEffect, useRef, useState } from "react";

/**
 * DateRangePicker
 * 
 * Props:
 * @param {string|null} defaultStartDate - ISO string (YYYY-MM-DD) initial start
 * @param {string|null} defaultEndDate   - ISO string (YYYY-MM-DD) initial end
 * @param {boolean}   defaultOpen        - whether calendar is open by default
 * @param {function}  onApply            - called with (startDate, endDate) when user applies
 * @param {function}  onCancel           - called when user cancels selection
 * @param {string}    label              - Label text for the input
 */
export function DateRangePicker({
  defaultStartDate = null,
  defaultEndDate = null,
  defaultOpen = false,
  onApply = () => {},
  onCancel = () => {},
}) {
  const [currentDate, setCurrentDate] = useState(new Date());
  const [selectedStartDate, setSelectedStartDate] = useState(defaultStartDate);
  const [selectedEndDate, setSelectedEndDate] = useState(defaultEndDate);
  const [isOpen, setIsOpen] = useState(defaultOpen);

  const datepickerRef = useRef(null);

  const renderCalendar = () => {
    const year = currentDate.getFullYear();
    const month = currentDate.getMonth();

    const firstDayOfMonth = new Date(year, month, 1).getDay();
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const daysArray = [];

    // empty slots
    for (let i = 0; i < firstDayOfMonth; i++) {
      daysArray.push(<div key={`empty-${i}`} />);
    }

    // date cells
    for (let i = 1; i <= daysInMonth; i++) {
      const day = new Date(year, month, i);
      const iso = day.toISOString().split('T')[0];
      let className =
        "flex items-center justify-center cursor-pointer w-[46px] h-[46px] rounded-full " +
        "text-gray-800 dark:text-gray-200 hover:bg-blue-500 hover:text-white";

      if (selectedStartDate === iso) {
        className += " bg-blue-600 text-white";
      }
      if (selectedEndDate === iso) {
        className += " bg-blue-600 text-white";
      }
      if (
        selectedStartDate &&
        selectedEndDate &&
        iso > selectedStartDate &&
        iso < selectedEndDate
      ) {
        className += " bg-gray-300 dark:bg-gray-600";
      }

      daysArray.push(
        <div
          key={iso}
          className={className}
          onClick={() => handleDayClick(iso)}
        >
          {i}
        </div>
      );
    }

    return daysArray;
  };

  const handleDayClick = (iso) => {
    if (!selectedStartDate || (selectedStartDate && selectedEndDate)) {
      setSelectedStartDate(iso);
      setSelectedEndDate(null);
    } else if (iso < selectedStartDate) {
      setSelectedEndDate(selectedStartDate);
      setSelectedStartDate(iso);
    } else {
      setSelectedEndDate(iso);
    }
  };

  const updateInput = () => {
    if (selectedStartDate && selectedEndDate) {
      return `${selectedStartDate} - ${selectedEndDate}`;
    } else if (selectedStartDate) {
      return selectedStartDate;
    }
    return '';
  };

  const toggleDatepicker = () => setIsOpen((open) => !open);

  const handleApplyClick = () => {
    onApply(selectedStartDate, selectedEndDate);
    setIsOpen(false);
  };

  const handleCancelClick = () => {
    setSelectedStartDate(defaultStartDate);
    setSelectedEndDate(defaultEndDate);
    onCancel();
    setIsOpen(false);
  };

  useEffect(() => {
    const onClickOutside = (e) => {
      if (datepickerRef.current && !datepickerRef.current.contains(e.target)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', onClickOutside);
    return () => document.removeEventListener('mousedown', onClickOutside);
  }, []);

  return (
   <section className="relative">
      <div ref={datepickerRef} className="relative">
        <div className="flex items-center">
          <input
            type="text"
            readOnly
            value={updateInput()}
            onClick={toggleDatepicker}
            className="w-full bg-neutral-800 border border-neutral-600 rounded-md shadow-sm px-4 py-2 text-white cursor-pointer focus:outline-none focus:ring-2 focus:ring-indigo-500"
            placeholder="Pick a date range"
          />
          <button
            onClick={toggleDatepicker}
            className="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-white"
          >
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10m-10 4h10m-7 4h4m-2-16h.01" />
            </svg>
          </button>
        </div>

        {isOpen && (
          <div className="absolute z-20 mt-2 w-[300px] rounded-lg bg-white p-4 shadow-lg dark:bg-gray-800">
            <div className="flex justify-between mb-2">
              <button
                onClick={() => setCurrentDate(new Date(currentDate.setMonth(currentDate.getMonth() - 1)))}
                className="px-2 py-1 rounded text-gray-600 hover:bg-gray-700"
              >
                ‹
              </button>
              <div className="text-sm font-medium text-gray-800 dark:text-gray-200">
                {currentDate.toLocaleString('default', { month: 'long' })} {currentDate.getFullYear()}
              </div>
              <button
                onClick={() => setCurrentDate(new Date(currentDate.setMonth(currentDate.getMonth() + 1)))}
                className="px-2 py-1 rounded text-gray-600 hover:bg-gray-700"
              >
                ›
              </button>
            </div>
            <div className="grid grid-cols-7 gap-1 text-xs text-center text-gray-500">
              {['Su','Mo','Tu','We','Th','Fr','Sa'].map(d => (
                <div key={d}>{d}</div>
              ))}
            </div>
            <div className="mt-1 grid grid-cols-7 gap-1 text-sm">
              {renderCalendar()}
            </div>
            <div className="mt-3 flex justify-end space-x-2">
              <button
                onClick={handleCancelClick}
                className="px-3 py-1 text-sm text-gray-600 hover:bg-gray-700 rounded"
              >
                Cancel
              </button>
              <button
                onClick={handleApplyClick}
                className="px-3 py-1 bg-indigo-600 text-white rounded hover:bg-indigo-700 text-sm"
              >
                Apply
              </button>
            </div>
          </div>
        )}
      </div>
    </section>
  );
}