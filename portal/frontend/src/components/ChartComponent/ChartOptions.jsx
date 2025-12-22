import {CrosshairMode, LineStyle} from 'lightweight-charts'

 export const options = {
      layout: {
        textColor: '#DDD',
        background: { color: '#1E1E1E' },
      },
      grid: {
        vertLines: { color: "#444" },
        horzLines: { color: "#444" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          width: 8,
          color: '#C3BCDB45',
          style: LineStyle.Solid,
          labelBackgroundColor: '#000',
        },
        horzLine: {
          color: '#C3BCDB70',
          labelBackgroundColor: '#000',
        },
      },
      localization: {
        timeFormatter: (businessDayOrTimestamp) => {
          // Handle both business days and UTC timestamps
          if (typeof businessDayOrTimestamp === 'object') {
            // Business day format
            const { year, month, day } = businessDayOrTimestamp;
            return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
          }
          // UTC timestamp in seconds - show date and time
          const date = new Date(businessDayOrTimestamp * 1000);
          const month = String(date.getUTCMonth() + 1).padStart(2, '0');
          const day = String(date.getUTCDate()).padStart(2, '0');
          const hours = String(date.getUTCHours()).padStart(2, '0');
          const minutes = String(date.getUTCMinutes()).padStart(2, '0');
          return `${month}/${day} ${hours}:${minutes}`;
        },
        dateFormatter: (businessDayOrTimestamp) => {
          if (typeof businessDayOrTimestamp === 'object') {
            const { year, month, day } = businessDayOrTimestamp;
            return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
          }
          const date = new Date(businessDayOrTimestamp * 1000);
          const year = date.getUTCFullYear();
          const month = String(date.getUTCMonth() + 1).padStart(2, '0');
          const day = String(date.getUTCDate()).padStart(2, '0');
          return `${year}-${month}-${day}`;
        },
      },
      timeScale: {
        // Force UTC time interpretation
        timeVisible: true,
        secondsVisible: false,
      },
    }

export const seriesOptions = {
      wickUpColor: 'rgb(54, 116, 217)',
      upColor: 'rgb(54, 116, 217)',
      wickDownColor: 'rgb(225, 50, 85)',
      downColor: 'rgb(225, 50, 85)',
      borderVisible: false,
    }